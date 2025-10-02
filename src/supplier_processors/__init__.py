import contextlib
from asyncio import gather, to_thread
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime
from ftplib import FTP, _SSLSocket  # type: ignore
from json import dump, loads
from logging import getLogger
from pathlib import Path, PurePosixPath
from re import Pattern, compile
from typing import Self

from aiologic import Lock
from database.cache import DatabaseCache
from environment_init_vars import CWD, SFT_WEBSITE_CREDS_FILE
from rich_custom import CustomTaskID, ProgressCustom
from typing_custom import CustomerID, StoreNum
from typing_custom.abc import SingletonType
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import SuppliersEnum

logger = getLogger(__name__)


def advance_pbar(pbar: ProgressCustom, task_id: CustomTaskID):
  def advance(data: bytes):
    pbar.update(task_id, advance=len(data))

  return advance


@dataclass
class FileRegisterData:
  storenum: StoreNum
  customer_id: CustomerID
  pickup_date: datetime
  dropoff_date: datetime
  file_pattern: Pattern[str]
  current_week: bool
  _waiting_folder: PurePosixPath

  file_name: list[str] = field(default_factory=list)
  pickup_success: dict[int, bool] = field(default_factory=dict)
  application_success: dict[int, bool] = field(default_factory=dict)

  @property
  def file_loc(self) -> list[PurePosixPath]:
    return [self._waiting_folder / name for name in self.file_name]


class SupplierProcessorBase[T_VendorFTP](metaclass=SingletonType):
  vendor_ftp: T_VendorFTP

  file_pickup_queue: dict[str, FileRegisterData] = {}
  file_waiting_queue: dict[str, FileRegisterData] = {}
  file_application_queue: dict[str, FileRegisterData] = {}

  file_queue_backup_folder: Path = CWD / "queue backups"

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  lock: Lock = Lock()

  pickup_ftp_creds: dict
  sft_ftp_creds: dict = loads(SFT_WEBSITE_CREDS_FILE.read_text())

  pickup_ftp_folder: PurePosixPath
  waiting_folder: PurePosixPath
  destination_ftp_folder: PurePosixPath

  def __init__(self, pbar: ProgressCustom = None) -> None:  # type: ignore
    self.file_queue_backup_folder.mkdir(exist_ok=True)

    self.pickup_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.application_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_application_queue.json"

    self.pbar = pbar

    self._load_queue_backups()

    self.cache: DatabaseCache = DatabaseCache()

  async def save_queue_backups_off_thread(self) -> None:
    await to_thread(self._save_backups)

  def _save_backups(self) -> None:
    with self.lock:
      backup = (
        (
          self.pickup_queue_backup_file,
          {
            k: {ik: (iv.pattern if isinstance(iv, Pattern) else str(iv)) for ik, iv in asdict(v).items()}
            for k, v in deepcopy(self.file_pickup_queue).items()
          },
        ),
        (
          self.waiting_queue_backup_file,
          {
            k: {ik: (iv.pattern if isinstance(iv, Pattern) else str(iv)) for ik, iv in asdict(v).items()}
            for k, v in deepcopy(self.file_waiting_queue).items()
          },
        ),
        (
          self.application_queue_backup_file,
          {
            k: {ik: (iv.pattern if isinstance(iv, Pattern) else str(iv)) for ik, iv in asdict(v).items()}
            for k, v in deepcopy(self.file_application_queue).items()
          },
        ),
      )

      for file, data in backup:
        with file.open("w", encoding="utf-8") as f:
          dump(data, f, indent=2)

  def _load_queue_backups(self) -> None:
    with self.lock:
      to_load = (
        (
          loads(self.pickup_queue_backup_file.read_text() if self.pickup_queue_backup_file.exists() else "{}"),
          self.file_pickup_queue,
        ),
        (
          loads(self.waiting_queue_backup_file.read_text() if self.waiting_queue_backup_file.exists() else "{}"),
          self.file_waiting_queue,
        ),
        (
          loads(self.application_queue_backup_file.read_text() if self.application_queue_backup_file.exists() else "{}"),
          self.file_application_queue,
        ),
      )

      for loaded, target in to_load:
        for k, v in loaded.items():
          target[k] = FileRegisterData(
            storenum=v["storenum"],
            customer_id=v["customer_id"],
            pickup_date=datetime.fromisoformat(v["pickup_date"]),
            dropoff_date=datetime.fromisoformat(v["dropoff_date"]),
            file_pattern=compile(v["file_pattern"]),
            current_week=v["current_week"],
            file_name=v.get("file_name", []),
            _waiting_folder=self.waiting_folder,
          )

  async def register_pickup(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool = True
  ) -> None: ...

  async def register_application(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool
  ) -> None: ...

  async def pickup_files(self) -> None: ...

  async def dropoff_files(self) -> None:
    if not self.file_application_queue:
      return
    async with self.lock:
      with self.pbar.add_task(
        "Moving files to application folder", total=sum(len(v.file_name) for v in self.file_application_queue.values())
      ) as files_move_task:
        futures = []
        for key, file_meta in tuple(self.file_application_queue.items()):
          futures.extend(
            to_thread(
              self._transfer_file_main_to_main,
              send_path=waiting_path,
              recv_path=(self.destination_ftp_folder / waiting_path.name),
              move_files_task=files_move_task,
              file_meta=file_meta,
              idx=idx,
            )
            for idx, waiting_path in enumerate(file_meta.file_loc)
          )
        await gather(*futures)

      for key, file_meta in tuple(self.file_application_queue.items()):
        if all(file_meta.application_success.values()):
          self.file_application_queue.pop(key)
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          logger.info(f"{self.__class__.__name__}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_applied")
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_applied)

  def _transfer_file_vend_to_main(
    self, send_path: PurePosixPath, recv_path: PurePosixPath, move_files_task: CustomTaskID, file_meta: FileRegisterData, idx: int
  ):
    with self.vendor_ftp(self.pickup_ftp_creds) as origin_client:  # type: ignore
      file_size = origin_client.stat(send_path.as_posix()).st_size
      with SFTFTPClient(self.sft_ftp_creds) as dest_client:
        dest_client.voidcmd("TYPE I")
        with self.pbar.add_task(f"Transferring {send_path.name}") as transfer_task:
          with origin_client.open(send_path.as_posix(), "rb") as read_file:
            read_file.prefetch(file_size)
            with dest_client.transfercmd(f"STOR {recv_path.as_posix()}") as write_file:
              while buffer := read_file.read(8192):
                write_file.sendall(buffer)
                self.pbar.update(transfer_task, advance=len(buffer))
              if _SSLSocket is not None and isinstance(write_file, _SSLSocket):
                write_file.unwrap()  # type: ignore
            dest_client.voidresp()
        logger.info(
          f"{self.__class__.__name__}: Transferred SAS [yellow]{send_path}[/] to SFT FTP [yellow]{recv_path}[/]",
          extra={"markup": True},
        )

        success = False
        with contextlib.suppress(Exception):
          dest_client.size(recv_path.as_posix())
          success = True
        file_meta.pickup_success[idx] = success
    self.pbar.update(move_files_task, advance=1)
    return success

  def _transfer_file_main_to_main(
    self, send_path: PurePosixPath, recv_path: PurePosixPath, move_files_task: CustomTaskID, file_meta: FileRegisterData, idx: int
  ) -> None:
    with SFTFTPClient(self.sft_ftp_creds) as origin_client:
      origin_client.voidcmd("TYPE I")
      origin_client.rename(send_path.as_posix(), recv_path.as_posix())

      success = False
      with contextlib.suppress(Exception):
        origin_client.size(recv_path.as_posix())
        success = True
        logger.info(f"{self.__class__.__name__}: Moved [yellow]{send_path}[/] to [yellow]{recv_path}[/]", extra={"markup": True})
      file_meta.application_success[idx] = success

    self.pbar.update(move_files_task, advance=1)


class SFTFTPClient(FTP):
  def __init__(self, creds: dict) -> None:
    self.creds = creds
    super().__init__()

  def __enter__(self) -> Self:
    self.connect(host=self.creds["HOST"], port=self.creds["PORT"])
    self.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    return self
