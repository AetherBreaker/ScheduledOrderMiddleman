from asyncio import gather, to_thread
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime
from ftplib import FTP
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
  _waiting_folder: Path

  file_name: list[str] = field(default_factory=list)
  pickup_success: dict[int, bool] = field(default_factory=dict)
  application_success: dict[int, bool] = field(default_factory=dict)

  @property
  def file_loc(self) -> list[Path]:
    return [self._waiting_folder / name for name in self.file_name]


class SupplierProcessorBase(metaclass=SingletonType):
  file_pickup_queue: dict[str, FileRegisterData] = {}
  file_waiting_queue: dict[str, FileRegisterData] = {}
  file_application_queue: dict[str, FileRegisterData] = {}

  file_queue_backup_folder: Path = CWD / "queue backups"

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  lock: Lock = Lock()

  pickup_ftp_creds: dict
  destination_ftp_creds: dict = loads(SFT_WEBSITE_CREDS_FILE.read_text())

  pickup_ftp_folder: PurePosixPath
  waiting_folder: Path
  destination_ftp_folder: PurePosixPath

  def __init__(self, pbar: ProgressCustom = None) -> None:  # type: ignore
    self.file_queue_backup_folder.mkdir(exist_ok=True)

    self.pickup_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.application_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_application_queue.json"

    self.waiting_folder.mkdir(exist_ok=True)
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

  def _dropoff_file(
    self,
    local_path: Path,
    destination_path: str,
    total_downloads_task: CustomTaskID,
    file_meta: FileRegisterData,
    idx: int,
  ):
    with SFTFTPClient(self.destination_ftp_creds) as ftp:
      ftp.voidcmd("TYPE I")

      with self.pbar.add_task(f"Uploading {local_path.name}") as upload_task:
        with local_path.open("rb") as file:
          ftp.storbinary(cmd=f"STOR {destination_path}", fp=file, callback=advance_pbar(self.pbar, upload_task))

      success = False
      try:
        ftp.size(destination_path)
        success = True
      except Exception:
        pass

      file_meta.application_success[idx] = success

      logger.info(f"Uploaded {local_path.name} to {destination_path}")
      self.pbar.update(total_downloads_task, advance=1)

  async def dropoff_files(self) -> None:
    if self.file_application_queue:
      async with self.lock:
        total_downloads_task = self.pbar.add_task("Total Uploads", total=len(self.file_application_queue))

        futures = []
        for key, file_meta in tuple(self.file_application_queue.items()):
          for idx, local_path in enumerate(file_meta.file_loc):
            futures.append(
              to_thread(
                self._dropoff_file,
                local_path=local_path,
                destination_path=(self.destination_ftp_folder / local_path.name).as_posix(),
                total_downloads_task=total_downloads_task,
                file_meta=file_meta,
                idx=idx,
              )
            )

        await gather(*futures)

        for key, file_meta in tuple(self.file_application_queue.items()):
          if all(file_meta.application_success.values()):
            for local_path in file_meta.file_loc:
              local_path.unlink()

            self.file_application_queue.pop(key)
            schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

            logger.info(f"Checking off {self.supplier_name}_{file_meta.storenum} invoice_applied")
            await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_applied)

        self.pbar.remove_task(total_downloads_task)


class SFTFTPClient(FTP):
  def __init__(self, creds: dict) -> None:
    self.creds = creds
    super().__init__()

  def __enter__(self) -> Self:
    self.connect(host=self.creds["HOST"], port=self.creds["PORT"])
    self.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    return self
