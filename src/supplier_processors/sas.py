from asyncio import gather, to_thread
from datetime import datetime
from ftplib import _SSLSocket  # type: ignore
from json import loads
from logging import getLogger
from pathlib import Path, PurePosixPath
from re import Pattern, compile

from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from environment_init_vars import CWD, SAS_FTP_CREDS_FILE
from paramiko import AutoAddPolicy, SFTPClient, SSHClient
from typing_custom import CustomerID, StoreNum
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import SuppliersEnum
from utils import paramiko_pbar_callback

from supplier_processors import FileRegisterData, SFTFTPClient, SupplierProcessorBase

logger = getLogger(__name__)


class SASSFTPClient:
  policy = AutoAddPolicy()

  def __init__(self, creds: dict):
    self.creds = creds

  def __enter__(self) -> SFTPClient:
    self.ssh_client = SSHClient()
    self.ssh_client.set_missing_host_key_policy(self.policy)

    self.ssh_client.connect(
      hostname=self.creds["HOSTNAME"],
      port=self.creds.get("PORT", 22),
      username=self.creds["USER"],
      password=self.creds["PWD"],
    )

    self.sftp_client = self.ssh_client.open_sftp()

    return self.sftp_client

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.sftp_client.close()
    self.ssh_client.close()


class SASProcessor(SupplierProcessorBase):
  pickup_ftp_creds: dict = loads(SAS_FTP_CREDS_FILE.read_text())

  pickup_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices")
  # pickup_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices/Archive")
  pickup_archive_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices/Archive")
  waiting_folder = CWD / "SAS Waiting Invoices"
  destination_ftp_folder = PurePosixPath("/SAS")

  queue_backup_prefix: str = "sas"

  supplier_name: SuppliersEnum = SuppliersEnum.SAS

  async def register_pickup(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool = True
  ) -> None:
    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )

    if picked_up:
      logger.warning(f"Attempted to register pickup for already grabbed invoice: {self.supplier_name}, {storenum}, {customer_id}")
      return
    if applied:
      logger.warning(f"Attempted to register pickup for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}")
      return

    pattern = self.assemble_filename_pattern(customer_id, pickup_date, dropoff_date, current_week)

    register_data = FileRegisterData(
      storenum=storenum,
      customer_id=customer_id,
      pickup_date=pickup_date,
      dropoff_date=dropoff_date,
      file_pattern=pattern,
      current_week=current_week,
      _waiting_folder=self.waiting_folder,
    )

    self.file_pickup_queue[self.assemble_queue_key(storenum, customer_id, pickup_date)] = register_data

  async def register_application(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool
  ) -> None:
    key = f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )
    if not picked_up:
      logger.warning(
        f"Attempted to register application for not-yet picked up invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      logger.warning(
        f"Attempted to register application for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return

    # first check if key is already in application queue
    if key not in self.file_application_queue:
      try:
        matched_item = self.file_waiting_queue.pop(key)
      except KeyError:
        logger.error(
          f"No waiting file found for: {self.supplier_name}, {storenum}, {customer_id}, {pickup_date.isoformat()}\n"
          f"Invoice may not have been picked up or is missing!"
        )
        return

      self.file_application_queue[key] = matched_item
    else:
      logger.warning(f"File already registered for application: {key}")

  # async def ensure_pickup(self, entries: Iterable[tuple[StoreNum, CustomerID, datetime, datetime, bool]]) -> None:
  #   now = datetime.now(tz=TZ)
  #   for storenum, customer_id, pickup_date, dropoff_date, current_week in entries:
  #     picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
  #       (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
  #     )
  #     if not picked_up and ((now - pickup_date) > timedelta(minutes=1)):
  #       expected_key = self.assemble_queue_key(storenum, customer_id, pickup_date)

  #       pickup = self.file_pickup_queue.get(expected_key)
  #       waiting = self.file_waiting_queue.get(expected_key)

  #       waiting_for_pickup = pickup is not None
  #       waiting_to_apply = waiting is not None

  #       if not waiting_for_pickup and not waiting_to_apply:
  #         logger.warning(f"File missing from both queues, registering for pickup: {expected_key}")
  #         await self.register_pickup(
  #           storenum=storenum,
  #           customer_id=customer_id,
  #           pickup_date=pickup_date,
  #           dropoff_date=dropoff_date,
  #           current_week=current_week,
  #         )
  #       # elif waiting_to_apply:
  #       #   try:
  #       #     futs = []
  #       #     for idx, missing_file in filter(lambda item: not item[1].exists(), enumerate(waiting.file_loc)):
  #       #       logger.warning(f"File missing on disk, attempting re-retrieval: {missing_file}")
  #       #       futs.append(
  #       #         to_thread(
  #       #           self._pickup_file,
  #       #           remote_path=self.pickup_ftp_folder / missing_file.name,
  #       #           local_path=missing_file,
  #       #           file_meta=waiting,
  #       #           idx=idx,
  #       #         )
  #       #       )
  #       #     if futs:
  #       #       await gather(*futs)
  #       #   except FileNotFoundError:
  #       #     logger.error(f"File not found on FTP: {waiting.file_loc}")
  #       #   if not all(file.exists() for file in waiting.file_loc):
  #       #     logger.error(
  #       #       f"One or more files still missing after re-retrieval: {waiting.file_loc}" f"\nAttempting to retrieve from archive."
  #       #     )
  #       #     try:
  #       #       futs = []
  #       #       for idx, missing_file in filter(lambda item: not item[1].exists(), enumerate(waiting.file_loc)):
  #       #         # TODO check archive file date first
  #       #         futs.append(
  #       #           to_thread(
  #       #             self._pickup_file,
  #       #             remote_path=self.pickup_archive_ftp_folder / missing_file.name,
  #       #             local_path=missing_file,
  #       #             file_meta=waiting,
  #       #             idx=idx,
  #       #           )
  #       #         )
  #       #       if futs:
  #       #         await gather(*futs)
  #       #     except FileNotFoundError:
  #       #       logger.error(f"File not found in archive either: {waiting.file_loc}")
  #       #     if not all(file.exists() for file in waiting.file_loc):
  #       #       logger.error(
  #       #         f"[red]One or more files still missing after archive retrieval: {waiting.file_loc}[/]", extra={"markup": True}
  #       #       )
  #       elif waiting_for_pickup:
  #         logger.warning(f"File(s) still waiting for pickup: {expected_key}")

  # async def ensure_application(self, entries: Iterable[tuple[StoreNum, CustomerID, datetime, datetime, bool]]) -> None:
  #   now = datetime.now(tz=TZ)
  #   for storenum, customer_id, pickup_date, dropoff_date, current_week in entries:
  #     applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
  #       (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
  #     )
  #     if not applied and (dropoff_date < now < (dropoff_date + timedelta(days=1))):
  #       expected_key = self.assemble_queue_key(storenum, customer_id, pickup_date)

  #       waiting = self.file_waiting_queue.get(expected_key)
  #       apply = self.file_application_queue.get(expected_key)

  #       waiting_for_application = apply is not None
  #       waiting_to_move = waiting is not None

  #       if not waiting_for_application and waiting_to_move:
  #         logger.warning(f"File still waiting to move, registering for application: {expected_key}")
  #         await self.register_application(
  #           storenum=storenum,
  #           customer_id=customer_id,
  #           pickup_date=pickup_date,
  #           dropoff_date=dropoff_date,
  #           current_week=current_week,
  #         )
  #       # elif waiting_for_application:
  #       #   futs = []
  #       #   for idx, missing_file in filter(lambda item: not item[1].exists(), enumerate(apply.file_loc)):
  #       #     logger.warning(f"File missing on disk, attempting re-retrieval: {missing_file}")
  #       #     futs.append(
  #       #       to_thread(
  #       #         self._pickup_file,
  #       #         remote_path=self.pickup_ftp_folder / missing_file.name,
  #       #         local_path=missing_file,
  #       #         file_meta=apply,
  #       #         idx=idx,
  #       #       )
  #       #     )
  #       #   if futs:
  #       #     await gather(*futs)
  #       #   if not all(file.exists() for file in apply.file_loc):
  #       #     logger.error(
  #       #       f"One or more files still missing after re-retrieval: {apply.file_loc}" f"\nAttempting to retrieve from archive."
  #       #     )
  #       #     futs = []
  #       #     for idx, missing_file in filter(lambda item: not item[1].exists(), enumerate(apply.file_loc)):
  #       #       futs.append(
  #       #         to_thread(
  #       #           self._pickup_file,
  #       #           remote_path=self.pickup_archive_ftp_folder / missing_file.name,
  #       #           local_path=missing_file,
  #       #           file_meta=apply,
  #       #           idx=idx,
  #       #         )
  #       #       )
  #       #     if futs:
  #       #       await gather(*futs)
  #       #     if not all(file.exists() for file in apply.file_loc):
  #       #       logger.error(
  #       #         f"[red]One or more files still missing after archive retrieval: {apply.file_loc}[/]", extra={"markup": True}
  #       #       )
  #       elif waiting_for_application:
  #         logger.warning(f"File(s) still waiting for application: {expected_key}")

  def assemble_queue_key(self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime) -> str:
    return f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern:
    dates = list(
      rrule(
        DAILY,
        dtstart=(start_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0, microsecond=0))
        - relativedelta(weeks=1 if current_week else 0),
        until=(end_date + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59, microsecond=999999))
        - relativedelta(weeks=0 if current_week else 1),
      )
    )

    years = set(str(date.year) for date in dates)
    months = set(f"{date.month:02d}" for date in dates)
    days = set(f"{date.day:02d}" for date in dates)

    years_part = "|".join(years)
    months_part = "|".join(months)
    days_part = "|".join(days)

    pattern = (
      rf"^EF{customer_id}_"
      r"(?P<timestamp>"
      rf"(?P<year>{years_part})"
      rf"(?P<month>{months_part})"
      rf"(?P<day>{days_part})"
      r"(?P<hour>\d{2})"
      r"(?P<minute>\d{2})"
      r"(?P<second>\d{2})"
      r"(?P<microsecond>\d{6})"
      r")\.TXT$"
    )
    return compile(pattern)

  # def _transfer_file(self, send_path: PurePosixPath, recv_path: PurePosixPath, file_meta: FileRegisterData, idx: int) -> None:
  #   with SASSFTPClient(self.pickup_ftp_creds) as origin_client:
  #     file_size = origin_client.stat(send_path.as_posix()).st_size
  #     with SFTFTPClient(self.destination_ftp_creds) as dest_client:
  #       with self.pbar.add_task(f"Transferring {send_path.name}") as transfer_task:
  #         with origin_client.open(send_path.as_posix(), "rb") as read_file:
  #           read_file.prefetch(file_size)
  #           with dest_client.transfercmd(f"STOR {recv_path.as_posix()}") as write_file:
  #             while buffer := read_file.read(8192):
  #               write_file.sendall(buffer)
  #               self.pbar.update(transfer_task, advance=len(buffer))
  #             if _SSLSocket is not None and isinstance(write_file, _SSLSocket):
  #               write_file.unwrap()  # type: ignore
  #           dest_client.voidresp()
  #       logger.info(f"Transferred SAS {send_path} to SFT FTP {recv_path}")

  #       success = False
  #       try:
  #         dest_client.size(recv_path.as_posix())
  #         success = True
  #       except Exception:
  #         pass
  #       file_meta.pickup_success[idx] = success

  def _pickup_file(self, remote_path: PurePosixPath, local_path: Path, file_meta: FileRegisterData, idx: int) -> None:
    with SASSFTPClient(self.pickup_ftp_creds) as sftp_client:
      with self.pbar.add_task(f"Downloading {remote_path}") as download_task:
        sftp_client.get(
          remote_path.as_posix(),
          local_path.as_posix(),
          callback=paramiko_pbar_callback(self.pbar, download_task),
        )
    logger.info(f"Downloaded {remote_path} to {local_path}")

    file_meta.pickup_success[idx] = local_path.exists()

  def _archive_file(self, remote_file: str) -> None:
    archive_loc = (self.pickup_archive_ftp_folder / remote_file).as_posix()
    with SASSFTPClient(self.pickup_ftp_creds) as sftp_client:
      try:
        sftp_client.stat(archive_loc)
        logger.warning("Archive file already exists. Deleting new file instead of moving.")

      except IOError:
        sftp_client.rename((self.pickup_ftp_folder / remote_file).as_posix(), archive_loc)
        logger.info(f"Archived {remote_file} to {self.pickup_archive_ftp_folder.as_posix()}")
        return

      sftp_client.remove((self.pickup_ftp_folder / remote_file).as_posix())
    pass

  async def pickup_files(self) -> None:
    if self.file_pickup_queue:
      async with self.lock:
        with SASSFTPClient(self.pickup_ftp_creds) as sftp_client:
          remote_files = [file_attr.filename for file_attr in sftp_client.listdir_attr(self.pickup_ftp_folder.as_posix())]

        items_to_dl: dict[str, FileRegisterData] = {}
        for key, file_meta in self.file_pickup_queue.items():
          matched_files = []

          for remote_file in remote_files:
            if match := file_meta.file_pattern.match(remote_file):
              matched_files.append(match)

          if matched_files:
            file_meta.file_name = []
            for m in matched_files:
              file_meta.file_name.append(m.string)

            items_to_dl[key] = file_meta
          else:
            logger.warning(f"No files matched for: {key} with pattern {file_meta.file_pattern.pattern}")
            continue

        dl_futures = []
        for key, file_meta in items_to_dl.items():
          for idx, (filename, local_path) in enumerate(zip(file_meta.file_name, file_meta.file_loc)):
            dl_futures.append(
              to_thread(
                self._pickup_file,
                remote_path=(self.pickup_ftp_folder / filename),
                local_path=local_path,
                file_meta=file_meta,
                idx=idx,
              )
            )

        await gather(*dl_futures)

        archive_futures = []
        items_to_advance = {}
        for key, file_meta in items_to_dl.items():
          if all(file_meta.pickup_success.values()):
            for filename in file_meta.file_name:
              archive_futures.append(to_thread(self._archive_file, filename))

            items_to_advance[key] = file_meta
            schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

            logger.info(f"Checking off {self.supplier_name}_{file_meta.storenum} invoice_grabbed")
            await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

        await gather(*archive_futures)

      for key, item in items_to_advance.items():
        self.file_waiting_queue[key] = item
        self.file_pickup_queue.pop(key)
