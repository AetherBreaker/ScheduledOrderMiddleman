if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import Event, run
from datetime import datetime, timedelta
from logging import getLogger

from apscheduler.triggers.cron import CronTrigger
from database.cache import DatabaseCache
from dateutil.relativedelta import SA, relativedelta
from environment_init_vars import TZ
from logging_config import RICH_CONSOLE
from rich_custom import LiveCustom
from scheduler_config import OrderProcessingScheduler
from supplier_processors.sas import SASProcessor
from typing_custom.enums import SuppliersEnum

logger = getLogger(__name__)


supplier_register = {
  SuppliersEnum.SAS: SASProcessor,
  # SuppliersEnum.RYO: ...,
}

scheduler = OrderProcessingScheduler.init_scheduler()


async def reschedule_all_tasks():
  cache = DatabaseCache()
  current_week = cache.schedule
  previous_week = cache.prev_week_schedule

  for supplier, processor in supplier_register.items():
    scheduler.add_job(
      processor().pickup_files,
      CronTrigger(minute="2-59/5"),
      id=f"{supplier}_pickup_files",
      replace_existing=True,
    )
    scheduler.add_job(
      processor().dropoff_files,
      CronTrigger(minute="4-59/5"),
      id=f"{supplier}_dropoff_files",
      replace_existing=True,
    )

    scheduler.add_job(
      processor().save_queue_backups_off_thread,
      CronTrigger(minute="*/30"),
      id=f"{supplier}_save_queue_backups",
      replace_existing=True,
    )

    # orders = []

    # async with current_week as cache:
    #   filtered = cache.loc[cache[DatabaseScheduleColumns.supplier] == supplier]
    #   for _, row in filtered.iterrows():
    #     order = current_week._model.model_construct(**row)  # type: ignore
    #     if not order.customer or not order.store and order.customer != "":
    #       continue
    #     orders.append((order.store, order.customer, order.invoice_pickup_time, order.invoice_application_time, True))
    # async with previous_week as cache:
    #   filtered = cache.loc[cache[DatabaseScheduleColumns.supplier] == supplier]
    #   for _, row in filtered.iterrows():
    #     order = previous_week._model.model_construct(**row)  # type: ignore
    #     if not order.customer or not order.store and order.customer != "":
    #       continue
    #     orders.append((order.store, order.customer, order.invoice_pickup_time, order.invoice_application_time, False))

    # scheduler.add_job(
    #   processor().ensure_pickup,
    #   CronTrigger(minute="1-59/5"),
    #   args=[orders],
    #   id=f"{supplier}_ensure_pickup",
    #   replace_existing=True,
    #   next_run_time=datetime.now(TZ) + timedelta(minutes=1),
    # )

    # scheduler.add_job(
    #   processor().ensure_application,
    #   CronTrigger(minute="3-59/5"),
    #   args=[orders],
    #   id=f"{supplier}_ensure_application",
    #   replace_existing=True,
    #   next_run_time=datetime.now(TZ) + timedelta(minutes=2),
    # )

  now = datetime.now(TZ)

  async for order in current_week.walk_typed_rows():
    if order.store == 33:
      pass
    if not order.customer or not order.store:
      continue
    if (now - order.invoice_pickup_time) > timedelta(minutes=1) and not order.invoice_grabbed:
      scheduler.add_job(
        supplier_register[order.supplier]().register_pickup,
        CronTrigger(
          minute="1-59/5",
          start_date=order.invoice_pickup_time,
          end_date=order.invoice_application_time + relativedelta(weekday=SA(1), hour=23, minute=59, second=59),
        ),
        kwargs={
          "storenum": order.store,
          "customer_id": order.customer,
          "pickup_date": order.invoice_pickup_time,
          "dropoff_date": order.invoice_application_time,
          "current_week": True,
        },
        id=f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
        replace_existing=True,
      )

    if (now - order.invoice_application_time) > timedelta(minutes=1) and not order.invoice_applied:
      if not order.customer or not order.store:
        continue
      scheduler.add_job(
        supplier_register[order.supplier]().register_application,
        CronTrigger(
          minute="3-59/5",
          start_date=order.invoice_application_time,
          end_date=order.invoice_application_time + relativedelta(weekday=SA(1), hour=23, minute=59, second=59),
        ),
        kwargs={
          "storenum": order.store,
          "customer_id": order.customer,
          "pickup_date": order.invoice_pickup_time,
          "dropoff_date": order.invoice_application_time,
          "current_week": True,
        },
        id=f"{order.supplier}_register_dropoff_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
        replace_existing=True,
      )

  async for order in previous_week.walk_typed_rows():
    if (now - order.invoice_pickup_time) > timedelta(minutes=1) and not order.invoice_grabbed:
      scheduler.add_job(
        supplier_register[order.supplier]().register_pickup,
        CronTrigger(
          minute="1-59/5",
          start_date=order.invoice_pickup_time,
          end_date=order.invoice_application_time + relativedelta(weekday=SA(1), hour=23, minute=59, second=59),
        ),
        kwargs={
          "storenum": order.store,
          "customer_id": order.customer,
          "pickup_date": order.invoice_pickup_time,
          "dropoff_date": order.invoice_application_time,
          "current_week": False,
        },
        id=f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
        replace_existing=True,
      )

    if (now - order.invoice_application_time) > timedelta(minutes=1) and not order.invoice_applied:
      scheduler.add_job(
        supplier_register[order.supplier]().register_application,
        CronTrigger(
          minute="3-59/5",
          start_date=order.invoice_application_time,
          end_date=order.invoice_application_time + relativedelta(weekday=SA(1), hour=23, minute=59, second=59),
        ),
        kwargs={
          "storenum": order.store,
          "customer_id": order.customer,
          "pickup_date": order.invoice_pickup_time,
          "dropoff_date": order.invoice_application_time,
          "current_week": True,
        },
        id=f"{order.supplier}_register_dropoff_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
        replace_existing=True,
      )


async def flip_week():
  scheduler.pause()
  scheduler.remove_all_jobs()
  cache = DatabaseCache()
  await cache.flip_to_new_week()

  await reschedule_all_tasks()

  scheduler.resume()


async def main():
  with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
    cache = DatabaseCache()

    for processor in supplier_register.values():
      processor(live.pbar)

    await cache.refresh_cache()

    scheduler.add_job(
      cache.refresh_cache,
      CronTrigger(minute="*/30"),
      id="refresh_cache",
      replace_existing=True,
      next_run_time=datetime.now(TZ) + timedelta(minutes=30),
    )

    scheduler.add_job(
      cache.submit_queued_writes_to_pool,
      CronTrigger(second="*/30"),
      id="submit_queued_writes_to_pool",
      replace_existing=True,
    )

    scheduler.add_job(
      reschedule_all_tasks,
      CronTrigger(
        hour=7,
      ),
      id="reschedule_all_tasks",
      replace_existing=True,
      next_run_time=datetime.now(TZ) + timedelta(seconds=10),
    )

    scheduler.add_job(
      flip_week,
      CronTrigger(
        day_of_week="sat",
        hour=23,
        minute=59,
        second=59,
      ),
      id="flip_week",
      replace_existing=True,
    )

    scheduler.start()

    if __debug__:
      pass
      # await flip_week()

      # order = await cache.schedule.read_typed_row((SuppliersEnum.SAS, 1))

      # pass
      # await supplier_register[order.supplier]().register_pickup(
      #   storenum=order.store,
      #   customer_id=order.customer,
      #   pickup_date=order.invoice_pickup_time,
      #   dropoff_date=order.invoice_application_time,
      #   current_week=True,
      # )
      # pass

      # await supplier_register[order.supplier]().save_queue_backups_off_thread()

      # await supplier_register[order.supplier]().pickup_files()

      # await supplier_register[order.supplier]().save_queue_backups_off_thread()

      # pass
      # await supplier_register[order.supplier]().register_application(
      #   storenum=order.store,
      #   customer_id=order.customer,
      #   pickup_date=order.invoice_pickup_time,
      # )

      # await supplier_register[order.supplier]().save_queue_backups_off_thread()

      # await supplier_register[order.supplier]().dropoff_files()

    await Event().wait()


if __name__ == "__main__":
  run(main())
