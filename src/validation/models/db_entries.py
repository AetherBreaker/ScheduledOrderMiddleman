if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime
from inspect import get_annotations
from logging import getLogger
from re import compile
from typing import Annotated, Optional

from dateutil.relativedelta import FR, MO, SA, SU, TH, TU, WE, relativedelta
from dateutil.utils import today
from environment_init_vars import TZ
from pydantic import BeforeValidator, TypeAdapter
from typing_custom import CustomerID, InvoiceNum, StoreNum
from typing_custom.enums import StateEnum, SuppliersEnum, WeekdayEnum

from validation import PYDANTIC_CONFIG, CustomBaseModel, CustomRootModel

logger = getLogger(__name__)


BASE_TIMESTAMP = datetime(
  year=1899,
  month=12,
  day=30,
  tzinfo=TZ,
)


weekday_lookup = {
  "Monday": MO,
  "Tuesday": TU,
  "Wednesday": WE,
  "Thursday": TH,
  "Friday": FR,
  "Saturday": SA,
  "Sunday": SU,
}

TIMESTAMP_PATTERN = compile(r"(?P<Weekday>\w*?) (?P<Hour>\d{1,2}):(?P<Minute>\d{2})(?P<Period>AM|PM)")


def process_time_pattern(target_time) -> datetime:
  match = TIMESTAMP_PATTERN.match(target_time) if target_time else None

  if match:
    now = today(tzinfo=TZ)

    next_sunday = now + relativedelta(weekday=SU)

    now.weekday()

    weekday = weekday_lookup.get(match.group("Weekday"))
    if not weekday:
      raise ValueError(f"Invalid weekday: {match.group('Weekday')}")

    hour = int(match.group("Hour")) + (12 if match.group("Period") == "PM" and match.group("Hour") != "12" else 0)
    minute = int(match.group("Minute"))

    result = now + relativedelta(weekday=weekday(+1), hour=hour, minute=minute)
    if result >= next_sunday:
      result -= relativedelta(weeks=1)

    return result

  else:
    return target_time  # type: ignore


class ScheduledOrderDBEntryModel(CustomBaseModel):
  supplier: SuppliersEnum
  store: StoreNum
  customer: CustomerID
  state: StateEnum
  expected_delivery_day: Annotated[WeekdayEnum, BeforeValidator(str.strip), BeforeValidator(str.title)]
  invoice_pickup_time: Annotated[datetime, BeforeValidator(process_time_pattern)]
  invoice_application_time: Annotated[datetime, BeforeValidator(process_time_pattern)]
  invoice_grabbed: bool = False
  invoice_applied: bool = False


class OrderLogDBEntryModel(CustomBaseModel):
  supplier: SuppliersEnum
  store: StoreNum
  po_number: InvoiceNum
  customer: CustomerID
  applied_date: datetime
  week_ending_date: datetime
  notes: Optional[str] = None


SCHEDULE_TYPE_ADAPTERS = {
  field: TypeAdapter(fieldinf, config=None if issubclass(fieldinf, (CustomBaseModel, CustomRootModel)) else PYDANTIC_CONFIG)
  for field, fieldinf in get_annotations(ScheduledOrderDBEntryModel).items()
}
ORDER_LOG_TYPE_ADAPTERS = {
  field: TypeAdapter(fieldinf, config=None if issubclass(fieldinf, (CustomBaseModel, CustomRootModel)) else PYDANTIC_CONFIG)
  for field, fieldinf in get_annotations(OrderLogDBEntryModel).items()
}
