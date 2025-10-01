if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from logging import getLogger
from typing import TypedDict

from gspread.utils import DateTimeOption, Dimension, ValueInputOption, ValueRenderOption

logger = getLogger(__name__)

type StoreNum = int
type CustomerID = str | int
type InvoiceNum = str | int


type SheetsValue = int | float | str
type SheetsRangeName = str
type SheetsRangeUpdateValues = list[list[SheetsValue]]


class ValueRange(TypedDict):
  range: SheetsRangeName
  majorDimension: Dimension
  values: SheetsRangeUpdateValues


class ValuesBatchUpdateBody(TypedDict):
  valueInputOption: ValueInputOption
  includeValuesInResponse: bool
  responseValueRenderOption: ValueRenderOption
  responseDateTimeRenderOption: DateTimeOption
  data: list[ValueRange]
