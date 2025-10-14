from collections.abc import Callable
from datetime import datetime, timedelta

from dateutil.utils import today as _today
from rich_custom import CustomTaskID, ProgressCustom


def paramiko_pbar_callback(pbar: ProgressCustom, task_id: CustomTaskID) -> Callable[[int, int], None]:
  def callback_transferred_bytes(transferred: int, total: int):
    pbar.update(task_id, completed=transferred, total=total)

  return callback_transferred_bytes


shift = timedelta()

# if __debug__:
#   # if debugging, calculate the difference between now and last saturday at 11:58pm
#   now = datetime.now()
#   last_saturday = now + relativedelta(weekday=SA(-1), hour=23, minute=58, second=0, microsecond=0)
#   shift = last_saturday - now


def today(tzinfo=None):
  """
  Returns a :py:class:`datetime` representing the current day at midnight

  :param tzinfo:
      The time zone to attach (also used to determine the current day).

  :return:
      A :py:class:`datetime.datetime` object representing the current day
      at midnight.
  """

  result = _today(tzinfo=tzinfo)

  result += shift

  return result


def get_now(tzinfo=None):
  """
  Returns a :py:class:`datetime` representing the current date and time

  :param tzinfo:
      The time zone to attach (also used to determine the current date and time).

  :return:
      A :py:class:`datetime.datetime` object representing the current date and time.
  """

  result = datetime.now(tz=tzinfo)

  result += shift

  return result
