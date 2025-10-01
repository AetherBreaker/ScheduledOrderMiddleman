from collections.abc import Callable

from rich_custom import CustomTaskID, ProgressCustom


def paramiko_pbar_callback(pbar: ProgressCustom, task_id: CustomTaskID) -> Callable[[int, int], None]:
  def callback_transferred_bytes(transferred: int, total: int):
    pbar.update(task_id, completed=transferred, total=total)

  return callback_transferred_bytes
