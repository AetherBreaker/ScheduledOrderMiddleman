# sourcery skip: raise-from-previous-error
if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import sys
from logging import getLogger
from pathlib import Path
from zoneinfo import ZoneInfo

from environment_settings import Settings

logger = getLogger(__name__)


# Settings
SETTINGS = Settings()  # type: ignore

# Folder paths
CWD = Path.cwd()

SPEC_CWD = Path(__file__).parent if getattr(sys, "frozen", False) else Path.cwd()

GOOGLE_API_KEY_FILE = SPEC_CWD / "db-key.json"

SFT_WEBSITE_CREDS_FILE = SPEC_CWD / "sft_creds.json"

SAS_FTP_CREDS_FILE = SPEC_CWD / "sas_ftp_creds.json"

TZ = ZoneInfo("US/Eastern")


if not GOOGLE_API_KEY_FILE.exists():
  raise FileNotFoundError(
    "Google API key file 'db-key.json' not found in the current directory.\n"
    "Please create a service account key in the Google Cloud Console "
    "and save it as 'db-key.json' in the current directory."
  )
