"""
config.py
---------
Centralizes all configuration parameters for the pipeline.
Values are read from environment variables (via .env file) to keep
credentials out of source code.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env into os.environ

# ---------------------------------------------------------------------------
# Base directories
# ---------------------------------------------------------------------------
# Root folder that contains input data files (SJR CSV, etc.).
# Defaults to the "data/" subfolder next to this script.
# Override with DATA_DIR=/absolute/path/to/your/folder in .env
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data")).expanduser().resolve()

# Root folder for credential files (service_account.json, etc.).
# Defaults to "credentials/" next to this script.
CREDENTIALS_DIR: Path = Path(os.getenv("CREDENTIALS_DIR", "credentials")).expanduser().resolve()

# ---------------------------------------------------------------------------
# Zotero
# ---------------------------------------------------------------------------
ZOTERO_API_KEY: str = os.environ["ZOTERO_API_KEY"]
ZOTERO_USER_ID: str = os.environ["ZOTERO_USER_ID"]

# "user" for personal library, "group" for shared group library
ZOTERO_LIBRARY_TYPE: str = os.getenv("ZOTERO_LIBRARY_TYPE", "user")

# Leave empty ("") to fetch the entire library, or set a Zotero collection key
# e.g. "ABC12DEF" — visible in the URL when you open a collection on zotero.org
ZOTERO_COLLECTION_KEY: str = os.getenv("ZOTERO_COLLECTION_KEY", "")

# ---------------------------------------------------------------------------
# SCImago / SJR
# ---------------------------------------------------------------------------
# Filename (or absolute path) of the CSV from scimagojr.com.
# If you set only a filename (e.g. "scimagojr 2024.csv"), the file is looked
# up inside DATA_DIR.  You can also override with a full absolute path.
_sjr_raw: str = os.getenv("SJR_CSV_PATH", "sjr.csv")
SJR_CSV_PATH: str = str(
    Path(_sjr_raw) if Path(_sjr_raw).is_absolute() else DATA_DIR / _sjr_raw
)

# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------
# Filename (or absolute path) of the service-account JSON from Google Cloud.
# If only a filename is given, it is resolved inside CREDENTIALS_DIR.
_sa_raw: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
GOOGLE_SERVICE_ACCOUNT_JSON: str = str(
    Path(_sa_raw) if Path(_sa_raw).is_absolute() else CREDENTIALS_DIR / _sa_raw
)

# The full URL or just the spreadsheet ID from the Sheets URL:
# https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit
SPREADSHEET_ID: str = os.environ["SPREADSHEET_ID"]

# Name of the worksheet tab to write to (will be created if absent)
WORKSHEET_NAME: str = os.getenv("WORKSHEET_NAME", "Zotero Library")

# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------
# Minimum fuzzy-match score (0-100) to accept a journal title match
# 100 = perfect match, lower values = more permissive (more false positives)
FUZZY_THRESHOLD: int = int(os.getenv("FUZZY_THRESHOLD", "85"))

# ---------------------------------------------------------------------------
# Output columns (order defines column order in Google Sheets)
# ---------------------------------------------------------------------------
OUTPUT_COLUMNS: list[str] = [
    "Date Added",
    "Year",
    "Title",
    "Publication Title",
    "ISSN",
    "DOI",
    "Scopus Link",
    "SJR Score",
    "SJR Quartile",
    "H-index Journal",
    "Rank",
    "Open Access Status",
    "Comments",
]
