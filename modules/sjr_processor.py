"""
modules/sjr_processor.py
------------------------
Loads and normalises the SCImago Journal Rank (SJR) CSV file downloaded from:
  https://www.scimagojr.com/journalrank.php
  (click Export → downloads a semicolon-separated CSV)

Typical column names in the exported file:
  Rank | Sourceid | Title | Type | Issn | SJR | SJR Best Quartile |
  H index | Total Docs. (year) | … | Country | Region | Publisher | …
"""

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# Columns we actually need from the SJR file (case-insensitive matching applied later)
_SJR_REQUIRED = {
    "rank": "Rank",
    "title": "sjr_title",          # renamed to avoid clash with Zotero 'Title'
    "issn": "sjr_issn_raw",
    "sjr": "SJR Score",
    "sjr best quartile": "SJR Quartile",
    "h index": "H-index Journal",
}


def _normalize_issn(issn_raw: str) -> list[str]:
    """
    Converts a raw SJR ISSN cell into a list of canonical ISSNs.
    SJR stores multiple ISSNs as comma-separated values: "1234-5678, 9876-5432"
    Some entries omit the hyphen.  Canonical form: "XXXX-XXXX" (uppercase).
    """
    if not issn_raw or pd.isna(issn_raw):
        return []
    tokens = re.split(r"[\s,;]+", str(issn_raw).strip())
    results = []
    for token in tokens:
        digits = re.sub(r"[^0-9Xx]", "", token)
        if len(digits) == 8:
            results.append(f"{digits[:4]}-{digits[4:]}".upper())
    return results


def load_sjr(csv_path: str) -> pd.DataFrame:
    """
    Reads the SJR CSV, renames/selects relevant columns, and builds lookup
    structures for both ISSN-based and title-based matching.

    Returns
    -------
    pd.DataFrame with columns:
        sjr_title        – journal title (lowercase stripped for matching)
        sjr_title_raw    – original title as in CSV
        sjr_issn_list    – list of canonical ISSNs for this journal
        SJR Score        – float or N/A
        SJR Quartile     – e.g. "Q1", "Q2", …
        H-index Journal  – int or N/A
        Rank             – int or N/A
    """
    logger.info("Loading SJR database from: %s", csv_path)

    # The file uses semicolons as separators and may have encoding issues
    try:
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype=str)
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, sep=";", encoding="latin-1", dtype=str)

    logger.info("SJR raw shape: %s", df.shape)

    # Normalise column names to lowercase for robust matching
    df.columns = [c.strip().lower() for c in df.columns]

    # Verify required columns exist
    missing = [col for col in _SJR_REQUIRED if col not in df.columns]
    if missing:
        raise ValueError(
            f"SJR CSV is missing expected columns: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )

    # Rename to our internal names
    df = df.rename(columns=_SJR_REQUIRED)

    # Keep only what we need (plus the raw ISSN column we need to explode)
    df = df[list(_SJR_REQUIRED.values())].copy()

    # --- Clean SJR Score (may use comma as decimal separator in some locales) ---
    df["SJR Score"] = (
        df["SJR Score"]
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    df["SJR Score"] = pd.to_numeric(df["SJR Score"], errors="coerce")

    # --- Clean Rank and H-index ---
    df["Rank"] = pd.to_numeric(df["Rank"].str.strip(), errors="coerce")
    df["H-index Journal"] = pd.to_numeric(df["H-index Journal"].str.strip(), errors="coerce")

    # --- Quartile: ensure values like "Q1", "Q2", "-" → clean string or N/A ---
    df["SJR Quartile"] = df["SJR Quartile"].str.strip()
    df.loc[df["SJR Quartile"].isin(["-", "", "nan"]), "SJR Quartile"] = None

    # --- ISSN processing ---
    df["sjr_title_raw"] = df["sjr_title"].str.strip()
    # lowercase title for fuzzy comparison
    df["sjr_title"] = df["sjr_title_raw"].str.lower().str.strip()

    # Build a list of canonical ISSNs per row
    df["sjr_issn_list"] = df["sjr_issn_raw"].apply(_normalize_issn)

    # Drop the raw ISSN column (no longer needed)
    df = df.drop(columns=["sjr_issn_raw"])

    # Fill NaN with None so downstream code can use `is None` checks uniformly
    df = df.where(pd.notna(df), None)

    logger.info("SJR database loaded: %d journals.", len(df))
    return df


def build_issn_index(sjr_df: pd.DataFrame) -> dict[str, int]:
    """
    Builds a flat dict mapping each individual canonical ISSN → row index
    in sjr_df.  Allows O(1) lookup during the matching phase.

    When an ISSN appears in multiple rows (rare but possible in multi-year
    SJR exports), the last row wins (most recent entry is kept).
    """
    issn_to_idx: dict[str, int] = {}
    for idx, issn_list in enumerate(sjr_df["sjr_issn_list"]):
        for issn in issn_list:
            issn_to_idx[issn] = idx
    logger.info("ISSN index built: %d unique ISSNs.", len(issn_to_idx))
    return issn_to_idx
