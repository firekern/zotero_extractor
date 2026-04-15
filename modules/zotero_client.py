"""
modules/zotero_client.py
------------------------
Handles all interaction with the Zotero API via pyzotero.
Returns a clean pandas DataFrame ready for the matching stage.
"""

import re
import logging
from datetime import datetime

import pandas as pd
from pyzotero import zotero

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_year(date_str: str) -> str:
    """
    Robustly extracts a 4-digit year from heterogeneous Zotero date strings.
    Handles: "2023", "2023-05-01", "May 2023", "01/05/2023", etc.
    Returns "N/A" when no year is found.
    """
    if not date_str:
        return "N/A"
    match = re.search(r"\b(19|20)\d{2}\b", str(date_str))
    return match.group(0) if match else "N/A"


def _parse_open_access(item_data: dict) -> str:
    """
    Attempts to detect Open Access status from the Zotero item.
    Zotero has no dedicated OA field, but several plugins (Unpaywall,
    OA Button, Zotero-OA) write information into `extra` or `tags`.

    Priority:
      1. 'extra' field  → looks for lines like "Open Access: true"
      2. Tags           → looks for tags containing "open access" (case-insensitive)
      3. Falls back to "N/A"
    """
    extra: str = item_data.get("extra", "") or ""
    for line in extra.splitlines():
        if re.match(r"open\s+access\s*:", line, re.IGNORECASE):
            value = line.split(":", 1)[1].strip().lower()
            if value in ("true", "yes", "1", "open"):
                return "Open Access"
            if value in ("false", "no", "0", "closed"):
                return "Closed Access"
            return value.capitalize()  # e.g. "bronze", "green", "gold"

    tags: list[dict] = item_data.get("tags", [])
    for tag in tags:
        tag_val = tag.get("tag", "").lower()
        if "open access" in tag_val or "openaccess" in tag_val:
            return "Open Access"

    return "N/A"


def _normalize_issn(issn_raw: str) -> list[str]:
    """
    Cleans and normalises an ISSN string into a list of canonical ISSNs.
    Canonical form: 8 digits with a hyphen, e.g. '1234-5678'.

    Zotero stores ISSNs in the 'ISSN' field as a single string that may
    contain one or multiple ISSNs separated by spaces or commas.

    Examples handled:
      "12345678"        → ["1234-5678"]
      "1234-5678"       → ["1234-5678"]
      "1234-5678 9876-5432" → ["1234-5678", "9876-5432"]
    """
    if not issn_raw:
        return []
    # Split on common separators (space, comma, semicolon)
    tokens = re.split(r"[\s,;]+", issn_raw.strip())
    results = []
    for token in tokens:
        digits = re.sub(r"[^0-9Xx]", "", token)  # keep digits + check digit X
        if len(digits) == 8:
            results.append(f"{digits[:4]}-{digits[4:]}".upper())
    return results


def _build_scopus_link(doi: str) -> str:
    """
    Builds a Scopus search URL from a DOI.
    Opens Scopus pre-filtered on that exact DOI — works without a direct EID.
    Returns 'N/A' when no DOI is available.
    """
    if not doi:
        return "N/A"
    return f"https://www.scopus.com/search/results.uri?query=DOI%28{doi}%29&origin=searchbasic"


def _parse_item(item: dict) -> dict | None:
    """
    Extracts the relevant fields from a raw Zotero item dict.
    Returns None for item types we don't care about (attachments, notes).
    """
    item_type = item.get("data", {}).get("itemType", "")
    # Skip non-research items
    if item_type in ("attachment", "note", "annotation"):
        return None

    data = item["data"]

    # ISSN: store both the raw string and the normalised list
    issn_raw = data.get("ISSN", "") or ""
    issn_normalised = _normalize_issn(issn_raw)
    # Primary ISSN for display (first one found, or N/A)
    issn_display = issn_normalised[0] if issn_normalised else "N/A"

    return {
        "zotero_key": item.get("key", ""),
        "Date Added": (data.get("dateAdded", "") or "")[:10],  # YYYY-MM-DD
        "Year": _extract_year(data.get("date", "")),
        "Title": (data.get("title", "") or "").strip(),
        "Publication Title": (data.get("publicationTitle", "") or "").strip(),
        "ISSN": issn_display,
        # Internal: full normalised ISSN list for matching
        "_issn_list": issn_normalised,
        "DOI": (data.get("DOI", "") or "").strip() or "N/A",
        "Scopus Link": _build_scopus_link((data.get("DOI", "") or "").strip()),
        "Open Access Status": _parse_open_access(data),
        "Comments": "",  # always empty — for user notes
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_zotero_items(
    api_key: str,
    user_id: str,
    library_type: str = "user",
    collection_key: str = "",
) -> pd.DataFrame:
    """
    Connects to Zotero and downloads all items (or items in a collection).

    Parameters
    ----------
    api_key        : Zotero API key
    user_id        : Zotero user/group ID
    library_type   : "user" or "group"
    collection_key : Zotero collection key (empty = entire library)

    Returns
    -------
    pd.DataFrame with one row per research item, columns matching OUTPUT_COLUMNS
    plus internal helper columns prefixed with "_".
    """
    logger.info("Connecting to Zotero (library_type=%s, user_id=%s)…", library_type, user_id)
    zot = zotero.Zotero(user_id, library_type, api_key)

    # Pyzotero paginates automatically; everything() fetches all pages
    if collection_key:
        logger.info("Fetching collection: %s", collection_key)
        raw_items = zot.everything(zot.collection_items(collection_key))
    else:
        logger.info("Fetching entire library…")
        raw_items = zot.everything(zot.items())

    logger.info("Downloaded %d raw items from Zotero.", len(raw_items))

    records = []
    skipped = 0
    for item in raw_items:
        parsed = _parse_item(item)
        if parsed is None:
            skipped += 1
            continue
        records.append(parsed)

    logger.info(
        "Parsed %d research items (%d attachments/notes skipped).",
        len(records),
        skipped,
    )

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    return df
