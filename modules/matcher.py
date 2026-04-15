"""
modules/matcher.py
------------------
Joins Zotero items with SJR metrics using a two-stage matching strategy:

  Stage 1 – ISSN match (exact, after normalisation)
    → Fast O(1) lookup via pre-built index.
    → Tries every ISSN variant stored for the Zotero item.

  Stage 2 – Fuzzy title match (fallback)
    → Used only when Stage 1 fails (item has no ISSN, or ISSN not in SJR).
    → Uses RapidFuzz (fast C-backed implementation of Levenshtein ratio).
    → Applies a configurable confidence threshold to avoid false positives.

Result columns added to the Zotero DataFrame:
    SJR Score | SJR Quartile | H-index Journal | Rank | _match_method
"""

import logging

import pandas as pd

# rapidfuzz is the maintained successor of python-Levenshtein + thefuzz;
# it exposes the same API but is significantly faster.
try:
    from rapidfuzz import process as fuzz_process
    from rapidfuzz import fuzz
    _FUZZ_BACKEND = "rapidfuzz"
except ImportError:
    # Graceful fallback to thefuzz (slower but compatible)
    from thefuzz import process as fuzz_process  # type: ignore[no-redef]
    from thefuzz import fuzz                      # type: ignore[no-redef]
    _FUZZ_BACKEND = "thefuzz"

logger = logging.getLogger(__name__)

# SJR columns we want to copy into the output DataFrame
_SJR_COLS = ["SJR Score", "SJR Quartile", "H-index Journal", "Rank"]


def _format_metric(value) -> str:
    """Converts a metric value to a clean string, or 'N/A' if missing.
    Handles: None, float NaN, strings (e.g. 'Q1'), floats, ints.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    # Strings (e.g. SJR Quartile "Q1", "Q2") pass through unchanged
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else "N/A"
    # Float: keep 3 decimal places
    if isinstance(value, float):
        return f"{value:.3f}"
    # Int or anything else
    return str(int(value))


def _issn_lookup(
    issn_list: list[str],
    issn_index: dict[str, int],
    sjr_df: pd.DataFrame,
) -> dict | None:
    """
    Tries each ISSN in `issn_list` against the pre-built index.
    Returns a dict of SJR metrics on first hit, or None.
    """
    for issn in issn_list:
        idx = issn_index.get(issn)
        if idx is not None:
            row = sjr_df.iloc[idx]
            return {col: row[col] for col in _SJR_COLS}
    return None


def _fuzzy_lookup(
    pub_title: str,
    sjr_titles: list[str],
    sjr_df: pd.DataFrame,
    threshold: int,
) -> tuple[dict | None, int]:
    """
    Uses token-sort ratio fuzzy matching to find the best matching journal
    title in the SJR database.

    token_sort_ratio is preferred over simple ratio because it handles
    word-order differences gracefully (e.g. "Journal of X" vs "X, Journal of").

    Returns
    -------
    (metrics_dict | None, score)
        metrics_dict is None when no match exceeds `threshold`.
    """
    if not pub_title or not pub_title.strip():
        return None, 0

    query = pub_title.strip().lower()

    # fuzz_process.extractOne returns (match_string, score, index)
    result = fuzz_process.extractOne(
        query,
        sjr_titles,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
    )

    if result is None:
        return None, 0

    _matched_title, score, matched_idx = result
    row = sjr_df.iloc[matched_idx]
    metrics = {col: row[col] for col in _SJR_COLS}
    return metrics, int(score)


def match_zotero_to_sjr(
    zotero_df: pd.DataFrame,
    sjr_df: pd.DataFrame,
    issn_index: dict[str, int],
    fuzzy_threshold: int = 85,
) -> pd.DataFrame:
    """
    Main matching function.  Annotates each Zotero row with SJR metrics.

    Parameters
    ----------
    zotero_df      : DataFrame from zotero_client.fetch_zotero_items()
    sjr_df         : DataFrame from sjr_processor.load_sjr()
    issn_index     : dict from sjr_processor.build_issn_index()
    fuzzy_threshold: minimum fuzzy score (0-100) to accept a title match

    Returns
    -------
    Annotated DataFrame with SJR columns added.
    """
    logger.info(
        "Starting matching (%d Zotero items, %d SJR journals, fuzzy_threshold=%d, backend=%s).",
        len(zotero_df),
        len(sjr_df),
        fuzzy_threshold,
        _FUZZ_BACKEND,
    )

    # Pre-build the list of SJR titles for vectorised fuzzy search
    # (fuzz_process.extractOne scans this list)
    sjr_titles: list[str] = sjr_df["sjr_title"].tolist()

    # Counters for reporting
    n_issn = n_fuzzy = n_none = 0

    # Lists to accumulate results (avoids repeated DataFrame row assignment)
    results: list[dict] = []

    for _, row in zotero_df.iterrows():
        issn_list: list[str] = row.get("_issn_list", []) or []
        pub_title: str = row.get("Publication Title", "") or ""

        # ---- Stage 1: ISSN match ----------------------------------------
        metrics = _issn_lookup(issn_list, issn_index, sjr_df)
        if metrics is not None:
            metrics["_match_method"] = "issn"
            n_issn += 1
            results.append(metrics)
            continue

        # ---- Stage 2: Fuzzy title match ----------------------------------
        metrics, score = _fuzzy_lookup(pub_title, sjr_titles, sjr_df, fuzzy_threshold)
        if metrics is not None:
            metrics["_match_method"] = f"fuzzy({score})"
            n_fuzzy += 1
            results.append(metrics)
            continue

        # ---- No match found ----------------------------------------------
        results.append({
            "SJR Score": None,
            "SJR Quartile": None,
            "H-index Journal": None,
            "Rank": None,
            "_match_method": "none",
        })
        n_none += 1

    logger.info(
        "Matching complete -> ISSN: %d | Fuzzy: %d | No match: %d",
        n_issn, n_fuzzy, n_none,
    )

    # Merge results back into the original DataFrame
    metrics_df = pd.DataFrame(results, index=zotero_df.index)
    merged = pd.concat([zotero_df, metrics_df], axis=1)

    # Format numeric metrics as clean strings (or 'N/A')
    for col in _SJR_COLS:
        merged[col] = merged[col].apply(_format_metric)

    return merged
