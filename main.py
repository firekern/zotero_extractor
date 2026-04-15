"""
main.py
-------
Pipeline orchestrator for the Zotero → SJR → Google Sheets pipeline.

Usage:
    python main.py

The script reads all configuration from environment variables (see .env).
Run `python main.py --help` for CLI override options.
"""

import argparse
import logging
import sys

# Force UTF-8 output on Windows (default console uses cp1252 which lacks many
# Unicode chars used in log messages like arrows and ellipses).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

import config
from modules.zotero_client import fetch_zotero_items
from modules.sjr_processor import load_sjr, build_issn_index
from modules.matcher import match_zotero_to_sjr
from modules.sheets_writer import write_to_sheets


# ---------------------------------------------------------------------------
# Logging setup — structured, timestamped output to stdout
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# CLI argument parser (all args are optional; defaults come from config.py)
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Zotero → SJR → Google Sheets pipeline"
    )
    p.add_argument(
        "--collection",
        default=config.ZOTERO_COLLECTION_KEY,
        help="Zotero collection key (empty = full library)",
    )
    p.add_argument(
        "--sjr-csv",
        default=config.SJR_CSV_PATH,
        help="Path to the SJR CSV file from SCImago",
    )
    p.add_argument(
        "--fuzzy-threshold",
        type=int,
        default=config.FUZZY_THRESHOLD,
        help="Minimum fuzzy match score 0-100 (default: 85)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline but skip writing to Google Sheets (prints preview instead)",
    )
    p.add_argument(
        "--output-csv",
        default="",
        help="Optional: also save the final DataFrame as a local CSV",
    )
    return p


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_pipeline(args: argparse.Namespace) -> pd.DataFrame:
    """
    Executes the full pipeline and returns the final DataFrame.
    Raises on unrecoverable errors; logs warnings for soft failures.
    """

    # -----------------------------------------------------------------------
    # Step 1: Extract Zotero items
    # -----------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 1 — Extracting Zotero library")
    logger.info("=" * 60)
    zotero_df = fetch_zotero_items(
        api_key=config.ZOTERO_API_KEY,
        user_id=config.ZOTERO_USER_ID,
        library_type=config.ZOTERO_LIBRARY_TYPE,
        collection_key=args.collection,
    )

    if zotero_df.empty:
        logger.warning("No items retrieved from Zotero. Aborting pipeline.")
        return pd.DataFrame()

    logger.info("Zotero items retrieved: %d", len(zotero_df))

    # -----------------------------------------------------------------------
    # Step 2: Load SJR database
    # -----------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 2 — Loading SJR database")
    logger.info("=" * 60)
    sjr_df = load_sjr(args.sjr_csv)
    issn_index = build_issn_index(sjr_df)

    # -----------------------------------------------------------------------
    # Step 3: Match Zotero items to SJR metrics
    # -----------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 3 -- Matching Zotero <-> SJR")
    logger.info("=" * 60)
    merged_df = match_zotero_to_sjr(
        zotero_df=zotero_df,
        sjr_df=sjr_df,
        issn_index=issn_index,
        fuzzy_threshold=args.fuzzy_threshold,
    )

    # -----------------------------------------------------------------------
    # Step 4: Post-processing & diagnostics
    # -----------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 4 — Post-processing")
    logger.info("=" * 60)

    # Matching quality report
    method_counts = merged_df["_match_method"].value_counts()
    logger.info("Match method distribution:\n%s", method_counts.to_string())

    # Items with no journal (e.g. books, conference papers) — expected
    no_pub_title = merged_df["Publication Title"].eq("").sum()
    if no_pub_title:
        logger.info(
            "%d item(s) have no Publication Title (books / conference papers / theses).",
            no_pub_title,
        )

    # Optional: save local CSV for inspection
    if args.output_csv:
        merged_df[config.OUTPUT_COLUMNS].to_csv(args.output_csv, index=False)
        logger.info("Local CSV saved to: %s", args.output_csv)

    # -----------------------------------------------------------------------
    # Step 5: Write to Google Sheets
    # -----------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 5 — Writing to Google Sheets")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN] Skipping Google Sheets write. Preview (first 5 rows):")
        print(merged_df[config.OUTPUT_COLUMNS].head().to_string(index=False))
    else:
        write_to_sheets(
            df=merged_df,
            output_columns=config.OUTPUT_COLUMNS,
            spreadsheet_id=config.SPREADSHEET_ID,
            worksheet_name=config.WORKSHEET_NAME,
            service_account_json=config.GOOGLE_SERVICE_ACCOUNT_JSON,
        )

    logger.info("Pipeline completed successfully. Total rows: %d", len(merged_df))
    return merged_df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = _build_parser()
    cli_args = parser.parse_args()
    run_pipeline(cli_args)
