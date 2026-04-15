"""
modules/sheets_writer.py
------------------------
Writes and formats the final DataFrame to Google Sheets.
Layout:
  Row 1        — disclaimer (merged, styled)
  Row 2        — empty separator
  Row 3        — header (frozen, dark background, white bold)
  Row 4+       — data rows (alternating colours)
  Last row + 2 — last-updated timestamp
"""

import logging
from datetime import datetime
from typing import Any

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DISCLAIMER = (
    "DISCLAIMER: I punteggi SJR, quartili e H-index sono estratti dal database "
    "SCImago Journal Rank e potrebbero non riflettere i valori piu aggiornati o "
    "corretti. Le metriche si riferiscono all'anno del file SJR utilizzato. "
    "Verificare sempre su scimagojr.com prima di citare questi dati in contesti "
    "accademici formali. Il match e' basato su ISSN o fuzzy-matching del titolo: "
    "controllare manualmente le righe con score SJR N/A."
)

# Column widths in pixels — order matches OUTPUT_COLUMNS in config.py
# Date Added | Year | Title | Publication Title | ISSN | DOI | Scopus Link |
# SJR Score | SJR Quartile | H-index Journal | Rank | Open Access Status | Comments
_COL_WIDTHS = [110, 55, 380, 210, 95, 210, 180, 80, 90, 90, 70, 130, 200]

# Colour palette
_C_HEADER_BG   = {"red": 0.106, "green": 0.173, "blue": 0.275}   # #1B2C46 navy
_C_HEADER_FG   = {"red": 1.0,   "green": 1.0,   "blue": 1.0}     # white
_C_ROW_ODD     = {"red": 0.933, "green": 0.945, "blue": 0.965}   # #EEF1F6 light blue-grey
_C_ROW_EVEN    = {"red": 1.0,   "green": 1.0,   "blue": 1.0}     # white
_C_DISCLAIMER  = {"red": 1.0,   "green": 0.976, "blue": 0.878}   # #FFF9E0 light yellow
_C_DIS_BORDER  = {"red": 0.945, "green": 0.714, "blue": 0.196}   # #F1B632 amber
_C_TIMESTAMP   = {"red": 0.6,   "green": 0.6,   "blue": 0.6}     # grey


def _rgb(c: dict) -> dict:
    return {"red": c["red"], "green": c["green"], "blue": c["blue"]}


def _get_client(service_account_json: str) -> gspread.Client:
    creds = Credentials.from_service_account_file(service_account_json, scopes=_SCOPES)
    return gspread.authorize(creds)


def _ensure_worksheet(spreadsheet: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        logger.info("Worksheet '%s' not found — creating it.", name)
        return spreadsheet.add_worksheet(title=name, rows=5000, cols=20)


def _df_to_values(df: pd.DataFrame, columns: list[str]) -> list[list[Any]]:
    rows = []
    for _, row in df[columns].iterrows():
        cells = []
        for col in columns:
            val = row[col]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                cells.append("N/A")
            else:
                cells.append(str(val))
        rows.append(cells)
    return rows


def _col_letter(n: int) -> str:
    """Convert 1-based column index to letter (1→A, 26→Z, 27→AA)."""
    result = ""
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _range(r1: int, c1: int, r2: int, c2: int) -> dict:
    """Build a GridRange dict (0-based)."""
    return {"startRowIndex": r1, "endRowIndex": r2,
            "startColumnIndex": c1, "endColumnIndex": c2}


def _border_side(style: str = "SOLID", width: int = 1, color: dict | None = None) -> dict:
    return {
        "style": style,
        "width": width,
        "color": _rgb(color or {"red": 0.8, "green": 0.8, "blue": 0.8}),
    }


def _apply_formatting(
    spreadsheet: gspread.Spreadsheet,
    worksheet: gspread.Worksheet,
    n_data_rows: int,
    n_cols: int,
) -> None:
    """
    Issues a single batchUpdate with all formatting requests.

    Layout rows (0-based):
      0  = disclaimer
      1  = empty separator
      2  = header
      3  = first data row
      3 + n_data_rows     = first empty row after data
      3 + n_data_rows + 1 = timestamp row
    """
    sheet_id = worksheet._properties["sheetId"]
    last_col = n_cols          # exclusive
    header_row  = 2            # 0-based row index
    data_start  = 3
    data_end    = data_start + n_data_rows   # exclusive
    ts_row      = data_end + 1

    requests = []

    # ── 1. Freeze row 3 (header) ───────────────────────────────────────────
    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": 3},
            },
            "fields": "gridProperties.frozenRowCount",
        }
    })

    # ── 2. Column widths ───────────────────────────────────────────────────
    for col_idx, px in enumerate(_COL_WIDTHS[:n_cols]):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": col_idx,
                    "endIndex": col_idx + 1,
                },
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })

    # ── 3. Disclaimer row (row 0): merge + style ───────────────────────────
    requests.append({
        "mergeCells": {
            "range": _range(0, 0, 1, last_col) | {"sheetId": sheet_id},
            "mergeType": "MERGE_ALL",
        }
    })
    requests.append({
        "repeatCell": {
            "range": _range(0, 0, 1, last_col) | {"sheetId": sheet_id},
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": _rgb(_C_DISCLAIMER),
                    "textFormat": {
                        "bold": False,
                        "italic": True,
                        "fontSize": 9,
                        "foregroundColor": {"red": 0.4, "green": 0.3, "blue": 0.0},
                    },
                    "wrapStrategy": "WRAP",
                    "verticalAlignment": "MIDDLE",
                    "borders": {
                        "top":    _border_side("SOLID_MEDIUM", 2, _C_DIS_BORDER),
                        "bottom": _border_side("SOLID_MEDIUM", 2, _C_DIS_BORDER),
                        "left":   _border_side("SOLID_MEDIUM", 2, _C_DIS_BORDER),
                        "right":  _border_side("SOLID_MEDIUM", 2, _C_DIS_BORDER),
                    },
                }
            },
            "fields": "userEnteredFormat",
        }
    })
    # Row 0 height
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 60},
            "fields": "pixelSize",
        }
    })

    # ── 4. Header row (row 2) ──────────────────────────────────────────────
    requests.append({
        "repeatCell": {
            "range": _range(header_row, 0, header_row + 1, last_col) | {"sheetId": sheet_id},
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": _rgb(_C_HEADER_BG),
                    "textFormat": {
                        "bold": True,
                        "fontSize": 10,
                        "foregroundColor": _rgb(_C_HEADER_FG),
                    },
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "borders": {
                        "bottom": _border_side("SOLID_MEDIUM", 2,
                                               {"red": 0.2, "green": 0.6, "blue": 0.9}),
                    },
                }
            },
            "fields": "userEnteredFormat",
        }
    })
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                      "startIndex": header_row, "endIndex": header_row + 1},
            "properties": {"pixelSize": 36},
            "fields": "pixelSize",
        }
    })

    # ── 5. Alternating data rows ───────────────────────────────────────────
    for i in range(n_data_rows):
        row_idx = data_start + i
        bg = _C_ROW_ODD if i % 2 == 0 else _C_ROW_EVEN
        requests.append({
            "repeatCell": {
                "range": _range(row_idx, 0, row_idx + 1, last_col) | {"sheetId": sheet_id},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": _rgb(bg),
                        "textFormat": {"fontSize": 10},
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                    }
                },
                "fields": "userEnteredFormat",
            }
        })

    # ── 6. Thin borders on all data cells ─────────────────────────────────
    if n_data_rows > 0:
        requests.append({
            "updateBorders": {
                "range": _range(data_start, 0, data_end, last_col) | {"sheetId": sheet_id},
                "innerHorizontal": _border_side("SOLID", 1),
                "innerVertical":   _border_side("SOLID", 1),
                "top":             _border_side("SOLID", 1),
                "bottom":          _border_side("SOLID_MEDIUM", 2,
                                                {"red": 0.6, "green": 0.6, "blue": 0.6}),
                "left":            _border_side("SOLID", 1),
                "right":           _border_side("SOLID", 1),
            }
        })

    # ── 7. Timestamp row ───────────────────────────────────────────────────
    requests.append({
        "repeatCell": {
            "range": _range(ts_row, 0, ts_row + 1, last_col) | {"sheetId": sheet_id},
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "italic": True,
                        "fontSize": 8,
                        "foregroundColor": _rgb(_C_TIMESTAMP),
                    },
                    "horizontalAlignment": "RIGHT",
                }
            },
            "fields": "userEnteredFormat",
        }
    })

    spreadsheet.batch_update({"requests": requests})
    logger.info("Formatting applied (%d requests).", len(requests))


def write_to_sheets(
    df: pd.DataFrame,
    output_columns: list[str],
    spreadsheet_id: str,
    worksheet_name: str,
    service_account_json: str,
) -> None:
    logger.info(
        "Connecting to Google Sheets (spreadsheet_id=%s, worksheet=%s)…",
        spreadsheet_id,
        worksheet_name,
    )
    client = _get_client(service_account_json)
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = _ensure_worksheet(spreadsheet, worksheet_name)

    n_cols = len(output_columns)
    last_col_letter = _col_letter(n_cols)

    data_rows = _df_to_values(df, output_columns)
    n_data = len(data_rows)
    ts = datetime.now().strftime("Ultimo aggiornamento: %d/%m/%Y %H:%M")

    # Full grid to write:
    #   row 1 (idx 0): disclaimer
    #   row 2 (idx 1): empty
    #   row 3 (idx 2): header
    #   rows 4+ (idx 3+): data
    #   empty row after data
    #   timestamp row
    values_to_write = (
        [[DISCLAIMER] + [""] * (n_cols - 1)],   # disclaimer
        [[""] * n_cols],                          # separator
        [output_columns],                         # header
        data_rows,                                # data
        [[""] * n_cols],                          # spacer
        [[""] * (n_cols - 1) + [ts]],            # timestamp (last col)
    )
    flat = []
    for block in values_to_write:
        flat.extend(block)

    logger.info("Writing %d data rows to Google Sheets…", n_data)
    worksheet.clear()
    worksheet.update(range_name="A1", values=flat)

    logger.info("Applying formatting…")
    try:
        _apply_formatting(spreadsheet, worksheet, n_data, n_cols)
    except Exception as e:
        logger.warning("Formatting step failed (data is still written): %s", e)

    logger.info("Google Sheets updated successfully.")
