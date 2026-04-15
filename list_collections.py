"""
list_collections.py
-------------------
Utility: prints all Zotero collections with their key and item count.
Run this once to find the ZOTERO_COLLECTION_KEY you want to use.

Usage:
    uv run py list_collections.py
"""

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from pyzotero import zotero
import config


def list_collections():
    zot = zotero.Zotero(config.ZOTERO_USER_ID, config.ZOTERO_LIBRARY_TYPE, config.ZOTERO_API_KEY)
    collections = zot.everything(zot.collections())

    if not collections:
        print("No collections found.")
        return

    # Sort alphabetically by name for readability
    collections.sort(key=lambda c: c["data"].get("name", "").lower())

    print(f"\n{'KEY':<12} {'ITEMS':>6}  NAME")
    print("-" * 60)
    for col in collections:
        key   = col["key"]
        name  = col["data"].get("name", "(unnamed)")
        count = col["meta"].get("numItems", "?")
        print(f"{key:<12} {str(count):>6}  {name}")

    print(f"\nTotal collections: {len(collections)}")
    print("\nCopy the KEY of the collection you want and set it in .env:")
    print("  ZOTERO_COLLECTION_KEY=<KEY>")


if __name__ == "__main__":
    list_collections()
