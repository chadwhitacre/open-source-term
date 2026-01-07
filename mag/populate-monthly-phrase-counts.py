#!/usr/bin/env python3
import sqlite3
import re
import unicodedata
from collections import defaultdict
from datetime import datetime

# "unicode61-ish" tokenization: keep letters/digits/underscore as tokens
TOKEN_RE = re.compile(r"[0-9A-Za-z_]+", re.UNICODE)

# Dash characters to treat as separators (fold open-source into open source)
DASHES = "-\u2010\u2011\u2012\u2013\u2014\u2015"  # hyphen..horizontal bar + ASCII -

# Extra punctuation to treat as separators (optional, but helps approximate FTS)
# We'll translate these to spaces before tokenizing.
SEPARATORS_RE = re.compile(rf"[{re.escape(DASHES)}\u00AD]")  # include soft hyphen \u00AD


def strip_diacritics(s: str) -> str:
    # unicode61 default remove_diacritics=1-ish
    # Decompose then drop combining marks.
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if unicodedata.category(ch) != "Mn"
    )


def tokenize_unicode61ish(text: str) -> list[str]:
    # Normalize compatibility forms (ligatures, fullwidth, etc.)
    text = unicodedata.normalize("NFKC", text)
    # Casefold is closer to Unicode-aware case normalization than lower()
    text = text.casefold()
    # Remove diacritics
    text = strip_diacritics(text)
    # Turn dashes into spaces so "open-source" => "open source"
    text = SEPARATORS_RE.sub(" ", text)
    # Extract tokens
    return TOKEN_RE.findall(text)


def count_phrase(tokens: list[str], phrase: list[str]) -> int:
    # Count adjacent token-sequence matches (phrase occurrences)
    n = 0
    L = len(phrase)
    if L == 0 or len(tokens) < L:
        return 0
    # Simple sliding window
    for i in range(len(tokens) - L + 1):
        if tokens[i:i + L] == phrase:
            n += 1
    return n


def month_range_inclusive(min_month: str, max_month: str) -> list[str]:
    # min_month/max_month are 'YYYY-MM'
    start = datetime.strptime(min_month + "-01", "%Y-%m-%d")
    end = datetime.strptime(max_month + "-01", "%Y-%m-%d")

    months = []
    cur = start
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        # add 1 month
        year = cur.year + (cur.month // 12)
        month = (cur.month % 12) + 1
        cur = cur.replace(year=year, month=month)
    return months


def main(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Speed knobs (safe defaults; comment out if you dislike PRAGMA changes)
    cur.execute("PRAGMA temp_store = MEMORY;")
    cur.execute("PRAGMA cache_size = -200000;")  # ~200MB cache if available
    cur.execute("PRAGMA synchronous = NORMAL;")
    cur.execute("PRAGMA journal_mode = WAL;")

    # Get month range
    cur.execute("SELECT MIN(month), MAX(month) FROM pages;")
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        raise SystemExit("pages table is empty (no min/max month).")
    min_month, max_month = row[0], row[1]

    # Stats dict keyed by month
    stats = defaultdict(lambda: {"n": 0, "open_source": 0, "free_software": 0})

    OPEN_SOURCE = ["open", "source"]
    FREE_SOFTWARE = ["free", "software"]

    # Stream pages
    cur.execute("SELECT month, text FROM pages;")
    count = 0
    for month, text in cur:
        count += 1
        toks = tokenize_unicode61ish(text)
        stats[month]["n"] += 1
        stats[month]["open_source"] += count_phrase(toks, OPEN_SOURCE)
        stats[month]["free_software"] += count_phrase(toks, FREE_SOFTWARE)

        print(f"\rProcessed {count:,} pages...", end='')

    print()

    # Ensure every month has a row (fill zeros)
    all_months = month_range_inclusive(min_month, max_month)
    rows = []
    for m in all_months:
        v = stats[m]  # creates zeros if missing
        rows.append((m, v["n"], v["open_source"], v["free_software"]))

    # Write results
    cur.execute("DELETE FROM monthly_phrase_stats;")
    cur.executemany(
        """
        INSERT INTO monthly_phrase_stats (month, n, open_source, free_software)
        VALUES (?, ?, ?, ?);
        """,
        rows
    )
    conn.commit()

    print(f"Inserted {len(rows):,} months into monthly_phrase_stats "
          f"({min_month}..{max_month}).")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit(f"Usage: {sys.argv[0]} path/to/db.sqlite")
    main(sys.argv[1])

