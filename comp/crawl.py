#!/usr/bin/env python3
import sqlite3
from datetime import date

DB_PATH = "comp.db"

def month_range(conn):
    row = conn.execute("""
        SELECT min(month), max(month)
        FROM monthly_phrase_stats
    """).fetchone()
    return row[0], row[1]

def next_month(ym):
    y, m = map(int, ym.split("-"))
    if m == 12:
        return f"{y+1}-01"
    return f"{y}-{m+1:02d}"

OPEN_SOURCE_SQL = """
WITH ids AS (
  SELECT p.id
  FROM posts p
  JOIN posts_fts f ON f.rowid = p.id
  WHERE p.month = :month
    AND f MATCH '"open source"'
),
counts AS (
  SELECT
    SUM(
      ( length(txt) - length(replace(txt, 'open source', '')) )
      / length('open source')
    ) AS c
  FROM (
    SELECT lower(replace(p.subject || ' ' || p.body, '-', ' ')) AS txt
    FROM posts p
    JOIN ids ON ids.id = p.id
  )
)
UPDATE monthly_phrase_stats
SET open_source = COALESCE((SELECT c FROM counts), 0)
WHERE month = :month;
"""

FREE_SOFTWARE_SQL = """
WITH ids AS (
  SELECT p.id
  FROM posts p
  JOIN posts_fts f ON f.rowid = p.id
  WHERE p.month = :month
    AND f MATCH '"free software"'
),
counts AS (
  SELECT
    SUM(
      ( length(txt) - length(replace(txt, 'free software', '')) )
      / length('free software')
    ) AS c
  FROM (
    SELECT lower(replace(p.subject || ' ' || p.body, '-', ' ')) AS txt
    FROM posts p
    JOIN ids ON ids.id = p.id
  )
)
UPDATE monthly_phrase_stats
SET free_software = COALESCE((SELECT c FROM counts), 0)
WHERE month = :month;
"""

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    start, end = month_range(conn)
    month = start
    total = 0

    print(f"Processing months {start} →  {end}")

    while month <= end:
        print(f"  {month} …", end="", flush=True)
        conn.execute("BEGIN;")
        conn.execute(OPEN_SOURCE_SQL, {"month": month})
        conn.execute(FREE_SOFTWARE_SQL, {"month": month})
        conn.commit()
        print("  done")
        total += 1
        month = next_month(month)

    print(f"Finished {total} months")

if __name__ == "__main__":
    main()
