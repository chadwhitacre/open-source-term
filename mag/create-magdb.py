#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('mag.db')

conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA foreign_keys=ON;")

# Main content table
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS pages (
        id          INTEGER PRIMARY KEY,
        magazine    TEXT NOT NULL,
        issue       TEXT NOT NULL,
        num         INTEGER NOT NULL,   -- 1-based
        date        TEXT,               -- YYYY-MM-DD
        month       TEXT NOT NULL,      -- YYYY-MM
        text        TEXT NOT NULL,
        UNIQUE(magazine, issue, num)
    );
    """
)

conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_month ON pages(month);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_date ON pages(date);")

# FTS virtual table with external content for fast search
conn.execute(
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts
    USING fts5(
        text,
        content='pages',
        content_rowid='id'
    );
    """
)

# Keep FTS in sync with pages
conn.execute(
    """
    CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
        INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
    END;
    """
)
conn.execute(
    """
    CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
        INSERT INTO pages_fts(pages_fts, rowid, text) VALUES('delete', old.id, old.text);
    END;
    """
)
conn.execute(
    """
    CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
        INSERT INTO pages_fts(pages_fts, rowid, text) VALUES('delete', old.id, old.text);
        INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
    END;
    """
)
