#!/usr/bin/env python3
import datetime
import mailbox
import os
import sys
import sqlite3
from email.utils import parsedate_to_datetime


def body_from_message(msg):
    """Return a best-effort unicode string containing subject + body text."""
    parts = []

    # Body
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = part.get_content_disposition()
            if disp == "attachment":
                continue
            # Prefer text/plain; skip HTML by default (can be added if you want)
            if ctype == "text/plain":
                try:
                    parts.append(part.get_content())
                except Exception:
                    payload = part.get_payload(decode=True) or b""
                    parts.append(payload.decode(errors="replace"))
    else:
        ctype = msg.get_content_type()
        if ctype == "text/plain":
            try:
                parts.append(msg.get_content())
            except Exception:
                payload = msg.get_payload(decode=True) or b""
                parts.append(payload.decode(errors="replace"))
        else:
            # If single-part but not text/plain, still try to decode as text
            payload = msg.get_payload(decode=True) or b""
            parts.append(payload.decode(errors="replace"))

    return "\n".join(parts)


class BadDate(Exception):
    pass

class MissingDate(Exception):
    pass

def parse_one(msg):
    msgid = msg.get('message-id', '').encode()

    if not msg['date']:
        raise MissingDate()
    try:
        date = parsedate_to_datetime(msg['date']).date()
    except:
        try:
            # Tue, 11 Apr 1995 17:09:50 -29900
            no_tz = msg['date'].split(' -')[0].split(' +')[0]
            date = parsedate_to_datetime(no_tz).date()
        except:
            try:
                date = msg['date']
                if '/' in date:
                    ymd = date.split('/')
                elif '-' in date:
                    ymd = date.split('-')
                date = datetime.date(*map(int, ymd))
            except:
                raise BadDate()
    date = date.isoformat()
    date_raw = msg['date'].encode()

    subject = msg.get("subject", '').encode()
    body = body_from_message(msg)
    return (msgid, date, date_raw, subject, body)


def load_mbox_into_db(mbox_path, db, ntotal, nbad, bad_log):
    """Load messages from an file into the db.
    """
    newsgroup = mbox_path.split('/')[-1]
    for msg in mailbox.mbox(mbox_path):
        ntotal += 1
        try:
            parsed = parse_one(msg)
        except MissingDate: # nothing to be done for it
            nbad += 1
            continue
        except BadDate:     # improve parsing if we can
            print(msg['date'], file=bad_log, flush=True)
            nbad += 1
            continue

        msgid, date, date_raw, subject, body = parsed
        db.cursor().execute('''
            INSERT INTO posts (msgid, newsgroup, date, date_raw, subject, body)
            VALUES (?, ?, ?, ?, ?, ?)''',
            (msgid, newsgroup, date, date_raw, subject, body)
        )
        db.commit()
        progress = f'{date} - {(nbad / ntotal) * 100:.01f} - {newsgroup}'
        print(f'\r{progress.ljust(80)}', end='')
    print()
    return (ntotal, nbad)


def main():
    db = sqlite3.connect('comp.db')
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id          INTEGER PRIMARY KEY,
        msgid       TEXT        NOT NULL,
        newsgroup   TEXT        NOT NULL,
        date        DATETIME    NOT NULL,
        date_raw    TEXT        NOT NULL,
        subject     TEXT        NOT NULL,
        body        TEXT        NOT NULL
    )
    ''')

    ntotal = nbad = 0
    root = sys.argv[1]
    start_with = sys.argv[2]
    bad_log = open('bad.log', 'w+')
    filenames = sorted(os.listdir(root))
    for filename in filenames:
        if not filename.endswith('.mbox'):
            continue
        if filename < start_with:
            continue
        filepath = '/'.join([root, filename])
        ntotal, nbad = load_mbox_into_db(filepath, db, ntotal, nbad, bad_log)

if __name__ == "__main__":
    main()
