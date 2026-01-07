#!/usr/bin/env python3
import datetime
import os
import sys
import sqlite3

import objc
import Quartz
from Foundation import NSURL


arg = sys.argv[1]
assert not (arg.startswith('.') or arg.startswith('/')) # mag[/issue.pdf]

if os.path.isdir(arg):
    root = arg.rstrip('/') # mag[/]
    filenames = sorted(os.listdir(root))
else:
    root, filename = arg.split('/')
    try:
        cmd = sys.argv[2]
    except IndexError:
        cmd = 'one'
    assert cmd in ('one', 'cont')
    if cmd == 'one':
        filenames = [filename]
    elif cmd == 'cont':
        filenames = sorted(os.listdir(root))
        filenames = filenames[filenames.index(filename):]
    else:
        print('unknown:', cmd)
        raise SystemExit
mag = root.lower()


conn = sqlite3.Connection('mag.db')
cursor = conn.cursor()


def pcworld_extractor(f):
    _, _, ym, _, y = f.split('_', 4)
    d = 1
    if '_' in y:
        y, bonus = y.split('_', 1)
        d = 2
    return int(y), int(ym[2:4]), d

def byte_extractor(f):
    y = int(f[:4])
    m = int(f[4:6])
    return y, m, 1

ymd_extractor = {
    'byte': byte_extractor, # 199602_Byte_Magazine_Vol_21-02...
    'pcmag': lambda f: f.split('-')[2:5],
    'pcworld': pcworld_extractor, # PC_World_9604_April_1996
}[mag]

for filename in filenames:
    if not filename.endswith('.pdf'):
        continue
    filepath = '/'.join((root, filename))
    ymd = list(map(int, ymd_extractor(filename.rsplit('.', 1)[0])))
    if len(ymd) == 2:
        ymd.append(1)
    date = datetime.date(*ymd)
    month = date.strftime('%Y-%m')

    print(f'Processing {mag} issue {filename} {date} {month}: ', end='')

    with objc.autorelease_pool():
        url = NSURL.fileURLWithPath_(filepath)
        pdf = Quartz.PDFDocument.alloc().initWithURL_(url)
        n = pdf.pageCount()
        print(f'{n} pages ...', flush=True)

        for i in range(n):
            page = pdf.pageAtIndex_(i)
            text = page.string() or ''
            cursor.execute('''
                INSERT INTO pages (magazine, issue, num, date, month, text)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            ''', (mag, filename, i + 1, date.isoformat(), month, text))
        conn.commit()
        del pdf
