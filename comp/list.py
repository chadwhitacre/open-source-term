#!/usr/bin/env python3
import os, sys

root = sys.argv[1]
for filename in sorted(os.listdir(root)):
    if not filename.endswith('.mbox'):
        continue
    print(filename)
