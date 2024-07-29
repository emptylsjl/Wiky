

import base64
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import time
import hashlib
import traceback
from collections import defaultdict
from pathlib import Path
import os
import binascii
import secrets
import bz2
import xml.etree.ElementTree as ET
import sqlite3
import mariadb


# full_metas = [
#     "E:/dt/other/wiki/20240601/enwiki-20240601-stub-meta-history1.xml",
#     "E:/dt/other/wiki/20240601/enwiki-20240601-stub-meta-history2.xml",
#     "E:/dt/other/wiki/20240601/enwiki-20240601-stub-meta-history3.xml",
#     "E:/dt/other/wiki/20240601/enwiki-20240601-stub-meta-history4.xml",
#     "E:/dt/other/wiki/20240601/enwiki-20240601-stub-meta-history5.xml",
#     "E:/dt/other/wiki/20240601/enwiki-20240601-stub-meta-history6.xml",
# ]
#
# for i in full_metas:
#     with open(i, 'rb') as fd:
#         fd.seek(-2252, os.SEEK_END)
#         a = fd.read(2252)
#         print(a.decode())
#         input("?")
