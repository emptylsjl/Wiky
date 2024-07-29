
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
import uuid
import pprint
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import os
import binascii
import secrets
import bz2
import xml.etree.ElementTree as ET
import sqlite3
import mariadb

# import requests
# from bs4 import BeautifulSoup
# from base64 import b64encode, b64decode
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.remote.webelement import WebElement

import r
import setup
from py.wiky_conn import DBConn
from action import *

r.set_logger('wiky_db.log')
logger = r.get_logger('wiky_db_test  ')


def test():
    setup.setup()

    db_a = DBConn("root2", database="wiky_base")
    add_item_list(db_a, "it0")
    add_item_list(db_a, "it1")
    add_item_list(db_a, "it2", "yanked")
    add_shop_item(db_a, 1, 5)
    add_shop_item(db_a, "it1", 6)
    add_shop_item(db_a, "it2", 7)

    db = DBConn("root", database="wiky_base")

    register_wiky(db, "bc0", "石の猫", "_32890*(", "ha@gmia.com")
    register_wiky(db, "ab8", "cat", "_32890*(", "ha@gm@a.com")
    register_wiky(db, "ab8", "cat", "_32890*(", "ha@gmia.com")
    register_wiky(db, "0oic", "wha", "_3dsjaklのの_{90*(", "ha@gmia.com")

    status, wiky_session = wiky_auth(db, "ab8", "_3dsjaklのの_{90*(")
    status, wiky_session = wiky_auth(db, "ab8", "_32890*(")

    create_wiky_profile(db, wiky_session)
    ok, info = get_user_info(db, wiky_session)
    pprint.pprint(info)

    add_balance(db, wiky_session, 200)
    user_request(db, wiky_session.name, "0oi")
    user_request(db, wiky_session.name, "0oic")

    pull_wiky(db, wiky_session, 40)
    item_purchase(db, wiky_session, 1, 4)
    item_purchase(db, wiky_session, "it1", 5)
    item_purchase(db, wiky_session, "it2", 10)

    ok, info, pulls = get_user_info(db, wiky_session)

    pprint.pprint(info)
    pprint.pprint(pulls)

    logout(db, wiky_session)


if __name__ == '__main__':
    test()

