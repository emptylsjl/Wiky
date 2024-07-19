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
from typing import TypeAlias
import mariadb

# import requests
# from bs4 import BeautifulSoup
# from base64 import b64encode, b64decode
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.remote.webelement import WebElement

import r

r.set_logger('wiky_db.log')
logger = r.get_logger('wiky_db_conns')

type Connection = mariadb.connections.Connection


class DBConn:
    def __init__(self, user, pwd="", host="localhost", port=3306, database=None):
        try:
            conn = mariadb.connect(
                user=user,
                password=pwd,
                host=host,
                port=port,
                database=database
            )
            self.conn = conn
            self.cursor = conn.cursor()

        except mariadb.Error as e:
            logger.error(f"connecting to MariaDB fail: {repr(user)}:{repr(e)} - {repr(traceback.format_exc())}")
            raise e

    def close(self):
        try:
            self.conn.close()
            logger.info("disconnected")
        except mariadb.Error as e:
            logger.error(f"disconnect fail: {e} - {traceback.format_exc()}")
            raise e


if __name__ == '__main__':
    # setup_index()
    #
    # wikitext = decode_page(550, 12)
    # print(wikitext.find("revision").find("text").text)
    # a.validate_index_dump()

    # print(a.zstd_len)
    pass
