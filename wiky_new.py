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

# import requests
# from bs4 import BeautifulSoup
# from base64 import b64encode, b64decode
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.remote.webelement import WebElement

import r

# a = r.rt("C:/a/enwiki/enwiki-20240601-pages-articles-multistream-index.txt")


def decode_page(offset, pid, title=None, block_size=524288):
    decoder = bz2.BZ2Decompressor()
    raw_text = b""
    with open("C:/a/enwiki/enwiki-20240601-pages-articles-multistream.xml.bz2", "rb") as fd:
        fd.seek(int(offset))
        while True:
            bz2_chunk = fd.read(block_size)
            try:
                raw_text += decoder.decompress(bz2_chunk)
            except EOFError:
                break

    text = raw_text.decode("utf-8")
    root = ET.fromstring(f"<root>\n{text}\n</root>")
    for page in root.findall("page"):
        if int(pid) == int(page.find("id").text):
            return page

def setup_index():
    wiky_index_db = sqlite3.connect('wiky_index.db')
    cursor = wiky_index_db.cursor()

    wiky_index = r.rt("C:/a/enwiki/enwiki-20240601-pages-articles-multistream-index.txt")

    cursor.execute("""CREATE TABLE IF NOT EXISTS WikyIndex (
        Title VARCHAR(265) NOT NULL,
        Offset INTEGER NOT NULL,
        ID INTEGER NOT NULL,
        UNIQUE(Offset, ID) ON CONFLICT REPLACE
    );""")

    for i, line in enumerate(wiky_index.split("\n")):
        if line:
            try:
                offset, id, name = line.split(":", 2)

                cursor.execute(
                    '''INSERT INTO WikyIndex (Title, Offset, ID) VALUES (?, ?, ?)''',
                    (name, offset, id)
                )

                if (i+1) % 100 == 0:
                    wiky_index_db.commit()

            except:
                print(traceback.format_exc())

    # wiky_index_db.execute("DELETE from WikyIndex where ID=10")

    wiky_index_db.commit()
    cursor.close()
    wiky_index_db.close()


def setup_metadata(offset_id):

    wiky_index_db = sqlite3.connect('wiky_index.db')
    cursor = wiky_index_db.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS WikyIndex (
        OffsetID INTEGER NOT NULL,
        Namespace INTEGER NOT NULL,
        Redirect BOOLEAN NOT NULL,
        RedirectTitle BOOLEAN VARCHAR(265),
        Revision BOOLEAN NOT NULL,
        RevisionID INTEGER,
        Parentid INTEGER,
        Timestamp INTEGER,
    );""")

    wiky_index_db.commit()
    cursor.close()
    wiky_index_db.close()


def add_metadata(
        cursor,
        offset_id: int,
        ns: int,
        timestamp: int,
        redirect: str = None,
        revision: int = None,
        parent_id: int = None
):
    try:

        cursor.execute(
            '''INSERT INTO WikyIndex (OffsetID, Namespace, Redirect, RedirectTitle, Revision, RevisionID, Parentid, Timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (offset_id, ns, redirect is None, redirect, revision is None, revision, parent_id, timestamp)
        )
        # Commit the transaction
        cursor.connection.commit()

    except:
        print(f"Error adding row \n{traceback.format_exc()}")


def setup_user():

    wiky_index_db = sqlite3.connect('wiky_index.db')
    cursor = wiky_index_db.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS User (
        UserID INTEGER NOT NULL PRIMARY KEY,
        Username VARCHAR(30) NOT NULL,
        EmailAddr CHAR(50) NOT NULL,
        PasswordHash INTEGER NOT NULL,
        Description VARCHAR(50),
        Level INTEGER,
        Privilege INTEGER,
        TimeCreated INTEGER NOT NULL,
        PlayCount INTEGER NOT NULL,
        PageCount INTEGER NOT NULL,
        TotalRoll INTEGER NOT NULL,
        Currency0Count INTEGER NOT NULL,
        Currency1Count INTEGER NOT NULL,
        Status CHAR(30),
    );""")

    wiky_index_db.commit()
    cursor.close()
    wiky_index_db.close()


if __name__ == '__main__':
    setup_index()

    wikitext = decode_page(550, 12)
    print(wikitext.find("revision").find("text").text)