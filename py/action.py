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
from py.wiky_conn import DBConn

r.set_logger('wiky_db.log')
logger = r.get_logger('wiky_db_action')


def validate_mail_str(mail: str, size) -> bool:
    valid = len(mail) <= size and bool(re.fullmatch(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b',
        mail
    ))
    if not valid:
        logger.warning(f'validate_mail:{repr(mail)}')
    return valid


def validate_phone_str(phone: str, size) -> bool:
    valid = len(phone) <= size and bool(re.fullmatch(r'\+[0-9]*$', phone))
    if not valid:
        logger.warning(f'validate_phone:{repr(phone)}')
    return valid


def ensure_char(text: str, size) -> bool:
    valid = len(text) <= size and bool(re.match(r'^[a-zA-Z0-9_.-]*$', text))
    if not valid:
        logger.warning(f'ensure_char:{repr(text)}')
    return valid


def send_mail():
    pass


def register_wiky(
        db: DBConn,
        username: str,
        display_name: str,
        pwd: str,
        mail: str,
        phone: str = None,
) -> (bool, str):

    valid = ensure_char(username, 20)
    valid = valid and ensure_char(display_name, 20)
    valid = valid and validate_phone_str(username, 16)
    valid = valid and validate_mail_str(username, 50)
    if not valid:
        logger.error(f'register_wiky invalid:{repr(username)}')
        return False
    pwd_hash = r.sha265(pwd)

    try:
        db.conn.begin()

        db.cursor.execute("SELECT incr_id FROM wiky_user_account WHERE username = %s", (username,))
        if (wiky_incr_id_row := db.cursor.fetchone()) is not None:
            logger.error(f"register_wiky error: {repr(username)} - wiky_acc_exist")
            return (False, "wiky_acc_exist")

        db.cursor.execute("SELECT incr_id FROM uni_account WHERE username = %s", (username,))
        if (uni_incr_id_row := db.cursor.fetchone()) is None:
            db.cursor.execute(
                '''
                INSERT INTO uni_account 
                (uni_uuid, username, display_name, pwd_hash, mail, phone, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''.strip(),
                (uuid.uuid4().bytes, username, display_name, pwd_hash, mail, phone, "pending")
            )
            incr_id = db.cursor.lastrowid
        else:
            uni_acc_exist = True
            incr_id = uni_incr_id_row[0]

        db.cursor.execute(
            '''
            INSERT INTO wiky_user_account 
            (uni_incr_id, service_id, user_uuid, username, display_name, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            '''.strip(),
            (incr_id, 1, uuid.uuid4().bytes, username, display_name, "pending")
        )

        send_mail()
        db.conn.commit()
        logger.info(f"register_uni ok: {repr(username)}:{repr(display_name)}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"register_wiky error: {repr(username)}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"register_wiky rollback: {repr(username)}")
        return (False, f"error - {repr(e)}")

    return (True, "ok")


def register_uni(
        db: DBConn,
        username: str,
        display_name: str,
        pwd: str,
        mail: str,
        phone: str = None,
) -> bool:

    valid = ensure_char(username, 20)
    valid = valid and ensure_char(display_name, 20)
    valid = valid and validate_phone_str(username, 16)
    valid = valid and validate_mail_str(username, 50)

    if not valid:
        logger.error(f'register_uni invalid:{repr(username)}')
        return False

    pwd_hash = r.sha265(pwd)

    try:
        db.conn.begin()
        db.cursor.execute(
            '''
            INSERT INTO uni_account 
            (uni_uuid, username, display_name, pwd_hash, mail, phone, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''.strip(),
            (uuid.uuid4().bytes, username, display_name, pwd_hash, mail, phone, "pending")
        )
        send_mail()
        db.conn.commit()
        logger.info(f"register_uni ok: {repr(username)}:{repr(display_name)}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"register_uni error: {repr(username)}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"register_uni rollback: {repr(username)}")
        return False

    return True


class WikySession:
    def __init__(self, uni_incr, wiky_incr, wiky_uuid, name, token, now):
        self.uni_incr = uni_incr
        self.wiky_incr = wiky_incr
        self.wiky_uuid = wiky_uuid
        self.name = name
        self.token = token
        self.login_time = now


def wiky_auth(db: DBConn, username: str, pwd: str) -> (bool, WikySession | str):
    try:
        db.conn.begin()
        cursor = db.conn.cursor(dictionary=True)
        db.cursor.execute(
            """
            SELECT 
            t1.incr_id, t1.uni_incr_id, t1.username, t1.user_uuid, t1.status, 
            t0.mail, t0.pwd_hash, t0.status
            FROM 
            wiky_user_account t1 JOIN uni_account t0 ON t1.uni_incr_id = t0.incr_id
            WHERE t1.username = %s # maybe add mail
            """.strip(),
            (username,)
        )

        row = db.cursor.fetchone()
        uni_incr, wiky_incr, wiky_uuid, name = (
            row['t1.uni_incr_id'], row['t1.incr_id'], row['t1.user_uuid'], row['t1.username'],
        )

        auth_ok = row['t0.pwd_hash'] == r.sha265(pwd)
        auth_ok = auth_ok and (row['t0.status'] == row['t1.status'] == "live")

        if not auth_ok:
            logger.warning(f"wiky_auth fail: {repr(username)}")
            return (False, "auth_fail")

        time_now = time.time()
        token_now = username+r.sha265(username+str(time_now))
        validity = datetime.now() + timedelta(days=7)

        cursor.execute(
            '''
            INSERT INTO wiky_session 
            (uni_incr_id, wiky_incr_id, user_uuid, session_token, token_validity)
            VALUES (%s, %s, %s, %s, %s)
            '''.strip(),
            (uni_incr, wiky_incr, wiky_uuid, token_now, validity)
        )

        logger.info(f"wiky_auth ok: {repr(username)}:{wiky_incr}")
        return (True, WikySession(uni_incr, wiky_incr, wiky_uuid, username, token_now, time_now))

    except Exception as e:
        db.conn.rollback()
        logger.warning(f"wiky_auth fail: {repr(username)}")
        logger.error(f"wiky_auth error: {repr(username)}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_session rollback: {repr(username)}")
        return (False, "auth_error")


def create_wiky_profile(db: DBConn, ws: WikySession):
    try:
        db.conn.begin()
        db.cursor.execute(
            '''
            INSERT INTO wiky_user_profile 
            (uni_incr_id, wiky_incr_id)
            VALUES (%s, %s)
            '''.strip(),
            (ws.uni_incr, ws.wiky_incr)
        )
        db.conn.commit()
        logger.info(f"create_wiky_profile ok: {repr(ws.name)}:{ws.wiky_incr}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"create_wiky_profile error: {ws.name}:{repr(e)} - {repr(traceback.format_exc())}")
        return False


def user_request(db: DBConn, name_src, name_dst) -> (bool, str):
    try:
        db.conn.begin()

        db.cursor.execute("SELECT incr_id FROM wiky_user_account WHERE username = %s", (name_src,))
        src_id_row = db.cursor.fetchone()

        db.cursor.execute("SELECT incr_id FROM wiky_user_account WHERE username = %s", (name_dst,))
        dst_id_row = db.cursor.fetchone()

        if src_id_row is None or dst_id_row is None:
            logger.error(f"user_request: {name_src}:{src_id_row is None} - {name_dst}:{dst_id_row is None}")
            return (False, "invalid name")

        src_id, dst_id = src_id_row[0], dst_id_row[0]

        db.cursor.execute(
            '''
            INSERT INTO wiky_relation 
            (wiky_incr_id0, wiky_incr_id1, src_0, status)
            VALUES (%s, %s, %s, %s)
            '''.strip(),
            (min(src_id, dst_id), max(src_id, dst_id), src_id < dst_id, "pending")
        )
        db.conn.commit()
        logger.info(f"user_request ok: {name_src}->{name_dst}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"user_request error: {name_src}->{name_dst}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_relation rollback: {name_src}->{name_dst}")
        return False


if __name__ == '__main__':
    # wiky_conn = DBConn("root")
    pass