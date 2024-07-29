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
    # dummy
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
    valid = valid and validate_mail_str(mail, 50)
    if phone:
        valid = valid and validate_phone_str(phone, 16)
    if not valid:
        logger.error(f'register_wiky invalid:{repr(username)}')
        return False
    pwd_hash = r.sha265(pwd)

    try:
        db.conn.begin()

        db.cursor.execute("select incr_id from wiky_user_account where username = %s", (username,))
        if (wiky_incr_id_row := db.cursor.fetchone()) is not None:
            logger.error(f"register_wiky error: {repr(username)} - wiky_acc_exist")
            return (False, "wiky_acc_exist")

        db.cursor.execute("select incr_id from uni_account where username = %s", (username,))
        if (uni_incr_id_row := db.cursor.fetchone()) is None:
            db.cursor.execute(
                '''
                insert into uni_account 
                (uni_uuid, username, display_name, pwd_hash, mail, phone, acc_status)
                values (%s, %s, %s, %s, %s, %s, %s)
                '''.strip(),
                (uuid.uuid4().bytes, username, display_name, pwd_hash, mail, phone, "live")
            )
            incr_id = db.cursor.lastrowid
        else:
            uni_acc_exist = True
            incr_id = uni_incr_id_row[0]

        db.cursor.execute(
            '''
            insert into wiky_user_account 
            (uni_incr_id, service_id, user_uuid, username, display_name, acc_status)
            values (%s, %s, %s, %s, %s, %s)
            '''.strip(),
            (incr_id, 1, uuid.uuid4().bytes, username, display_name, "live")
        )
        wiky_incr_id = db.cursor.lastrowid

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
            insert into uni_account 
            (uni_uuid, username, display_name, pwd_hash, mail, phone, acc_status)
            values (%s, %s, %s, %s, %s, %s, %s)
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
        dict_cursor = db.conn.cursor(dictionary=True)
        dict_cursor.execute(
            """
            select 
            t1.incr_id, t1.uni_incr_id, t1.username, t1.user_uuid, t1.acc_status, 
            t0.mail, t0.pwd_hash, t0.acc_status
            from 
            wiky_user_account t1 join uni_account t0 on t1.uni_incr_id = t0.incr_id
            where t1.username = %s # maybe add mail
            """.strip(),
            (username,)
        )

        row = dict_cursor.fetchone()
        uni_incr, wiky_incr, wiky_uuid, name = (
            row['uni_incr_id'], row['incr_id'], row['user_uuid'], row['username'],
        )

        if not row['pwd_hash'] == r.sha265(pwd):
            logger.warning(f"wiky_auth fail: {repr(username)} - password_missmatch")
            return (False, "password_missmatch")

        if not row['acc_status'] == row['acc_status'] == "live":
            logger.warning(f"wiky_auth fail: {repr(username)} - account_invalid")
            return (False, "account_invalid")

        time_now = time.time()
        token_now = username + r.sha265(username + str(time_now))
        validity = datetime.now() + timedelta(days=7)

        dict_cursor.execute(
            '''
            insert ignore into wiky_session 
            (wiky_incr_id, user_uuid, session_token, token_validity)
            values (%s, %s, %s, %s)
            '''.strip(),
            (wiky_incr, wiky_uuid, token_now, validity)
        )

        logger.info(f"wiky_auth ok: {repr(username)}:{wiky_incr}")
        return (True, WikySession(uni_incr, wiky_incr, wiky_uuid, username, token_now, time_now))

    except Exception as e:
        db.conn.rollback()
        logger.warning(f"wiky_auth fail: {repr(username)}")
        logger.error(f"wiky_auth error: {repr(username)}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_session rollback: {repr(username)}")
        return (False, "auth_error")


def logout(db: DBConn, ws: WikySession):
    try:
        db.conn.begin()
        db.cursor.execute(
            '''
            delete from wiky_session 
            where wiky_incr_id = %s
            '''.strip(),
            (ws.wiky_incr,)
        )
        logger.info(f"logout ok: {repr(ws.wiky_incr)}:{ws.name}:{ws.login_time}")
    except Exception as e:
        db.conn.rollback()
        logger.error(f"logout error: {repr(ws.wiky_incr)}:{ws.name}:{repr(e)} - {repr(traceback.format_exc())}")
        return False


def create_wiky_profile(db: DBConn, ws: WikySession):
    try:
        db.conn.begin()
        db.cursor.execute(
            '''
            insert into wiky_user_profile 
            (wiky_incr_id)
            values (%s)
            '''.strip(),
            (ws.wiky_incr,)
        )

        db.cursor.execute(
            '''
            insert into wiky_user_intro 
            (wiky_incr_id)
            values (%s)
            '''.strip(),
            (ws.wiky_incr,)
        )

        db.conn.commit()
        # logger.info(f"create_wiky_profile/intro ok: {repr(ws.name)}:{ws.wiky_incr}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"create_wiky_profile/intro error: {ws.name}:{repr(e)} - {repr(traceback.format_exc())}")
        return False


def user_request(db: DBConn, name_src, name_dst) -> (bool, str):
    try:
        db.conn.begin()

        db.cursor.execute("select incr_id from wiky_user_account where username = %s", (name_src,))
        src_id_row = db.cursor.fetchone()

        db.cursor.execute("select incr_id from wiky_user_account where username = %s", (name_dst,))
        dst_id_row = db.cursor.fetchone()

        if src_id_row is None or dst_id_row is None:
            logger.error(f"user_request: {name_src}:{src_id_row is None} - {name_dst}:{dst_id_row is None}")
            return (False, "invalid name")

        src_id, dst_id = src_id_row[0], dst_id_row[0]

        db.cursor.execute(
            '''
            insert into wiky_relation 
            (wiky_incr_id0, wiky_incr_id1, src_0, status)
            values (%s, %s, %s, %s)
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


def add_balance(db: DBConn, ws: WikySession, amount) -> (bool, str):
    try:
        db.conn.begin()
        db.cursor.execute(
            "select wiky_incr_id, balance from wiky_user_profile where wiky_incr_id = %s",
            (ws.wiky_incr,)
        )
        profile_row = db.cursor.fetchone()
        if profile_row is None:
            logger.error(f"add_balance fail: user not found - {ws.wiky_uuid}:{ws.name}")
            return (False, "invalid name")

        db.cursor.execute(
            "update wiky_user_profile set balance = %s where wiky_incr_id = %s",
            (profile_row[1] + amount, ws.wiky_incr)
        )
        db.conn.commit()
        logger.info(f"update wiky_user_profile: {repr(ws.wiky_uuid)}:{ws.name}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"add_balance error: {ws.wiky_uuid}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_relation rollback: {repr(ws.wiky_uuid)}:{ws.name}")
        return False


def pull_wiky(db: DBConn, ws: WikySession, amount) -> (bool, str):
    try:
        db.conn.begin()
        db.cursor.execute(
            '''
            select t0.wiky_incr_id, t0.balance
            from wiky_user_profile t0
            where t0.wiky_incr_id = %s
            ''',
            (ws.wiky_incr,)
        )
        profile_row = db.cursor.fetchone()
        if profile_row is None:
            logger.error(f"pull_wiky fail: profile not found - {ws.wiky_uuid}:{ws.name}")
            return (False, "invalid name")

        if amount > profile_row[1]:
            logger.warning(f"pull_wiky fail: insufficient balance - {profile_row[1]}")
            return (False, "insufficient balance")
        db.cursor.execute(
            "update wiky_user_profile set balance = %s where wiky_incr_id = %s",
            (profile_row[1] - amount, ws.wiky_incr)
        )

        db.cursor.execute(
            '''
            select t0.incr_id, t0.page_id, t0.zstd_st, t1.ed
            from (
                select incr_id, zstd_st, page_id
                from wiky_index
                order by RAND()
                limit %s
            ) t0
            join zstd_range t1 on t0.zstd_st = t1.st
            order by t0.page_id
            ''',
            (amount,)
        )
        page_rows = db.cursor.fetchall()

        for p in page_rows:
            db.cursor.execute(
                """
                insert ignore into wiky_user_collection (wiky_incr_id, page_incr_id)
                values (%s, %s)
                """,
                (ws.wiky_incr, p[1])
            )

        db.conn.commit()
        logger.info(f"pull_wiky ok: {repr(ws.wiky_uuid)}:{ws.name}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"add_balance error: {ws.wiky_uuid}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_relation rollback: {repr(ws.wiky_uuid)}:{ws.name}")
        return False


def add_item_list(db: DBConn, name: str, stat='live') -> (bool, int):
    try:
        db.conn.begin()

        db.cursor.execute(
            '''
            insert into wiky_item_list 
            (item_name, item_stat)
            values (%s, %s)
            '''.strip(),
            (name, stat)
        )
        db.conn.commit()
        logger.info(f"add_item_list ok: {name}:{stat}")

        return (True, db.cursor.lastrowid)
    except Exception as e:
        db.conn.rollback()
        logger.error(f"add_item_list error: {name}:{stat}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_item_list rollback: {name}:{stat}")
        return (False, -1)


def update_item_list(db: DBConn, item_id: int, name: str, stat: str):
    try:
        db.conn.begin()

        db.cursor.execute(
            '''
            update wiky_item_list 
            set item_name = %s, item_stat = %s
            where incr_id = %s
            ''',
            (name, stat, item_id)
        )
        db.conn.commit()
        logger.info(f"add_item_list ok: {item_id}, new_name:{name}:{stat}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"add_item_list error: {item_id}, new_name:{name}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"wiky_item_list rollback: {item_id}")
        return False


def add_shop_item(db: DBConn, item: int | str, cost: int):
    try:
        db.conn.begin()

        if type(item) is str:
            db.cursor.execute(
                "select incr_id, item_name, item_stat from wiky_item_list where item_name = %s",
                (item,)
            )
            item_row = db.cursor.fetchone()

            if item_row is None:
                logger.error(f"add_shop_item fail: item not exist - {item}")
                return (False, "invalid name")
            elif item_row[2] == 'yanked':
                logger.error(f"add_shop_item fail: bad status - {item_row}")
                return (False, "bad status")

            item_id = item_row[0]
        else:
            item_id = item

        db.cursor.execute(
            '''
            insert into wiky_item_shop 
            (item_id, item_cost)
            values (%s, %s)
            '''.strip(),
            (item_id, cost)
        )
        db.conn.commit()
        logger.info(f"add_shop_item ok: {item}:{item_id}:{cost}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"add_shop_item error: {item} - {repr(traceback.format_exc())}")
        logger.error(f"add_shop_item rollback: {item}")
        return False


def remove_shop_item(db: DBConn, item: int | str):
    try:
        db.conn.begin()

        if type(item) is str:
            db.cursor.execute(
                "select incr_id, item_name, item_stat from wiky_item_list where item_name = %s",
                (item,)
            )
            item_row = db.cursor.fetchone()

            if item_row is None:
                logger.error(f"remove_shop_item fail: item not found - {item}")
                return (False, "invalid name")

            item_id = item_row[0]
        else:
            item_id = item

        db.cursor.execute(
            '''
            delete from wiky_item_shop
            where item_id = %s
            '''.strip(),
            (item_id,)
        )
        db.conn.commit()
        logger.info(f"remove_shop_item ok: {item}:{item_id}")

    except Exception as e:
        db.conn.rollback()
        logger.error(f"remove_shop_item error: {item} - {repr(traceback.format_exc())}")
        return False


def item_purchase(db: DBConn, ws: WikySession, item: int | str, count: int) -> (bool, str):
    try:
        db.conn.begin()

        db.cursor.execute(
            "select balance from wiky_user_profile where wiky_incr_id = %s",
            (ws.wiky_incr,)
        )

        profile_row = db.cursor.fetchone()
        if profile_row is None:
            logger.error(f"item_purchase fail: user not found - {ws.wiky_uuid}:{ws.name}")
            return (False, "invalid name")

        if type(item) is str:
            db.cursor.execute(
                """
                select 
                t0.incr_id, t0.item_name, t0.item_stat, t1.item_cost
                from 
                wiky_item_list t0 join wiky_item_shop t1 on t0.incr_id = t1.item_id
                where t0.item_name = %s
                """.strip(),
                (item,)
            )
            item_row = db.cursor.fetchone()

            if item_row is None:
                logger.error(f"item_purchase fail: item not exist - {item}")
                return (False, "invalid name")
            elif item_row[2] == 'yanked':
                logger.error(f"item_purchase fail: item bad status - {item_row}")
                return (False, "bad status")

            item_id = item_row[0]
        else:
            item_id = item
            db.cursor.execute(
                """
                select 
                t0.incr_id, t0.item_name, t0.item_stat, t1.item_cost
                from 
                wiky_item_list t0 join wiky_item_shop t1 on t0.incr_id = t1.item_id
                where t0.incr_id = %s
                """.strip(),
                (item_id,)
            )
            item_row = db.cursor.fetchone()

            if item_row is None:
                logger.error(f"item_purchase fail: id not exist - {item}")
                return (False, "invalid id")
            elif item_row[2] == 'yanked':
                logger.error(f"item_purchase fail: item bad status - {item_row}")
                return (False, "bad status")

        if item_row[3] * count > profile_row[0]:
            logger.warning(f"item_purchase fail: insufficient balance - {item}")
            return (False, "insufficient balance")

        db.cursor.execute(
            "update wiky_user_profile set balance = %s where wiky_incr_id = %s",
            (profile_row[0] - count * item_row[3], ws.wiky_incr)
        )

        db.cursor.execute(
            """
            insert into wiky_user_storage (wiky_incr_id, item_id, item_count)
            values (%s, %s, %s)
            on duplicate key update item_count = item_count + %s;
            """,
            (ws.wiky_incr, item_id, count, count)
        )

        db.conn.commit()
        logger.info(f"item_purchase ok: {ws.name}")
        logger.info(f"update wiky_user_profile: {ws.name}")
        return (True, "ok")
    except Exception as e:
        db.conn.rollback()
        logger.error(f"item_purchase error: {ws.wiky_uuid}:{repr(e)} - {repr(traceback.format_exc())}")
        logger.error(f"item_purchase rollback: {repr(ws.wiky_uuid)}:{ws.name}")
        return False


def get_user_info(db: DBConn, ws: WikySession) -> (bool, dict | str, list):
    try:
        db.conn.begin()
        dict_cursor = db.conn.cursor(dictionary=True)
        dict_cursor.execute(
            """
            select 
            t1.incr_id, t1.uni_incr_id, t1.username, t1.display_name, t1.user_uuid, t1.acc_status, t1.time_created,
            t0.mail, t0.pwd_hash, t0.acc_status,
            t2.balance, t2.pull_count, t2.ranks,
            t3.description
            from wiky_user_account t1 
            join uni_account t0 on t1.uni_incr_id = t0.incr_id
            join wiky_user_profile t2 on t0.incr_id = t2.wiky_incr_id
            join wiky_user_intro t3 on t0.incr_id = t3.wiky_incr_id
            
            where t1.incr_id = %s
            """.strip(),
            (ws.wiky_incr,)
        )

        profile_row = dict_cursor.fetchone()
        if profile_row is None:
            logger.error(f"get_user_info fail: user not found - {ws.wiky_uuid}:{ws.name}")
            return (False, "invalid id")

        db.cursor.execute(
            '''
            select * from wiky_user_collection
            where wiky_incr_id = %s;
            ''',
            (ws.wiky_incr,)
        )
        pulls = db.cursor.fetchall()

        return (True, profile_row, pulls)

    except Exception as e:
        db.conn.rollback()
        logger.error(f"get_user_info error: {ws.wiky_uuid}:{repr(ws.name)}:{repr(e)} - {repr(traceback.format_exc())}")
        return (False, f"get_user_info: {repr(e)}", None)


if __name__ == '__main__':
    # wiky_conn = DBConn("root")
    pass
