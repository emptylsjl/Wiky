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

# import requests
# from bs4 import BeautifulSoup
# from base64 import b64encode, b64decode
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.remote.webelement import WebElement

import r

from py.wiky_new import DBConn

init_db = lambda db_name: f"create database if not exists {db_name};"

drop_db = lambda db_name: f"drop database if exists {db_name};"

use_db = lambda db_name: f"use {db_name};"

init_uni_account = '''
create table uni_account (
    incr_id integer auto_increment primary key,
    acc_uuid varchar(40) unique,
    username varchar(20) unique,
    display_name varchar(30) not null,
    pwd_hash varchar(128) not null,
    mail varchar(40) unique,
    phone varchar(20),
    time_created timestamp default current_timestamp,
    time_updated timestamp default current_timestamp
) engine = InnoDB;
'''.strip()

init_service_maintained = '''
create table service_maintained (
    service_id integer auto_increment primary key,
    service_name varchar(50) not null,
    display_name varchar(50) not null
    -- maybe some other stuff like maintainer list
) engine = InnoDB;
'''.strip()

init_wiky_user_account = '''
create table wiky_user_account (
    incr_uid integer auto_increment primary key,
    uni_incr_id integer unique,
    service_id integer unique,
    user_uuid varchar(40) unique,
    username varchar(20) not null,
    display_name varchar(30) not null,
    pwd_hash varchar(128) not null,
    pull_count INT DEFAULT 0,
    storage INT DEFAULT 0,
    time_created timestamp default current_timestamp,
    time_updated timestamp default current_timestamp,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (service_id) references service_maintained(service_id)
) engine = InnoDB;
'''.strip()

init_wiky_session = '''
create table wiky_session (
    session_id integer auto_increment primary key,
    uni_incr_id integer unique,
    wiky_incr_uid integer unique,
    user_uuid varchar(40) unique,
    session_token varchar(40) not null,
    token_validity DATETIME(3) not null,
    login_time timestamp default current_timestamp
) engine=memory;
'''.strip()

init_wiky_user_storage = '''
create table wiky_user_storage (
    incr_id integer auto_increment primary key,
    uni_incr_id integer unique,
    wiky_incr_uid integer unique,
    wiky_storage json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_uid) references wiky_user_account(incr_uid)
) engine = InnoDB;
'''.strip()

init_wiky_task_list = '''
create table wiky_task_list (
    uni_incr_id integer unique,
    wiky_incr_uid integer unique,
    type ENUM('main', 'yanked'),
    progress float,
    setting json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_uid) references wiky_user_account(incr_uid)
) engine = InnoDB;
'''.strip()

init_wiky_main_task = '''
create table wiky_main_task (
    uni_incr_id integer unique,
    wiky_incr_uid integer unique,
    status ENUM('filled', 'unfilled', 'blocked', 'ignored', 'reported', 'yanked'),
    progress float,
    setting json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_uid) references wiky_user_account(incr_uid)
) engine = InnoDB;
'''.strip()

init_wiky_friend = '''
create table wiky_friend (
    incr_id integer auto_increment primary KEY,
    wiky_incr_uid0 integer,
    wiky_incr_uid1 integer,
    status ENUM('pending', 'accepted', 'blocked', 'ignored', 'reported', 'yanked'),
    foreign key (wiky_incr_uid0) references wiky_user_account(incr_uid),
    foreign key (wiky_incr_uid1) references wiky_user_account(incr_uid),
    unique (wiky_incr_uid0, wiky_incr_uid1)
) engine = InnoDB;
'''.strip()

init_wiky_setting = '''
create table wiky_setting (
    uni_incr_id integer unique,
    wiky_incr_uid integer unique,
    setting json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_uid) references wiky_user_account(incr_uid)
) engine = InnoDB;
'''.strip()

init_zstd_range = '''
create table zstd_range (
    incr_id integer auto_increment primary KEY,
    st integer unique,
    ed integer unique,
    unique (st, ed)
) engine = InnoDB;
'''.strip()

init_wiky_index = '''
create table wiky_index (
    incr_id integer auto_increment primary KEY,
    zstd_st integer not null,
    page_id integer unique,
    page_title varchar(280) not null,
    category varchar(100),
    link_count integer default 0,
    sect_count integer default 0,
    foreign key (zstd_st) references zstd_range(st)
) engine = InnoDB;
'''.strip()

init_wiky_transaction = '''
create table wiky_transaction (
    -- todo
) engine = InnoDB;
'''.strip()


def exec(db: DBConn, task: str):
    try:
        db.cursor.execute(task)
        db.conn.commit()
    except mariadb.Error as e:
        print(e, traceback.format_exc())
        exit(1)


def setup():
    db = DBConn("root")
    db.cursor.execute(drop_db("wiky_base"))
    db.cursor.execute(init_db("wiky_base"))
    db.cursor.execute(use_db("wiky_base"))
    db.cursor.execute(init_uni_account)
    db.cursor.execute(init_service_maintained)
    db.cursor.execute(init_wiky_user_account)
    db.cursor.execute(init_wiky_session)
    db.cursor.execute(init_wiky_user_storage)
    db.cursor.execute(init_wiky_task_list)
    db.cursor.execute(init_wiky_main_task)
    db.cursor.execute(init_wiky_friend)
    db.cursor.execute(init_wiky_setting)
    db.cursor.execute(init_zstd_range)
    db.cursor.execute(init_wiky_index)

    db.cursor.execute('''
    INSERT INTO service_maintained (service_id, service_name, display_name) VALUES (%s, %s, %s);
    '''.strip(), )

    db.close()
    pass


if __name__ == '__main__':
    setup()