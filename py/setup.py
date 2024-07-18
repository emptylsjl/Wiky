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
from py.wiky_conn import DBConn

r.set_logger('wiky_db.log')
logger = r.get_logger('wiky_db_setup')


init_db = lambda db_name: f"create database if not exists {db_name};"

drop_db = lambda db_name: f"drop database if exists {db_name};"

use_db = lambda db_name: f"use {db_name};"

init_uni_account = '''
create table uni_account (
    incr_id bigint auto_increment primary key,
    uni_uuid binary(16) unique,
    username char(20) unique,
    display_name char(30) not null,
    pwd_hash char(128) not null,
    mail char(60) unique,
    phone char(20),
    status enum('pending', 'live', 'hold', 'yanked', 'archive', 'deleted') not null,
    time_created timestamp default current_timestamp,
    time_updated timestamp default current_timestamp,
    index uni_uuid_index (uni_uuid),
    index username_index (username)
) engine = InnoDB;
'''.strip()

init_service_maintained = '''
create table service_maintained (
    service_id bigint auto_increment primary key,
    service_name char(50) not null,
    display_name char(50) not null
    -- maybe some other stuff like maintainer list
) engine = InnoDB;
'''.strip()

init_wiky_user_account = '''
create table wiky_user_account (
    incr_id bigint auto_increment primary key,
    uni_incr_id bigint unique,
    service_id bigint unique,
    username char(20) unique,
    user_uuid binary(16) unique,
    display_name char(30) not null,
    status enum('pending', 'live', 'hold', 'yanked', 'archive', 'deleted') not null,
    time_created timestamp default current_timestamp,
    time_updated timestamp default current_timestamp,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (service_id) references service_maintained(service_id),
    index username_index (username),
    index user_uuid_index (user_uuid)
) engine = InnoDB;
'''.strip()

init_wiky_user_profile = '''
create table wiky_user_profile (
    incr_id bigint auto_increment primary key,
    uni_incr_id bigint unique,
    wiky_incr_id bigint unique,
    level INT DEFAULT 0,
    balance INT DEFAULT 0,
    storage INT DEFAULT 0,
    page_count INT DEFAULT 0,
    pull_count INT DEFAULT 0,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_id) references wiky_user_account(incr_id),
    index uni_incr_id_index (uni_incr_id),
    index wiky_incr_id_index (wiky_incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_session = '''
create table wiky_session (
    session_id bigint auto_increment primary key,
    uni_incr_id bigint unique,
    wiky_incr_id bigint unique,
    user_uuid binary(16) unique,
    session_token char(84) not null,
    token_validity datetime(3) not null,
    login_time datetime(3) default current_timestamp(3),
    index user_uuid_index (user_uuid),
    index uni_incr_id_index (uni_incr_id),
    index wiky_incr_id_index (wiky_incr_id)
) engine=memory;
'''.strip()

init_wiky_user_storage = '''
create table wiky_user_storage (
    incr_id bigint auto_increment primary key,
    uni_incr_id bigint unique,
    wiky_incr_id bigint unique,
    wiky_storage json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_id) references wiky_user_account(incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_task_list = '''
create table wiky_task_list (
    uni_incr_id bigint unique,
    wiky_incr_id bigint unique,
    type enum('main', 'yanked'),
    progress float,
    setting json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_id) references wiky_user_account(incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_main_task = '''
create table wiky_main_task (
    uni_incr_id bigint unique,
    wiky_incr_id bigint unique,
    status enum('filled', 'unfilled', 'blocked', 'ignored', 'reported', 'yanked'),
    progress float,
    setting json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_id) references wiky_user_account(incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_relation = '''
create table wiky_relation (
    incr_id bigint auto_increment primary KEY,
    wiky_incr_id0 bigint,
    wiky_incr_id1 bigint,
    src_0 boolean not null,
    status enum('pending', 'accepted', 'blocked', 'ignored', 'yanked'),
    foreign key (wiky_incr_id0) references wiky_user_account(incr_id),
    foreign key (wiky_incr_id1) references wiky_user_account(incr_id),
    unique (wiky_incr_id0, wiky_incr_id1)
) engine = InnoDB;
'''.strip()

init_wiky_setting = '''
create table wiky_setting (
    uni_incr_id bigint unique,
    wiky_incr_id bigint unique,
    setting json,
    foreign key (uni_incr_id) references uni_account(incr_id),
    foreign key (wiky_incr_id) references wiky_user_account(incr_id)
) engine = InnoDB;
'''.strip()

init_zstd_range = '''
create table zstd_range (
    incr_id bigint auto_increment primary KEY,
    st bigint unique,
    ed bigint unique,
    unique (st, ed),
    index st_index (st),
    index ed_index (ed)
) engine = InnoDB;
'''.strip()

init_wiky_index = '''
create table wiky_index (
    incr_id bigint auto_increment primary KEY,
    zstd_st bigint not null,
    page_id bigint unique,
    redirect bigint default -1,
    page_title varchar(280) not null,
    page_type enum(
        'article', 'vital', 'featured', 'good', 'spoken', 'overviews', 
        'outlines', 'lists', 'portals', 'glossaries', 'categories', 'indices'
    ),
    main_category binary(32),
    link_count int default 0,
    sect_count int default 0,
    foreign key (zstd_st) references zstd_range(st),
    index zstd_st_index (zstd_st),
    index page_id_index (page_id)
) engine = InnoDB;
'''.strip()


def setup():
    db = DBConn("root")
    try:
        db.cursor.execute(drop_db("wiky_base"))
        db.cursor.execute(init_db("wiky_base"))
        db.cursor.execute(use_db("wiky_base"))
        db.cursor.execute(init_uni_account)
        db.cursor.execute(init_service_maintained)
        db.cursor.execute(init_wiky_user_account)
        db.cursor.execute(init_wiky_user_profile)
        db.cursor.execute(init_wiky_user_storage)
        db.cursor.execute(init_wiky_session)
        db.cursor.execute(init_wiky_task_list)
        db.cursor.execute(init_wiky_main_task)
        db.cursor.execute(init_wiky_relation)
        db.cursor.execute(init_wiky_setting)
        db.cursor.execute(init_zstd_range)
        db.cursor.execute(init_wiky_index)

        db.cursor.execute("alter table wiky_index convert to character set utf8mb4 collate utf8mb4_unicode_ci;")
    except Exception as e:
        logger.error(f"wiky_db setup error: {repr(e)} - {repr(traceback.format_exc())}")
        print(e, traceback.format_exc())
        db.close()
        exit(1)

    db.cursor.execute(
        '''
        INSERT INTO service_maintained (service_name, display_name) VALUES (%s, %s);
        '''.strip(),
        ("wiky_dev", "wiky")
    )
    db.conn.commit()
    db.close()
    logger.info(f"wiky_db table creation complete")


if __name__ == '__main__':
    setup()

