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
logger = r.get_logger('wiky_db_setup ')


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
    mail char(60) not null,
    phone char(20), 
    acc_status enum('pending', 'live', 'hold', 'yanked', 'archive', 'deleted') not null,
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

init_wiky_category = '''
create table wiky_category (
    page_id bigint primary KEY,
    category varchar(140) not null,
    index category_index (category)
) engine = InnoDB;
'''.strip()

init_wiky_index = '''
create table wiky_index (
    incr_id bigint auto_increment primary KEY,
    page_id bigint not null,
    zstd_st bigint not null,
    link_count int default 0,
    sect_count int default 0,
    revi_count int default 0,
    page_type enum(
        'redirect', 'template', 'wikipedia', 'portals',
        'categories', 'module', 'help', 'article'
    ) not null,
    page_title varchar(280) not null,
    foreign key (zstd_st) references zstd_range(st),
    index page_id_index (page_id),
    index page_type_index (page_type),
    index page_title_index (page_title)
) engine = InnoDB;
'''.strip()

init_wiky_page_link = '''
create table wiky_page_link (
    page_id bigint,
    parent_id bigint,
    foreign key (page_id) references wiky_index(page_id),
    foreign key (parent_id) references wiky_category(page_id),
    unique (page_id, parent_id),
    index page_id_index (page_id),
    index parent_id_index (parent_id)
) engine = InnoDB;
'''.strip()

# init_wiky_category_link = '''
# create table wiky_wiky_category_link (
#     page_id bigint,
#     parent_id int,
#     foreign key (page_id) references wiky_index(page_id),
#     foreign key (parent_id) references wiky_category(incr_id),
#     unique (page_id, parent_id),
#     index page_id_index (page_id),
#     index parent_id_index (parent_id)
# ) engine = InnoDB;
# '''.strip()

init_wiky_user_account = '''
create table wiky_user_account (
    incr_id bigint auto_increment primary key,
    uni_incr_id bigint unique,
    service_id bigint not null,
    username char(20) unique,
    user_uuid binary(16) unique,
    display_name char(30) not null,
    acc_status enum('pending', 'live', 'hold', 'yanked', 'archive', 'deleted') not null,
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
    wiky_incr_id bigint unique,
    ranks INT DEFAULT 0,
    balance INT DEFAULT 0,
    pull_count INT DEFAULT 0,
    foreign key (wiky_incr_id) references wiky_user_account(incr_id),
    index wiky_incr_id_index (wiky_incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_user_description = '''
create table wiky_user_intro (
    incr_id bigint auto_increment primary key,
    wiky_incr_id bigint unique,
    description varchar(60),
    foreign key (wiky_incr_id) references wiky_user_account(incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_session = '''
create table wiky_session (
    session_id bigint auto_increment primary key,
    wiky_incr_id bigint unique,
    user_uuid binary(16) unique,
    session_token char(84) not null,
    token_validity datetime(3) not null,
    login_time datetime(3) default current_timestamp(3),
    index user_uuid_index (user_uuid),
    index wiky_incr_id_index (wiky_incr_id)
) engine=memory;
'''.strip()

init_wiky_item_list = '''
create table wiky_item_list (
    incr_id int auto_increment primary key,
    item_name char(30) unique,
    item_stat enum('live', 'yanked'),
    index item_name_index (item_name)
) engine = InnoDB;
'''.strip()

init_wiky_item_shop = '''
create table wiky_item_shop (
    incr_id bigint auto_increment primary key,
    item_id int unique,
    item_cost int unsigned not null,
    foreign key (item_id) references wiky_item_list(incr_id)
) engine = InnoDB;
'''.strip()

init_wiky_user_storage = '''
create table wiky_user_storage (
    incr_id bigint auto_increment primary key,
    wiky_incr_id bigint not null,
    item_id int not null,
    item_count int not null,
    foreign key (wiky_incr_id) references wiky_user_account(incr_id),
    foreign key (item_id) references wiky_item_list(incr_id),
    unique (wiky_incr_id, item_id)
) engine = InnoDB;
'''.strip()

init_wiky_user_collection = '''
create table wiky_user_collection (
    incr_id bigint auto_increment primary key,
    wiky_incr_id bigint not null,
    page_incr_id bigint not null,
    foreign key (wiky_incr_id) references wiky_user_account(incr_id),
    foreign key (page_incr_id) references wiky_index(incr_id),
    unique (wiky_incr_id, page_incr_id)
) engine = InnoDB;
'''.strip()

# init_wiky_task_list = '''
# create table wiky_task_list (
#     wiky_incr_id bigint unique,
#     task_type enum('main', 'yanked'),
#     progress float,
#     setting json,
#     foreign key (wiky_incr_id) references wiky_user_account(incr_id)
# ) engine = InnoDB;
# '''.strip()
#
# init_wiky_main_task = '''
# create table wiky_main_task (
#     uni_incr_id bigint unique,
#     wiky_incr_id bigint unique,
#     status enum('filled', 'unfilled', 'blocked', 'ignored', 'reported', 'yanked'),
#     progress float,
#     setting json,
#     foreign key (uni_incr_id) references uni_account(incr_id),
#     foreign key (wiky_incr_id) references wiky_user_account(incr_id)
# ) engine = InnoDB;
# '''.strip()

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
    wiky_incr_id bigint unique,
    setting json,
    foreign key (wiky_incr_id) references wiky_user_account(incr_id)
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

        db.cursor.execute(init_zstd_range)
        db.cursor.execute(init_wiky_index)
        db.cursor.execute(init_wiky_category)
        db.cursor.execute(init_wiky_page_link)
        # db.cursor.execute(init_wiky_category_link)

        db.cursor.execute(init_wiky_user_account)
        db.cursor.execute(init_wiky_user_profile)
        db.cursor.execute(init_wiky_user_description)
        db.cursor.execute(init_wiky_session)
        db.cursor.execute(init_wiky_relation)
        db.cursor.execute(init_wiky_setting)
        db.cursor.execute(init_wiky_item_list)
        db.cursor.execute(init_wiky_item_shop)
        db.cursor.execute(init_wiky_user_storage)
        db.cursor.execute(init_wiky_user_collection)

        db.cursor.execute("alter table wiky_index convert to character set utf8mb4 collate utf8mb4_unicode_ci;")
        db.cursor.execute("alter table wiky_category convert to character set utf8mb4 collate utf8mb4_unicode_ci;")
        db.cursor.execute("alter table wiky_user_intro convert to character set utf8mb4 collate utf8mb4_unicode_ci;")
        db.cursor.execute("alter table wiky_item_list convert to character set utf8mb4 collate utf8mb4_unicode_ci;")
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


def wiky_parser_setup():
    import wiky

    src_bz2_simple = "path/to/dump"
    src_index_simple = "path/to/dump"
    src_bz2 = "path/to/dump"
    src_index = "path/to/dump"

    dst_zstd_simple = "path/to/export.zstd"
    dst_index_simple = "path/to/export.txt"
    dst_zstd = "path/to/export.zstd"
    dst_index = "path/to/export.txt"

    import wiky
    print(wiky.__all__)

    wiky.setup_dump(
        src_bz2_simple,
        src_index_simple,
        dst_zstd_simple,
        dst_index_simple,
    )

    time.sleep(2)

    a = wiky.WikySource(dst_index_simple, dst_zstd_simple)
    a.validate_index_dump()


if __name__ == '__main__':
    setup()

