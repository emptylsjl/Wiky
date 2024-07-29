
## 07/28
### tracking

- add optional full/incr backup
- build/package crate `wiky_paser` and publish to PyPI
- add event system for `wiky_service`
- impl types and attributes for wiky page
- impl ranking for wiky page
- impl display for wiky page
- add pull pooling system for wiky page
- design user management system for database
- add battle system for `wiky_user`
- add compression to `wiky_index` (currently 4.6GB with 23M row) <br>
  expecting 70% reduction in storage capacity
- add utf8 and description support for `wiky_user`
- refactor page `storage` mechanic (currently mariadb json)
- impl actually http based client (? maybe)
- cross platform support (? maybe)


## 07/19
### Changed

- add changelog.md
- update readme.md
- fix str validation in `register_wiky`
- update status in `register_wiky`
- when inserting `wiky_user`, `wiky_profile` is inserted
- fix cursor dict usage in `wiky_auth` 
- add detailed authentication message for `wiky_auth`
- fix `add_balance` sql syntax
- fix `pull_wiky` when joining `storage`
- fix multiple unique key in table
- add wiky index setup in python (require maturin)
- impl general test for user system
- add logging during db establishment 
- update `setup.rs` so that remapped zstd is not appended<br> onto old zstd
- export json extract to python


## 07/18
### Changed

- fix pycharm auto capital
- impl balance
- impl page pulling from `wiky_index`
- update multiple varchar to char 
- fix utf8 conversion for page title
- partial implement of page title compression with dict
- impl error mapping in rs
- impl decode chunk for use in python


## 07/17
### Changed

- impl logging system
- impl text/mail/phone validation
- add indexing for multiple table
- impl `register_wiky`
- impl `register_uni`
- add `WikySession`
- impl `wiky_auth`
- impl `create_wiky_profile`
- impl `user_request`
- fix base64 encoding penitential error
- update uuid column to binary
- update multiple varchar to char 
- update int to bigint (`wiky_index` can not be hold with int)
- large refactor on multiple table layout
- add info/type/category to wiky page
- fix table setup error
- impl insertion/setup for `wiky_index` from enwiki_dump_remap
- impl insertion/setup for `zstd_range` from enwiki_dump_remap
- impl chunking for `WikySource`
- experimental ffi with pyo3
- add function to set thread count with lazy global
- export wiky remapping through pyo3
- export wiky benchmarking through pyo3


## 07/14
### Changed

- update system component graph
- add glue pyo3 for python with cargo
- impl zstd remapping for wiky page chunk (from bz2)
- impl enwiki_dump validation
- impl enwiki_dump/remapped benchmark
- impl chunk extraction


## 07/03
### Changed
- update gitignore
- migrate `wiky` from sqlite to mariadb
- impl connection for mariadb
- impl `wiky_parser`
- add `WikySource`
- rewrote table layout
- impl setup for mariadb tables
- complete system interaction layout graph
- complete enwiki full iteration with wiky_parser


## 06/17
### Changed

- shifted project to standalone repo (wiky)


## 06/14
### Changed
- complete initial system design
- impl initial table layout in sqlite
- impl basic select/insert in sqlite
- impl basic per page extraction from enwiki
- start parsing full enwiki dump


## 06/01
### Changed
- decide on project title, overall goal
- create initial system design
- init python sqlite project in base_repo
- impl initial table layout in sqlite
- attempting to download enwiki dump