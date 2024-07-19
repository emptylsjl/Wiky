# Wiky

Wiky is an wikipedia based card game

## Setup requirement

### Python:
- Python 3.11 and up
- mariadb
- maturin
- 
### wikipedia:
- enwiki-multistream.xml.bz2
- enwiki-index.text
<br>(dump 2024-06-01 is used for testing)

### Cargo:
- rustc with cargo installed
- maturin
<br> (not that rust build system will not be need after package is published/built, not yet)

  
## Setup:
### init database:
```bash
$ python py/setup.py
```
### remap wiky dump:
```bash
# require cargo
$ cargo run --release
```
### for wiky parser to run in python:
```bash
$ pip install maturin
# or pipx install maturin
$ maturin develop
# or maturin build --release

# change path in setup_wiky_index()
# run setup_wiky_index from setup.py
```
