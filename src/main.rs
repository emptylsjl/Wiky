#![feature(hash_raw_entry)]

mod wiky_source;
mod constant;
mod setup;
mod misc;

use std::collections::{HashMap, HashSet};
use std::{fs, io, time, vec};
use std::fmt::{Display, format};
use std::fs::File;
use std::io::{BufReader, SeekFrom};
use std::io::prelude::*;
use std::iter::once;
use std::ops::{Add, Shr, Sub};
use std::path::{Path, PathBuf};
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};
use std::time::Instant;

use bzip2::{Compression, Decompress};
use bzip2::read::{BzEncoder, BzDecoder};
use anyhow::{Context, Result};
use itertools::Itertools;
use mysql::{Conn, OptsBuilder};
use nohash_hasher::BuildNoHashHasher;
use quickxml_to_serde::Config;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use zstd_safe::{CCtx, DCtx, create_cdict, CDict, DDict};

use utils::*;

use misc::*;
use setup::*;
use constant::*;
use wiky_source::*;



fn main() -> Result<()> {

    set_thread(20);

    let src_bz2_simple = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream-simple.xml.bz2";
    let src_index_simple = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream-index-simple.txt";
    let src_bz2 = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream.xml.bz2";
    let src_index = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream-index.txt";

    let dst_zstd_simple = "C:/a/enwiki/enwiki-20240601-pages-simple.xml.zstd";
    let dst_index_simple = "C:/a/enwiki/enwiki-20240601-index-remapped-simple.txt";
    let dst_zstd = "C:/a/enwiki/enwiki-20240601-pages.xml.zstd";
    let dst_index = "C:/a/enwiki/enwiki-20240601-index-remapped.txt";

    // let a = site_info(
    //     "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream.xml.bz2",
    //     "C:/a/enwiki/enwiki-20240601-site-info.txt",
    //     550
    // )?;
    //
    setup_dump(
        src_bz2_simple,
        src_index_simple,
        dst_zstd_simple,
        dst_index_simple,
    )?;
    //
    // let a = setup_dump(
    //     src_bz2,
    //     src_index,
    //     dst_zstd,
    //     dst_index,
    // )?;
    // sleep(4000);

    let ws = WikySource::from_path(
        dst_index,
        dst_zstd
    ).unwrap();
    // ws.validate_index_dump().unwrap();

    // let opts = OptsBuilder::new()
    //     .user(Some("root"))
    //     .db_name(Some("wiky_base"));
    // let mut conn = Conn::new(opts)?;
    //
    // insert_zstd_range(&mut conn, &ws).unwrap();
    // insert_wiky_index(&mut conn, &ws).unwrap();


    // bench_bz2(src_bz2, src_index).unwrap();
    // ws.bench_zstd().unwrap();

    // use quickxml_to_serde::xml_string_to_json;
    // let mut xml = fs::read(r"C:\a\enwiki\debug\pages82311823-83633146")?;
    // let json = xml_string_to_json(xml, &Config::new_with_defaults()).unwrap();


    // let index_file = fs::File::open(src_index)?;
    // let max = io::BufReader::new(index_file)
    //     .lines()
    //     .flatten()
    //     .map(|line| {
    //         let mut line = line.splitn(3, ':');
    //         let values = [line.next().unwrap(), line.next().unwrap()];
    //         let [st, ed] = values.map(|x| x.parse::<u64>().unwrap());
    //         // ed - st
    //         st
    //     })
    //     .collect::<HashSet<_>>();
    //     // .max().unwrap();
    //
    // println!("{}", max.iter().max().unwrap());
    // println!("{}", max.len());

    // let (k, v) = (m.keys().collect_vec(), m.values().collect_vec());
    // println!("{}", k.len());
    //
    // let v = v.iter().fold(HashMap::new(), |mut a, &&b| { a.entry(b).and_modify(|x| *x += 1).or_insert(1); a });
    //
    // println!("{:?}", v);
    // let b = 0;

    // let mut contents = String::new();
    // decompressor.read_to_string(&mut contents).unwrap();
    // assert_eq!(contents, "Hello, World!");
    Ok(())
}
