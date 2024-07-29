#![feature(hash_raw_entry)]
#![feature(ascii_char)]
#![feature(iter_collect_into)]
#![feature(thread_id_value)]
extern crate core;

mod wiky_source;
mod constant;
mod setup;
mod misc;
mod temp;

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
use nohash_hasher::BuildNoHashHasher;
use quickxml_to_serde::Config;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use regex::Regex;
use zstd_safe::{CCtx, DCtx, create_cdict, CDict, DDict};

use utils::*;

use misc::*;
use setup::*;
use constant::*;
use wiky_source::*;



fn main() -> Result<()> {

    // let s0 = " ";
    // let s1 = "\t ";
    // let s2 = " 2\nb ";
    // let s3 = " abc";
    // let s4 = "abc \t";
    let s5 = " Bicolano people";
    //
    // println!("{:?}", trim_s(s0));
    // println!("{:?}", trim_s(s1));
    // println!("{:?}", trim_s(s2));
    // println!("{:?}", trim_s(s3));
    // println!("{:?}", trim_s(s4));
    //
    // println!("{:?}", &s0[trim_r(s0)]);
    // println!("{:?}", &s1[trim_r(s1)]);
    // println!("{:?}", &s2[trim_r(s2)]);
    // println!("{:?}", &s3[trim_r(s3)]);
    // println!("{:?}", &s4[trim_r(s4)]);


    let comment_re = Regex::new(r"<!--.*?-->").unwrap();
    let code_re = Regex::new(r"<code>.*?</code>").unwrap();
    let nowiki_re = Regex::new(r"<nowiki>.*?</nowiki>").unwrap();
    let noinclude_re = Regex::new(r"<noinclude>.*?</noinclude>").unwrap();
    let includeonly_re = Regex::new(r"<includeonly>.*?</includeonly>").unwrap();
    // let nowiki_re = Regex::new(r"<nowiki>(.*?)</nowiki>").unwrap();
    // let category_re = Regex::new(r"\[\[Category:([^|\]]+)\]\]").unwrap();
    let category_re = Regex::new(r"\[\[Category:([^|\]]*)").unwrap();

    let test0 = "}}<noinclude>

[[Category:Rhodesia <!--politics and government--> templates<!--| -->]]
[[Category:Politics by country sidebar templates|Rhodesia]]
</noinclude>
";
    let mut test0 = test0.to_string();

    unsafe {
        let text_buf = test0.as_bytes_mut();
        let ranges = nowiki_re
            .find_iter(std::str::from_utf8_unchecked(text_buf))
            .chain(includeonly_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
            .map(|m| (m.start(), m.end()))
            .collect_vec();

        ranges.into_iter()
            .for_each(|(s, e)| {
                for i in s..e {
                    text_buf[i] = 0;
                }
            });
    }

    // println!("{test0}");

    let categories = category_re.captures_iter(&test0)
        .map(|c| c.get(1).map(|m| trim_s(m.as_str())))
        .flatten()
        .filter(|s| memchr::memchr(b'\n', s.as_bytes()).is_none())
        // .map(|s| comment_re.replace_all(s, "").to_string())
        .join("|");
    // println!("{categories}");


    temp::init().expect("uh");

    // let text = r#"
    // <html>
    // <!--c0-->
    // <body>
    //     <!-- c2-->
    // </body>
    // </html>
    // "#;
    //
    // // Create a regex pattern to match HTML comments
    // let re = Regex::new(r"<!--.*?-->").unwrap();
    //
    // // Replace the HTML comments with an empty string
    // let result = re.replace_all(text, "");
    //
    // // Print the result
    // println!("{}", result);

    // set_thread(8);
    //
    // let src_bz2_simple = "path/to/dump";
    // let src_index_simple = "path/to/dump";
    // let src_bz2 = "path/to/dump";
    // let src_index = "path/to/dump";
    //
    // let dst_zstd_simple = "path/to/export.zstd";
    // let dst_index_simple = "path/to/export.txt";
    // let dst_zstd = "path/to/export.zstd";
    // let dst_index = "path/to/export.txt";
    //
    // site_info(
    //     src_bz2,
    //     "path/to/site-info.txt",
    //     550
    // )?;
    //
    // setup_dump(
    //     src_bz2,
    //     src_index,
    //     dst_zstd,
    //     dst_index,
    // )?;
    //
    // let ws = WikySource::from_path(
    //     dst_index,
    //     dst_zstd
    // ).unwrap();
    //
    // ws.validate_index_dump().unwrap();
    //
    // // bench_bz2(src_bz2, src_index).unwrap();
    // // ws.bench_zstd().unwrap();
    //
    // let opts = OptsBuilder::new()
    //     .user(Some("root"))     // change username maybe
    //     .db_name(Some("wiky_base"));
    // let mut conn = Conn::new(opts)?;
    //
    // insert_zstd_range(&mut conn, &ws).unwrap();
    // insert_wiky_index(&mut conn, &ws).unwrap();

    // use quickxml_to_serde::xml_string_to_json;
    // let xml = fs::read(r"C:\a\enwiki\debug\pages82311823-83633146")?;
    // let xml = String::from_utf8(xml)?;
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
