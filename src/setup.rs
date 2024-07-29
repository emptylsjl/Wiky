


use std::collections::{HashMap, HashSet};
use std::{fs, io, thread, time, vec};
use std::fmt::{Display, format, Formatter};
use std::fs::File;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::io::prelude::*;
use std::iter::once;
use std::ops::{Add, Shr, Sub};
use std::path::{Path, PathBuf};
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};
use std::time::Instant;

use bzip2::{Compression, Decompress};
use bzip2::read::{BzEncoder, BzDecoder};
use anyhow::{Context, Result};
use chrono::DateTime;
use itertools::Itertools;
use nohash_hasher::BuildNoHashHasher;
use pyo3::prelude::*;
use quickxml_to_serde::Config;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use mysql::*;
use mysql::binlog::TransactionPayloadCompressionType::NONE;
use mysql::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use quick_xml::de;
use quick_xml::name::QName;
use regex::Regex;
use serde::Deserialize;
use utils::*;
use zstd_safe::{CCtx, DCtx, create_cdict, CDict, DDict, WriteBuf};

use crate::misc::*;
use crate::constant::*;
use crate::wiky_source::*;

pub fn get_wiki_bz2_offsets(
    wiki_index: &[u8],
    wiki_bz2_len: u64
) -> Result<(Vec<(u64, u64)>, HashMap<u64, Vec<(u64, &str)>, BuildNoHashHasher<u8>>)> {

    let index_map = HashMap::<u64, Vec<_>, BuildNoHashHasher<u8>>::with_capacity_and_hasher(
        400_000, BuildNoHashHasher::default()
    );

    let mut offset_map = std::str::from_utf8(&wiki_index)?
        .trim()
        .split('\n')
        .map(|line| {
            let mut line = line.splitn(3, ':');
            let (offset, id, title) = (line.next().unwrap(), line.next().unwrap(), line.next().unwrap());
            let (offset, id) = (offset.parse::<u64>().unwrap(), id.parse::<u64>().unwrap());
            // println!("{:?}", (offset, id, title));
            (offset, id, title)
        })
        .fold(index_map, |mut m, (offset, id, title)| {
            m.entry(offset)
                .and_modify(|v| v.push((id, title)))
                .or_insert({
                    let mut v = Vec::with_capacity(100);
                    v.push((id, title));
                    v
                });
            m
        });

    let offsets = offset_map.keys().copied().sorted().collect_vec();
    let offsets = offsets.iter().copied().zip(offsets[1..].iter().copied().chain(once(wiki_bz2_len))).collect_vec();
    Ok((offsets, offset_map))
}

pub fn setup_dump_chunk<P: AsRef<Path>, Q: AsRef<Path>, O: AsRef<Path>, R: AsRef<Path>>(
    src_bz2: P, src_index: Q, dst_zstd: O, dst_index: R
) -> Result<()> {

    // let a = time::Instant::now();
    // let wiki_index_bz2 = fs::read("C:/a/enwiki/enwiki-20240601-pages-articles-multistream-index.txt.bz2")?;
    // let mut decompresser = Decompress::new(false);
    // let mut wiki_index = Vec::with_capacity(1_123_603_000);
    // let status = decompresser.decompress_vec(&wiki_index_bz2, &mut wiki_index)?;
    // println!("wiki_index_bz2 decompress: {status:?} - {:?}", a.elapsed());

    let mut wiki_bz2 = fs::File::open(src_bz2).context("can not open file")?;
    let wiki_index = fs::read(src_index).context("can not open file")?;
    let wiki_bz2_len = wiki_bz2.metadata().unwrap().len();
    println!("{wiki_bz2_len} - {}", memchr::Memchr::new(b'\n', &wiki_index).count());

    let (offsets, offset_map) = get_wiki_bz2_offsets(&wiki_index, wiki_bz2_len)?;

    let mut chunk_index = 0;
    // let mut last_offset = 550u64;
    let block_bz2_size = 6_000_000;
    let chunk_size = *THREAD_COUNT.get().unwrap_or(&4) * 10;
    // // let a = Vec::<u64>::with_capacity(120);
    // let mut bz2_index = 0;
    // let mut stage_bz2 = vec![Vec::<(u64, u64, &str)>::with_capacity(120); THREAD_COUNT+1];
    let mut bz2_raw_buf = vec![0; block_bz2_size * chunk_size];
    // let mut bz2_extract_buf: Vec::<Vec<u8>> = vec![vec![0; 20_000_000]; THREAD_COUNT];
    let mut zstd_dst_buf = vec![vec![0u8; 6_050_000*5]; *THREAD_COUNT.get().unwrap_or(&4) * 10 / 4 + 1];
    let mut remapped_index = vec![];

    let mut offset_st_remapped = 0;


    if dst_zstd.as_ref().exists() {
        fs::remove_file(&dst_zstd)?;
    }
    let mut zstd_fd = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&dst_zstd)?;

    offsets.chunks(chunk_size)
        .for_each(|ranges| {
            let (chunk_st, chunk_ed) = (ranges[0].0, ranges[ranges.len()-1].1);
            println!("--- {chunk_st}:{chunk_ed} - {}  - {:7.4}%", ranges.len(), (chunk_ed as f64 / wiki_bz2_len as f64) * 100.0);

            wiki_bz2.seek(SeekFrom::Start(chunk_st)).unwrap();
            wiki_bz2.read_exact(&mut bz2_raw_buf[..(chunk_ed-chunk_st) as usize]).unwrap();
            // .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} ! {err}"));

            let text_vec = ranges.par_iter().map(|(st_o, ed_o)| {
                let (st, ed) = ((st_o-chunk_st) as usize, (ed_o-chunk_st) as usize);

                let mut decompressed_buf = Vec::with_capacity(60_000_000);
                let status = decompress_bz2(&bz2_raw_buf[st..ed], &mut decompressed_buf)
                    .unwrap_or_else(|err| panic!("{st} - {ed} - {err}"));

                if let Err(e) = validate_xml(std::str::from_utf8(&decompressed_buf).unwrap()) {
                    println!("xml_err at: {} - {e}", st_o);
                };

                decompressed_buf
            }).collect::<Vec<_>>();

            let indexs = ranges.iter()
                .map(|(st, _)| offset_map.get(st).unwrap())
                .collect::<Vec<_>>();

            let output = indexs.chunks(4)
                .zip(text_vec.chunks(4))
                .enumerate()
                .par_bridge()
                .map(|(i, (indexs, content))| {
                    let content = content.concat();
                    let indexs = indexs.iter().flat_map(|&x| x).collect_vec();

                    let zstd_chunk = unsafe {
                        // let dst = &mut *slice_from_raw_parts_mut(zstd_dst_buf[i].as_ptr() as *mut _, zstd_dst_buf[i].len());
                        // zstd::stream::encode_all(&*content, 9).unwrap()
                        let mut dst = vec![0u8; 60_000_000];
                        let dst_size = zstd_safe::compress(&mut dst, &content, 9)
                            .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} - zstd_err:{err}"));
                        // &dst[..dst_size]
                        dst.truncate(dst_size);
                        dst
                    };
                    (indexs, zstd_chunk)
                })
                .collect::<Vec<_>>();

            for (indexs, zstd_chunk) in output {
                let offset_ed_remapped = offset_st_remapped + zstd_chunk.len();
                indexs.iter().for_each(|(id, title)|
                    remapped_index.push((offset_st_remapped, offset_ed_remapped, *id, title))
                );
                zstd_fd.write_all(&zstd_chunk).expect("write fail");
                offset_st_remapped = offset_ed_remapped;
                chunk_index += 1;

                #[cfg(any())]
                {
                    let mut dst1 = vec![0u8; 40_000_000];
                    let code = zstd_safe::decompress(&mut dst1, &zstd_chunk).unwrap();
                    let text = std::str::from_utf8(&dst1[..code]).unwrap();

                    if let Err(e) = validate_xml(text) {
                        println!("xml_err 1 at: {} - {e}", indexs[0].0);
                    };

                    let validate = indexs.iter().all(|(id, title)| {
                        text.contains(&("<id>".to_owned()+&id.to_string()+"</id>"))
                            && text.contains(&("<title>".to_owned()+title.trim()+"</title>"))
                    });
                    if !validate {
                        println!("validate fail {validate:5} - st:{}", indexs[0].0);
                    }
                }
            }
        });

    let remapped_index_text = remapped_index.iter().map(|(a, b, c, d)| format!("{a}:{b}:{c}:{d}")).join("\n");
    fs::write(dst_index, remapped_index_text).expect("?");

    Ok(())
}

pub fn site_info<P: AsRef<Path>, Q: AsRef<Path>>(src_bz2: P, dst_text: Q, offset: usize) -> Result<()> {
    let mut wiki_bz2 = fs::File::open(src_bz2).context("open bz2 fail")?;
    let mut bz2_raw_buf = vec![0; offset];
    wiki_bz2.read_exact(&mut bz2_raw_buf).context("read site_info from multistream fail")?;

    let mut decompressed_buf = Vec::with_capacity(1_000_000);
    let status = decompress_bz2(&bz2_raw_buf, &mut decompressed_buf)
        .context("decompress bz2 fail")?;
    println!("site info bz2 status: {status:?}");

    let site_info_text = std::str::from_utf8(&decompressed_buf)?.trim();
    fs::write(dst_text, site_info_text).expect("write site_info fail");
    Ok(())
}

pub fn bench_bz2<P: AsRef<Path>, Q: AsRef<Path>>(src_bz2: P, src_index: Q) -> Result<()> {

    let mut wiki_bz2 = fs::File::open(src_bz2).context("can not open file")?;
    let wiki_index = fs::read(src_index).context("can not open file")?;
    let wiki_bz2_len = wiki_bz2.metadata().unwrap().len();
    println!("{wiki_bz2_len} - {}", memchr::Memchr::new(b'\n', &wiki_index).count());

    let index_map = HashMap::<u64, Vec<_>, BuildNoHashHasher<u8>>::with_capacity_and_hasher(
        400_000, BuildNoHashHasher::default()
    );

    let mut offset_map = std::str::from_utf8(&wiki_index)?
        .trim()
        .split('\n')
        .map(|line| {
            let mut line = line.splitn(3, ':');
            let (offset, id, title) = (line.next().unwrap(), line.next().unwrap(), line.next().unwrap());
            let (offset, id) = (offset.parse::<u64>().unwrap(), id.parse::<u64>().unwrap());
            // println!("{:?}", (offset, id, title));
            (offset, id, title)
        })
        .fold(index_map, |mut m, (offset, id, title)| {
            m.entry(offset)
                .and_modify(|v| v.push((id, title)))
                .or_insert({
                    let mut v = Vec::with_capacity(100);
                    v.push((id, title));
                    v
                });
            m
        });

    let offsets = offset_map.keys().copied().sorted().collect_vec();
    let offsets = offsets.iter().zip(offsets[1..].iter().chain(once(&wiki_bz2_len))).collect_vec();

    println!("bz2 chunk len: {}", offsets.len());
    println!("bz2 chunk size max: {}", offsets.iter().map(|(&a, &b)| b - a).max().unwrap());
    println!("bz2 chunk size mean: {}", offsets.iter().map(|(&a, &b)| b - a).sum::<u64>() as f64 / offsets.len() as f64);

    let block_bz2_size = 6_000_000;
    let chunk_size = *THREAD_COUNT.get().unwrap_or(&4) * 10;
    let mut bz2_raw_buf = vec![0; block_bz2_size * chunk_size];

    let now = time::Instant::now();

    let sizes = offsets.chunks(chunk_size)
        .flat_map(|ranges| {
            let (&chunk_st, &chunk_ed) = (ranges[0].0, ranges[ranges.len() - 1].1);
            println!("--- {chunk_st}:{chunk_ed} - {:7.4}%", (chunk_ed as f64 / wiki_bz2_len as f64) * 100.0);

            wiki_bz2.seek(SeekFrom::Start(chunk_st)).unwrap();
            wiki_bz2.read_exact(&mut bz2_raw_buf[..(chunk_ed - chunk_st) as usize]).unwrap();
            // .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} ! {err}"));

            ranges.par_iter().map(|(&st, &ed)| {
                let (st, ed) = ((st - chunk_st) as usize, (ed - chunk_st) as usize);

                let mut decompressed_buf = Vec::with_capacity(10_000_000);
                let status = decompress_bz2(&bz2_raw_buf[st..ed], &mut decompressed_buf)
                    .unwrap_or_else(|err| panic!("{st} - {ed} - {err}"));
                decompressed_buf.len()
            }).collect::<Vec<_>>()
        }).collect_vec();

    println!("t:{THREAD_COUNT:?}\nelapsed: {:?}", now.elapsed());

    println!("text chunk len: {}", sizes.len());
    println!("text chunk size max: {}", sizes.iter().max().unwrap());
    println!("text chunk size mean: {}", sizes.iter().sum::<usize>() as f64 / sizes.len() as f64);

    Ok(())
}

pub fn parse_wiki_history<R: BufRead, W: Write>(reader: R, writer: &mut W, chunk_id: u32) -> Result<()> {

    let mut reader = Reader::from_reader(reader);
    // let mut reader = Reader::from(&dst[..dst_size]);
    // reader.config_mut().trim_text(true);

    // let mut metas = vec![];
    let mut idx = 0;
    let [mut ct, mut ctt] = [0, 0];
    let mut in_id = false;
    let mut in_page = false;
    let mut in_revi = false;
    let mut xml_buf = vec![];
    let mut page_buf = vec![];

    loop {
        xml_buf.clear();
        match reader.read_event_into(&mut xml_buf) {
            Ok(Event::Start(e)) => {
                if e.name() == QName(b"page") {
                    in_page = true;
                }
                if in_page {
                    page_buf.push(b'<');
                    page_buf.extend_from_slice(e.as_slice());
                    page_buf.push(b'>');
                }
            }
            Ok(Event::End(e)) if in_page => {
                page_buf.extend_from_slice(b"</");
                page_buf.extend_from_slice(e.as_slice());
                page_buf.push(b'>');
                if e.name() == QName(b"page") {
                    in_page = false;
                    let page_meta = de::from_reader::<_, PageMeta>(page_buf.as_slice()).unwrap();
                    page_buf.clear();
                    ctt += 1;

                    if page_meta.redirect.is_none() {
                        ct += 1;
                        write!(
                            writer, "{},{}\n",
                            page_meta.page_id,
                            page_meta.revisions.iter().map(ToString::to_string).join(",")
                        ).unwrap();
                    }
                    if ct % 1000 == 0 {
                        println!("chunk:{chunk_id} thread:{:<4} page:{ct} - total:{ctt}", thread::current().id().as_u64().get());
                    }

                }
            }
            Ok(Event::Empty(e)) if in_page => {
                page_buf.push(b'<');
                page_buf.extend_from_slice(e.as_slice());
                page_buf.extend_from_slice(b"/>");
            }
            Ok(Event::Text(e)) if in_page => { page_buf.extend_from_slice(e.as_slice()); }

            Ok(Event::Eof) => break,

            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => {}
        }
    };
    // let pages2 = pages.into_iter().map(String::from_utf8).collect::<std::result::Result<Vec<String>, _>>();
    // // let uha = pages2.clone().unwrap();
    // // let c = 0;
    // // let d = 0;
    // pages2.context("collect page failed")
    Ok(())
}

pub fn setup_history() {

    let meta_historys = [
        (01, "path/to/enwiki-20240601-stub-meta-history01.xml", "dst/path/enwiki-20240601-fullmeta-remapped01.xml"),
        (02, "path/to/enwiki-20240601-stub-meta-history02.xml", "dst/path/enwiki-20240601-fullmeta-remapped02.xml"),
        (03, "path/to/enwiki-20240601-stub-meta-history03.xml", "dst/path/enwiki-20240601-fullmeta-remapped03.xml"),
        (04, "path/to/enwiki-20240601-stub-meta-history04.xml", "dst/path/enwiki-20240601-fullmeta-remapped04.xml"),
        (05, "path/to/enwiki-20240601-stub-meta-history05.xml", "dst/path/enwiki-20240601-fullmeta-remapped05.xml"),
        (06, "path/to/enwiki-20240601-stub-meta-history06.xml", "dst/path/enwiki-20240601-fullmeta-remapped06.xml"),
        (07, "path/to/enwiki-20240601-stub-meta-history07.xml", "dst/path/enwiki-20240601-fullmeta-remapped07.xml"),
        (08, "path/to/enwiki-20240601-stub-meta-history08.xml", "dst/path/enwiki-20240601-fullmeta-remapped08.xml"),
        (09, "path/to/enwiki-20240601-stub-meta-history09.xml", "dst/path/enwiki-20240601-fullmeta-remapped09.xml"),
        (10, "path/to/enwiki-20240601-stub-meta-history10.xml", "dst/path/enwiki-20240601-fullmeta-remapped10.xml"),
        (11, "path/to/enwiki-20240601-stub-meta-history11.xml", "dst/path/enwiki-20240601-fullmeta-remapped11.xml"),
        (12, "path/to/enwiki-20240601-stub-meta-history12.xml", "dst/path/enwiki-20240601-fullmeta-remapped12.xml"),
        (13, "path/to/enwiki-20240601-stub-meta-history13.xml", "dst/path/enwiki-20240601-fullmeta-remapped13.xml"),
        (14, "path/to/enwiki-20240601-stub-meta-history14.xml", "dst/path/enwiki-20240601-fullmeta-remapped14.xml"),
        (15, "path/to/enwiki-20240601-stub-meta-history15.xml", "dst/path/enwiki-20240601-fullmeta-remapped15.xml"),
        (16, "path/to/enwiki-20240601-stub-meta-history16.xml", "dst/path/enwiki-20240601-fullmeta-remapped16.xml"),
        (17, "path/to/enwiki-20240601-stub-meta-history17.xml", "dst/path/enwiki-20240601-fullmeta-remapped17.xml"),
        (18, "path/to/enwiki-20240601-stub-meta-history18.xml", "dst/path/enwiki-20240601-fullmeta-remapped18.xml"),
        (19, "path/to/enwiki-20240601-stub-meta-history19.xml", "dst/path/enwiki-20240601-fullmeta-remapped19.xml"),
        (20, "path/to/enwiki-20240601-stub-meta-history20.xml", "dst/path/enwiki-20240601-fullmeta-remapped20.xml"),
        (21, "path/to/enwiki-20240601-stub-meta-history21.xml", "dst/path/enwiki-20240601-fullmeta-remapped21.xml"),
        (22, "path/to/enwiki-20240601-stub-meta-history22.xml", "dst/path/enwiki-20240601-fullmeta-remapped22.xml"),
        (23, "path/to/enwiki-20240601-stub-meta-history23.xml", "dst/path/enwiki-20240601-fullmeta-remapped23.xml"),
        (24, "path/to/enwiki-20240601-stub-meta-history24.xml", "dst/path/enwiki-20240601-fullmeta-remapped24.xml"),
        (25, "path/to/enwiki-20240601-stub-meta-history25.xml", "dst/path/enwiki-20240601-fullmeta-remapped25.xml"),
        (26, "path/to/enwiki-20240601-stub-meta-history26.xml", "dst/path/enwiki-20240601-fullmeta-remapped26.xml"),
        (27, "path/to/enwiki-20240601-stub-meta-history27.xml", "dst/path/enwiki-20240601-fullmeta-remapped27.xml"),
    ].as_slice();

    meta_historys.iter().par_bridge().for_each(|(i, meta_path, dst_path)| {

        let meta_src = File::open(meta_path).expect("Failed to open file");
        let meta_dst = File::create(dst_path).expect("Failed to open file");
        let mut buf_reader = BufReader::new(meta_src);
        let mut buf_writer = BufWriter::new(meta_dst);
        parse_wiki_history(buf_reader, &mut buf_writer, *i).unwrap();

    });
}

#[pyfunction]
pub fn set_thread(n: usize) {
    THREAD_COUNT.set(n).unwrap();
    ThreadPoolBuilder::new().num_threads(*THREAD_COUNT.get().unwrap_or(&4)).build_global().unwrap();
}

#[derive(Debug)]
pub struct CateLink<'a> {
    pub id: u64,
    pub title: &'a str,
    pub cates: Vec<&'a str>,
}

impl CateLink<'_> {
    pub fn new<'a>(id: u64, title: &'a str, cates: Vec<&'a str>) -> CateLink<'a> {
        CateLink { id, title, cates, }
    }
}

// #[derive(Debug)]
// pub struct CateLink {
//     pub id: String,
//     pub title: String,
//     pub cates: Vec<String>,
// }
//
// impl CateLink {
//     pub fn new(id: String, title: String, cates: Vec<String>) -> CateLink {
//         CateLink { id, title, cates, }
//     }
// }

// pub fn load_category(page_index: &str) -> Vec<(&str, &str, Vec<&str>)> {
pub fn load_category(page_index: &str) -> Vec<CateLink> {
    let page_index = trim_s(&page_index)
        .split("\n")
        .par_bridge()
        .filter_map(|s| {
            let mut sp = s.split('|');
            let (id, title, cates) = (
                sp.next().unwrap(),
                sp.next().unwrap(),
                sp.filter(|x| !x.is_empty()).collect_vec()
            );
            if title.starts_with("category:") && !cates.is_empty() {
                // Some(CateLink::new(id, &title[9..], cates))
                Some(CateLink::new(id.parse().unwrap(), &title[9..], cates))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    println!("L: {}", page_index.len());
    page_index
}

pub fn load_page(page_index: &str) -> Vec<CateLink> {
    let page_list = trim_s(&page_index)
        .split("\n")
        .par_bridge()
        .filter_map(|s| {
            let mut sp = s.split('|');
            let (id, title) = (sp.next().unwrap(), sp.next().unwrap());
            let cates = sp.filter(|x| !x.is_empty()).collect_vec();

            let start_with = title.starts_with("category:") ||
                title.starts_with("module:") || title.starts_with("template:") ||
                title.starts_with("portal:") || title.starts_with("help:");

            if !start_with && !cates.is_empty() {
                Some(CateLink::new(id.parse().unwrap(), title, cates))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    page_list
}

pub fn insert_wiky_category(conn: &mut Conn, page_index: &[CateLink]) -> Result<()> {

    let mut ct = 0u64;
    page_index.chunks(1000).try_for_each(|ps| {
        let mut tx = conn.start_transaction(TxOpts::default())?;
        tx.exec_batch(
            r"
            insert into wiky_category (page_id, category)
            values (:page_id, :category)
            ",
            ps.iter()
                .inspect(|_| {
                    ct += 1;
                    if ct % 1000 == 0 {
                        println!("{:.3}", ct as f64 /page_index.len() as f64)
                    }
                })
                // .inspect(|c| println!("{} - {}", c.id, c.title))
                .map(|c| params! {
                    "page_id" => c.id,
                    "category" => c.title,
                })
        ).context("insert category failed")?;
        tx.commit().unwrap();
        Ok(())
    })
}

pub fn insert_zstd_range(conn: &mut Conn, ws: &WikySource) -> Result<()> {

    conn.query_drop("delete from wiky_index").unwrap();
    conn.query_drop("delete from zstd_range").unwrap();
    ws.chunk_map((*THREAD_COUNT.get().unwrap_or(&4)) * 20, |chunk_st, zstd_bufs| {
        let mut tx = conn.start_transaction(TxOpts::default())?;
        let result = tx.exec_batch(
            r"
            insert into zstd_range (st, ed)
            values (:st, :ed)
            ",
            zstd_bufs.iter().map(|(st, buf, v)| params! {
                "st" => st,
                "ed" => st + buf.len() as u64,
            }),
        ).context(format!("insert failed st:{chunk_st}"));
        tx.commit().context(format!("commit failed st:{chunk_st}"))?;
        result
    }).collect::<Result<()>>()
}

pub fn insert_wiky_index(conn: &mut Conn, ws: &WikySource) -> Result<()> {

    conn.query_drop("delete from wiky_index").unwrap();
    let rt = ws.chunk_map(*oc_get(&THREAD_COUNT), |ck_st, zstd_buf| {
        println!("{ck_st}");

        let iters = zstd_buf.iter().map(|(st, buf, v)| {
            let mut dst = vec![0; 200_000_000];
            let dst_size = zstd_safe::decompress(&mut dst, buf)
                .unwrap_or_else(|err| panic!("st:{ck_st} - zstd_err:{err}"));
            let pages = de::from_reader::<_, Vec<PageText>>(&dst[..dst_size])
                .unwrap_or_else(|err| panic!("st:{ck_st} - xml_de_err:{err}"));

            let link_re = Regex::new(r"\[\[([^\]]+)\]\]").unwrap();
            let sect_re = Regex::new(r"==[^=]+==").unwrap();

            let mut tx = conn.start_transaction(TxOpts::default()).unwrap();

            let page_iter = pages.iter()
                .map(|p| {
                    let (link_count, sect_count) = p.revisions.text.text.as_ref().map(|s| {
                        (link_re.find_iter(&s).count(), sect_re.find_iter(&s).count())
                    }).unwrap_or_default();

                    let page_type = if p.redirect.is_some() {
                        "redirect"
                    } else if p.title.starts_with("Module:") {
                        "module"
                    } else if p.title.starts_with("Template:") {
                        "template"
                    } else if p.title.starts_with("Portal:") {
                        "portals"
                    } else if p.title.starts_with("Category:") {
                        "categories"
                    } else if p.title.starts_with("Help:") {
                        "help"
                    } else if p.title.starts_with("Wikipedia:") {
                        "wikipedia"
                    } else {
                        "article"
                    }.to_string();
                    (p.page_id.parse::<u64>().unwrap(), &p.title, link_count, sect_count, page_type)
                });

            tx.exec_batch(
                r"
                insert into wiky_index (zstd_st, page_id, page_title)
                values (:zstd_st, :page_id, :page_title)
                ",
                page_iter.map(|(pid, title, lk, sc, p_type)| params! {
                    "zstd_st" => st,
                    "page_id" => pid,
                    "page_title" => title,
                    "link_count" => lk,
                    "sect_count" => sc,
                    "page_type" => p_type,
                })
            ).context(format!("insert failed st:{st}")).unwrap();
            tx.commit().context(format!("commit failed st:{st}")).unwrap();
        }).count();
    }).count();
    Ok(())
}
