


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
use pyo3::prelude::*;
use quickxml_to_serde::Config;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use mysql::*;
use mysql::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use utils::*;
use zstd_safe::{CCtx, DCtx, create_cdict, CDict, DDict};

use crate::misc::*;
use crate::constant::*;
use crate::wiky_source::*;

pub fn setup_dump<P: AsRef<Path>, Q: AsRef<Path>, O: AsRef<Path>, R: AsRef<Path>>(
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

    let offsets = offset_map.keys().copied().sorted().collect_vec();
    let offsets = offsets.iter().zip(offsets[1..].iter().chain(once(&wiki_bz2_len))).collect_vec();

    if Path::new(&dst_zstd).exists() {
        fs::remove_file(&dst_zstd)?;
    }
    let mut zstd_fd = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&dst_zstd)?;

    offsets.chunks(chunk_size)
        .for_each(|ranges| {
            let (&chunk_st, &chunk_ed) = (ranges[0].0, ranges[ranges.len()-1].1);
            println!("--- {chunk_st}:{chunk_ed} - {}  - {:7.4}%", ranges.len(), (chunk_ed as f64 / wiki_bz2_len as f64) * 100.0);

            wiki_bz2.seek(SeekFrom::Start(chunk_st)).unwrap();
            wiki_bz2.read_exact(&mut bz2_raw_buf[..(chunk_ed-chunk_st) as usize]).unwrap();
            // .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} ! {err}"));

            let text_vec = ranges.par_iter().map(|(&st_o, &ed_o)| {
                let (st, ed) = ((st_o-chunk_st) as usize, (ed_o-chunk_st) as usize);

                let mut decompressed_buf = Vec::with_capacity(60_000_000);
                let status = decompress_bz2(&bz2_raw_buf[st..ed], &mut decompressed_buf)
                    .unwrap_or_else(|err| panic!("{st} - {ed} - {err}"));

                if let Err(e) = validate_xml(std::str::from_utf8(&decompressed_buf).unwrap()) {
                    println!("xml_err 1 at: {} - {e}", st_o);
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
                        let dst = &mut *slice_from_raw_parts_mut(zstd_dst_buf[i].as_ptr() as *mut _, zstd_dst_buf[i].len());
                        // zstd::stream::encode_all(&*content, 9).unwrap()
                        // let mut dst1 = vec![0u8; 6_050_000];
                        let dst_size = zstd_safe::compress(dst, &content, 9)
                            .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} - zstd_err:{err}"));
                        &dst[..dst_size]
                        // dst1
                    };
                    (indexs, zstd_chunk)
                })
                .collect::<Vec<_>>();

            for (indexs, zstd_chunk) in output {
                let offset_ed_remapped = offset_st_remapped + zstd_chunk.len();
                indexs.iter().for_each(|(id, title)|
                remapped_index.push((offset_st_remapped, offset_ed_remapped, *id, title))
                );
                zstd_fd.write_all(zstd_chunk).expect("write fail");
                offset_st_remapped = offset_ed_remapped;
                chunk_index += 1;

                #[cfg(any())]
                {
                    let mut dst1 = vec![0u8; 40_000_000];
                    let code = zstd_safe::decompress(&mut dst1, &zstd_chunk).unwrap();
                    let text = std::str::from_utf8(&dst1[..code]).unwrap();

                    if let Err(e) = validate_xml(std::str::from_utf8(&decompressed_buf).unwrap()) {
                        println!("xml_err 1 at: {} - {e}", st_o);
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

    // for (line_index, line_text) in String::from_utf8(wiki_index)?.split('\n').enumerate() {
    //     let mut line = line_text.splitn(3, ':');
    //     let (offset, id, title) = (line.next().unwrap(), line.next().unwrap(), line.next().unwrap());
    //     let (offset, id) = (offset.parse::<u64>().unwrap(), id.parse::<u64>().unwrap());
    //
    //     let end_of_line = line_index == index_line_count;
    //     if end_of_line {
    //         stage_bz2 = stage_bz2
    //             .into_iter()
    //             .filter(|x| !x.is_empty())
    //             .chain(once(vec![(wiki_bz2_len, 0, "")]))
    //             .collect_vec();
    //     }
    //     if offset != last_offset || end_of_line {
    //         bz2_index += 1;
    //         if bz2_index % (THREAD_COUNT+1) == 0 || end_of_line {
    //
    //             let ranges = stage_bz2[0..THREAD_COUNT]
    //                 .iter()
    //                 .zip(stage_bz2[1..].iter())
    //                 .filter(|(a, b)| !a.is_empty() && !b.is_empty())
    //                 .map(|(a, b)| (a[0].0, b[0].0))
    //                 .collect_vec();
    //
    //             ranges.iter().for_each(|x| println!("{x:?}"));
    //
    //             let (chunk_st, chunk_ed) = (ranges[0].0, ranges[ranges.len()-1].1);
    //             wiki_bz2.seek(SeekFrom::Start(chunk_st)).expect("can not seek index");
    //             wiki_bz2.read_exact(&mut bz2_raw_buf[0usize..(chunk_ed-chunk_st) as usize])?;
    //
    //             let extract_vec = ranges.par_iter().enumerate().map(|(i, &(st, ed))| {
    //                 let (st, ed) = ((st - chunk_st) as usize, (ed - chunk_st) as usize);
    //                 let mut decompresser = Decompress::new(false);
    //                 let mut decompressed_buf = Vec::with_capacity(10_000_000);
    //                 let status = decompresser
    //                     .decompress_vec(&bz2_raw_buf[st..ed+1], &mut decompressed_buf)
    //                     .inspect_err(|err| println!("{st} - {ed} - {err}"))
    //                     .unwrap();
    //                 // println!("{st} - {ed} - stats:{status:?}");
    //                 String::from_utf8(decompressed_buf).inspect_err(|err| println!("{st} - {ed} - err:{err:?}")).unwrap_or_default()
    //             }).collect::<Vec<_>>().iter().join("\n");
    //
    //             fs::write(format!("C:/a/enwiki/pages_rs/p{page_index}"), &extract_vec).expect("write fail");
    //             page_index += 1;
    //
    //             println!("index:{line_index} - len:{}", &extract_vec.len());
    //             stage_bz2[0] = stage_bz2[THREAD_COUNT].clone();
    //             stage_bz2[1..].iter_mut().for_each(|v| v.clear());
    //             bz2_index = 0;
    //             input("uha:?").expect("uha?");
    //         }
    //         // let a = stage_bz2.iter().map(|x| x.iter().map(|(a, b, _)| format!("{a}, {b}")).join("\n")).join("\n-----------------\n");
    //         // input(a).expect("uh");
    //         last_offset = offset;
    //     }
    //     stage_bz2[bz2_index].push((offset, id, title));
    // }

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

#[pyfunction]
pub fn set_thread(n: usize) {
    THREAD_COUNT.set(n).unwrap();
    ThreadPoolBuilder::new().num_threads(*THREAD_COUNT.get().unwrap_or(&4)).build_global().unwrap();
}

pub fn insert_wiky_index(conn: &mut Conn, ws: &WikySource) -> Result<()> {

    conn.query_drop("delete from wiky_index").unwrap();
    ws.chunks((*THREAD_COUNT.get().unwrap_or(&4)) * 20, |chunk_st, chunk_ed, ranges, zstd_buf| {
        let mut tx = conn.start_transaction(TxOpts::default())?;
        let result = tx.exec_batch(
            r"
            insert into wiky_index (zstd_st, page_id, page_title)
            values (:zstd_st, :page_id, :page_title)",
            ranges.iter()
                .flat_map(|(st, ed, v)| v.iter().map(move |(pid, title)| {
                    (st, pid, title)
                }))
                .map(|(st, pid, title)| params! {
                    "zstd_st" => st,
                    "page_id" => pid,
                    "page_title" => title,
                })
        ).context(format!("insert failed at {chunk_st}:{chunk_ed}"));
        tx.commit().context(format!("commit failed at {chunk_st}:{chunk_ed}"))?;
        result
    }).collect::<Result<()>>()
}

pub fn insert_zstd_range(conn: &mut Conn, ws: &WikySource) -> Result<()> {

    conn.query_drop("delete from wiky_index").unwrap();
    conn.query_drop("delete from zstd_range").unwrap();
    ws.chunks((*THREAD_COUNT.get().unwrap_or(&4)) * 20, |chunk_st, chunk_ed, ranges, zstd_buf| {
        let mut tx = conn.start_transaction(TxOpts::default())?;
        let result = tx.exec_batch(r"
            insert into zstd_range (st, ed)
            values (:st, :ed)",
            ranges.iter().map(|(st, ed, v)| params! {
                "st" => st,
                "ed" => ed,
            }),
        ).context(format!("insert failed at {chunk_st}:{chunk_ed}"));
        tx.commit().context(format!("commit failed at {chunk_st}:{chunk_ed}"))?;
        result
    }).collect::<Result<()>>()
}