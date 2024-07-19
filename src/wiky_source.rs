
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
use quickxml_to_serde::{Config, xml_string_to_json};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use pyo3::prelude::*;

use crate::constant::*;
use crate::misc::*;
use crate::setup;

pub struct OffsetId {
    st_id: u64,
    ed: u64,
}

impl OffsetId {
    fn new(st: u64, ed: u64, id: u64) -> Self {
        Self {
            st_id: Self::merge(st, id), ed
        }
    }

    fn merge(st: u64, id: u64) -> u64 {
        st<<28 | id
    }

    fn split(st_id: u64) -> (u64, u64) {
        let pid = st_id & 0xFFFFFFF;
        let offset = st_id >> 28;
        (offset, pid)
    }

    fn st_id(&self) -> u64 {
        self.st_id
    }

    fn id(&self) -> u64 {
        self.st_id & 0xFFFFFFF
    }

    fn st(&self) -> u64 {
        self.st_id >> 28
    }

    fn ed(&self) -> u64 {
        self.ed
    }
}

#[cfg(feature = "index_u64")]
type Indexes = u64;
#[cfg(not(feature = "index_u64"))]
type Indexes = (u64, String);

#[pyclass]
#[derive(Clone)]
pub struct WikySource {
    pub index_path: PathBuf,
    pub zstd_path: PathBuf,
    pub zstd_len: u64,
    // pub indexes: HashMap<(u64, u64), Vec<(u64, String)>, BuildNoHashHasher<u8>>,
    pub indexes: Vec<(u64, u64, Vec<Indexes>)>,
}

impl WikySource {
    pub fn from_path<P: AsRef<Path>, Q: AsRef<Path>>(index_path: P, zstd_path: Q) -> PyResult<Self> {

        let index_path = index_path.as_ref().to_path_buf();
        let zstd_path = zstd_path.as_ref().to_path_buf();
        let index_file = fs::File::open(&index_path).context("open index fail")?;
        let wiki_zstd = fs::File::open(&zstd_path).context("can not open file")?;
        let zstd_len = wiki_zstd.metadata().unwrap().len();

        let mut last_st = 1;
        let mut indexes = Vec::with_capacity(240_000);
        io::BufReader::new(index_file)
            .lines()
            .flatten()
            .map(|line_text| {
                let mut line = line_text.splitn(4, ':');
                let values = [line.next().unwrap(), line.next().unwrap(), line.next().unwrap()];
                let [st, ed, id] = values.map(|x| x.parse::<u64>().unwrap_or_else(|e| panic!("{line_text} - {values:?} - {e}")));
                (st, ed, id, line.next().unwrap().to_string())
            })
            .for_each(|(st, ed, id, title)| {
                if st != last_st {
                    let mut v = Vec::with_capacity(400);
                    #[cfg(feature = "index_u64")]
                    v.push(id);
                    #[cfg(not(feature = "index_u64"))]
                    v.push((id, title));
                    indexes.push((st, ed, v));
                    last_st = st;
                } else {
                    #[cfg(feature = "index_u64")]
                    indexes.last_mut().unwrap().2.push(id);
                    #[cfg(not(feature = "index_u64"))]
                    indexes.last_mut().unwrap().2.push((id, title));
                }
            });

        Ok(Self {
            index_path,
            zstd_path,
            zstd_len,
            indexes,
        })
    }

    pub fn open_zstd(&self) -> Result<File>{
        fs::OpenOptions::new()
            .read(true)
            .open(&self.zstd_path)
            .context("open zstd fail")
    }

    pub fn chunks<'a, T, F: FnMut(u64, u64, &[(u64, u64, Vec<Indexes>)], &mut [u8]) -> T + 'a>(
        &'a self,
        chunk_size: usize,
        mut runner: F
    ) -> impl Iterator<Item = T> + 'a {

        let mut wiki_zstd = self.open_zstd().unwrap();
        let mut zstd_buf = vec![0; 6_200_000 * (*THREAD_COUNT.get().unwrap_or(&4)) * 100];

        self.indexes.chunks(chunk_size).map(move |ranges| {
            let (chunk_st, chunk_ed) = (ranges[0].0, ranges[ranges.len()-1].1);

            wiki_zstd.seek(SeekFrom::Start(chunk_st)).unwrap();
            wiki_zstd.read_exact(&mut zstd_buf[..(chunk_ed-chunk_st) as usize]).unwrap();

            runner(chunk_st, chunk_ed, ranges, &mut zstd_buf)
        })
    }
}


#[pymethods]
impl WikySource {
    #[new]
    pub fn new(index_path: &str, zstd_path: &str) -> PyResult<Self> {
        Self::from_path(index_path, zstd_path)
    }

    pub fn decode_chunk(&self, chunk_st: usize, chunk_ed: usize) -> PyResult<String> {
        let mut zstd_buf = vec![0; chunk_ed-chunk_st];
        let mut wiki_zstd = self.open_zstd().unwrap();

        wiki_zstd.seek(SeekFrom::Start(chunk_st as u64)).unwrap();
        wiki_zstd.read_exact(&mut zstd_buf[..(chunk_ed-chunk_st)]).unwrap();

        let mut dst = vec![0; 60_200_000];
        let len = zstd_safe::decompress(&mut dst, &zstd_buf)
            .map_err(|e| py_err(e.to_string()))?;
        dst.truncate(len);
        String::from_utf8(dst).map_err(|e| py_err(e.to_string()))
    }

    pub fn decode_page_json(&self, chunk_st: usize, chunk_ed: usize, page_id: u64) -> PyResult<String> {
        let chunk_text = self.decode_chunk(chunk_st, chunk_ed)?;

        let conf = Config::new_with_defaults();
        let json = xml_string_to_json(format!("<root>{chunk_text}</root>"), &conf)
            .map_err(|e| py_err("malformed ".to_owned()+&e.to_string()))?;
        // let a = json[""];
        Ok(json.to_string())
    }

    pub fn validate_index_dump(&self) -> PyResult<()> {

        let _ = self.chunks((*THREAD_COUNT.get().unwrap_or(&4)) * 20, |chunk_st, chunk_ed, ranges, zstd_buf| {

            let valid = ranges.par_iter().all(|(st, ed, v)| {
                let (st, ed) = ((st - chunk_st) as usize, (ed - chunk_st) as usize);

                let mut dst = vec![0; 200_000_000];
                let dst_size = zstd_safe::decompress(&mut dst, &zstd_buf[st..ed])
                    .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} - zstd_err:{err}"));
                let text = std::str::from_utf8(&dst[..dst_size]).unwrap();

                #[cfg(feature = "index_u64")]
                let valid = v.iter().all(|id| {
                    text.contains(&("<id>".to_owned()+&id.to_string()+"</id>"))
                });
                #[cfg(not(feature = "index_u64"))]
                let valid = v.iter().all(|(id, title)| {
                    text.contains(&("<id>".to_owned()+&id.to_string()+"</id>"))
                        && text.contains(&("<title>".to_owned()+title.trim()+"</title>"))
                });
                let valid = valid && validate_xml(text).is_ok();
                if !valid {
                    fs::write(format!("C:/a/enwiki/debug/pages{st}-{ed}"), text).unwrap();
                    fs::write(format!("C:/a/enwiki/debug/index{st}-{ed}"), v.iter().map(|x| format!("{x:?}")).join("\n")).unwrap();
                }
                valid
            });
            println!("- validate {valid:5} - {chunk_ed}  - {:7.4}%", (chunk_ed as f64 / self.zstd_len as f64) * 100.0);
            valid
        }).count();

        Ok(())
    }

    pub fn bench_zstd(&self) -> PyResult<()> {

        println!("zstd chunk len: {}", self.indexes.len());
        println!("zstd chunk size max: {}", self.indexes.iter().map(|(a, b, c)| b - a).max().unwrap());
        println!("zstd chunk size mean: {}", self.indexes.iter().map(|(a, b, c)| b - a).sum::<u64>() as f64 / self.indexes.len() as f64);

        let now = time::Instant::now();

        let sizes = self.chunks(
            (*THREAD_COUNT.get().unwrap_or(&4)) * 20,
            |chunk_st, chunk_ed, ranges, zstd_buf| {
                println!("--- {chunk_st}:{chunk_ed} - {:7.4}%", (chunk_ed as f64 / self.zstd_len as f64) * 100.0);

                ranges.par_iter().map(|(st, ed, v)| {
                    let (st, ed) = ((st - chunk_st) as usize, (ed - chunk_st) as usize);

                    let mut dst = vec![0; 60_200_000];
                    let dst_size = zstd_safe::decompress(&mut dst, &zstd_buf[st..ed])
                        .unwrap_or_else(|err| panic!("{chunk_st} - {chunk_ed} - zstd_err:{err}"));
                    dst_size
                }).collect::<Vec<_>>()

            }).flatten().collect_vec();

        println!("t:{THREAD_COUNT:?}\nelapsed: {:?}", now.elapsed());

        println!("text chunk len: {}", sizes.len());
        println!("text chunk size max: {}", sizes.iter().max().unwrap());
        println!("text chunk size mean: {}", sizes.iter().sum::<usize>() as f64 / sizes.len() as f64);

        Ok(())
    }
}

// #[pyclass]
// #[derive(Clone)]
// pub struct WikySourceIter {
//     zstd_buf: Vec<u8>,
//     indexes: [(u64, u64, Vec<Indexes>)],
//     index: usize,
// }

