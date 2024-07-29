
use std::collections::{HashMap, HashSet};
use std::{fs, io, time, vec};
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
use memchr;
use nohash_hasher::BuildNoHashHasher;
use quickxml_to_serde::{Config, xml_string_to_json};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use pyo3::prelude::*;
use quick_xml::de;
use quick_xml::utils::is_whitespace;
use regex::Regex;
use serde::Deserialize;
use utils::*;

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

#[cfg(feature = "index_u64")]
fn to_str(i: &Indexes) -> String {
    i.to_string()
}
#[cfg(not(feature = "index_u64"))]
fn to_str(i: &Indexes) -> String {
    i.0.to_string() + ", " + &i.1
}

pub fn decode_chunk(zstd_path: &str, chunk_st: usize, chunk_ed: usize) -> PyResult<String> {
    let mut zstd_buf = vec![0; chunk_ed-chunk_st];
    let mut wiki_zstd =
        fs::OpenOptions::new()
            .read(true)
            .open(zstd_path)?;

    wiki_zstd.seek(SeekFrom::Start(chunk_st as u64)).unwrap();
    wiki_zstd.read_exact(&mut zstd_buf[..(chunk_ed-chunk_st)]).unwrap();

    let mut dst = vec![0; 60_200_000];
    let len = zstd_safe::decompress(&mut dst, &zstd_buf)
        .map_err(|e| py_err(e.to_string()))?;
    dst.truncate(len);
    String::from_utf8(dst).map_err(|e| py_err(e.to_string()))
}


#[derive(Debug, Deserialize)]
pub struct PageMeta {
    #[serde(rename = "id")]
    pub page_id: u64,
    pub redirect: Option<Redirect>,
    #[serde(rename = "revision")]
    pub revisions: Vec<Revision<XmlSize>>,
}

#[derive(Debug, Deserialize)]
pub struct PageText {
    #[serde(rename = "id")]
    pub page_id: String,
    pub title: String,
    pub redirect: Option<Redirect>,
    #[serde(rename = "revision")]
    pub revisions: Revision<XmlText>,
}

#[derive(Debug, Deserialize)]
pub struct Redirect {
    #[serde(rename = "@title")]
    pub title: String,
}

#[derive(Debug, Deserialize)]
pub struct Revision<T> {
    pub id: u64,
    pub timestamp: String,
    // contributor: Contributor,
    pub text: T,
}

#[derive(Debug, Deserialize)]
pub struct Contributor {
    pub ip: Option<String>,
    pub id: Option<String>,
    #[serde(rename = "username")]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct XmlSize {
    #[serde(rename = "@bytes")]
    pub bytes: u32,
}

#[derive(Debug, Deserialize)]
pub struct XmlText {
    #[serde(rename = "$value")]
    pub text: Option<String>,
}

impl Display for Revision<XmlSize> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "{}-{}",
            DateTime::parse_from_rfc3339(&self.timestamp).unwrap_or_else(|e| panic!("{e}-{}", &self.timestamp)).timestamp(),
            self.text.bytes
        )
    }
}

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

    pub fn chunk_map<'a, T, F: FnMut(u64, Vec<(u64, &[u8], &Vec<Indexes>)>) -> T + 'a>(
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

            let zstd_bufs = ranges.iter().map(|(st_o, ed_o, v)| {
                let (st, ed) = ((st_o - chunk_st) as usize, (ed_o - chunk_st) as usize);
                (*st_o, &zstd_buf[st..ed], v)
            }).collect_vec();

            runner(chunk_st, zstd_bufs)
        })
    }

    pub fn category_index<W: Write>(&self, buf: &mut W) {
        self.chunk_map(*oc_get(&THREAD_COUNT), |ck_st, zstd_buf| {
            println!("{ck_st}");

            let iters = zstd_buf.into_par_iter().map(|(st, buf, v)| {

                let mut dst = vec![0; 200_000_000];
                let dst_size = zstd_safe::decompress(&mut dst, buf)
                    .unwrap_or_else(|err| panic!("st:{ck_st} - zstd_err:{err}"));
                let pages = de::from_reader::<_, Vec<PageText>>(&dst[..dst_size])
                    .unwrap_or_else(|err| panic!("st:{ck_st} - xml_de_err:{err}"));

                let cmt_re = Regex::new(r"<!--.*?-->").unwrap();
                let pre_re = Regex::new(r"(?s)<pre>.*?</pre>").unwrap();
                let ref_re = Regex::new(r"(?s)<ref>.*?</ref>").unwrap();
                let code_re = Regex::new(r"(?s)<code>.*?</code>").unwrap();
                let nowiki_re = Regex::new(r"(?s)<nowiki>.*?</nowiki>").unwrap();
                let noinclude_re = Regex::new(r"(?s)<noinclude>.*?</noinclude>").unwrap();
                let includeonly_re = Regex::new(r"(?s)<includeonly>.*?</includeonly>").unwrap();

                let space_re = Regex::new(r"  +").unwrap();

                let category_re = Regex::new(r"\[\[Category:([^|\]]*)").unwrap();

                let parsed = pages.into_iter()
                    .filter_map(move |mut p| if let (None, Some(mut xml_text)) = (p.redirect, p.revisions.text.text) {
                        if p.title.starts_with("Module:") || p.title.starts_with("Template:") {
                            None
                        } else {
                            unsafe {
                                let text_buf = xml_text.as_bytes_mut();
                                let ranges = nowiki_re
                                    .find_iter(std::str::from_utf8_unchecked(text_buf))
                                    .chain(cmt_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    // .chain(pre_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(ref_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(code_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(noinclude_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(includeonly_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .map(|m| (m.start(), m.end()))
                                    .collect_vec();

                                ranges.into_iter()
                                    .for_each(|(s, e)| {
                                        for i in s..e {
                                            text_buf[i] = 0x20;
                                        }
                                    });
                            }

                            let categories = category_re.captures_iter(&xml_text)
                                .map(|c| c.get(1).map(|m| trim_s(m.as_str())))
                                .flatten()
                                .filter(|s| memchr::memchr(b'\n', s.as_bytes()).is_none())
                                .map(|s| space_re.replace_all(s, " "))
                                .map(|s| s.to_string())
                                .join("|");

                            Some(p.page_id + "|" + &p.title + "|" + &categories)
                        }
                    } else {
                        None
                    });
                parsed
            }).collect::<Vec<_>>();

            iters.into_iter().flatten().for_each(|s| {
                buf.write_all((s+"\n").as_bytes()).unwrap()
            })
        }).count();
    }
}


#[pymethods]
impl WikySource {
    #[new]
    pub fn new(index_path: &str, zstd_path: &str) -> PyResult<Self> {
        Self::from_path(index_path, zstd_path)
    }

    pub fn zstd_path_str(&self) -> &str {
        self.zstd_path.as_os_str().to_str().unwrap()
    }

    pub fn decode_page_json(&self, chunk_st: usize, chunk_ed: usize, page_id: u64) -> PyResult<String> {
        let chunk_text = decode_chunk(self.zstd_path_str(), chunk_st, chunk_ed)?;

        let conf = Config::new_with_defaults();
        let json = xml_string_to_json(format!("<root>{chunk_text}</root>"), &conf)
            .map_err(|e| py_err("malformed ".to_owned()+&e.to_string()))?;
        // let a = json[""];
        Ok(json.to_string())
    }

    pub fn validate_index_dump(&self) -> PyResult<()> {

        let result = self.chunk_map((*THREAD_COUNT.get().unwrap_or(&4)) * 20, |chunk_st, zstd_bufs| {

            let valid = zstd_bufs.par_iter().all(|(st, buf, v)| {

                let mut dst = vec![0; 200_000_000];
                let dst_size = zstd_safe::decompress(&mut dst, buf)
                    .unwrap_or_else(|err| panic!("st:{chunk_st} - zstd_err:{err}"));
                let text = std::str::from_utf8(&dst[..dst_size]).unwrap();

                #[cfg(feature = "index_u64")]
                let valid = v.iter().all(|id| {
                    text.contains(&("<id>".to_owned()+&id.to_string()+"</id>"))
                });
                #[cfg(not(feature = "index_u64"))]
                let mut valid = v.iter().all(|(id, title)| {
                    let title = if title.contains("") { "" } else { title.trim() };
                    text.contains(&("<id>".to_owned()+&id.to_string()+"</id>"))
                        && text.contains(&("<title>".to_owned()+title+"</title>"))
                });
                if !valid {
                    println!("validation error: st:{chunk_st}, text missmatch");
                }
                if let Err(e) = validate_xml(text) {
                    println!("validation error: st:{chunk_st}, xml {e}");
                    valid = false;
                }
                if !valid {
                    fs::write(format!("C:/a/enwiki/debug/pages{chunk_st}"), text).unwrap();
                    fs::write(format!("C:/a/enwiki/debug/index{chunk_st}"), v.iter().map(to_str).join("\n")).unwrap();
                }
                valid
            });
            println!("- validate {valid:5} - st:{chunk_st}  - {:7.4}%", (chunk_st as f64 / self.zstd_len as f64) * 100.0);
            valid
        }).collect_vec();
        println!("validate result: {}", result.iter().all(|x| *x));

        Ok(())
    }

    pub fn bench_zstd(&self) -> PyResult<()> {

        println!("zstd chunk len: {}", self.indexes.len());
        println!("zstd chunk size max: {}", self.indexes.iter().map(|(a, b, c)| b - a).max().unwrap());
        println!("zstd chunk size mean: {}", self.indexes.iter().map(|(a, b, c)| b - a).sum::<u64>() as f64 / self.indexes.len() as f64);

        let now = time::Instant::now();

        let sizes = self.chunk_map(
            (*THREAD_COUNT.get().unwrap_or(&4)) * 20,
            |chunk_st, zstd_bufs| {
                println!("--- st:{chunk_st} - {:7.4}%", (chunk_st as f64 / self.zstd_len as f64) * 100.0);

                zstd_bufs.par_iter().map(|(st, buf, v)| {
                    let mut dst = vec![0; 60_200_000];
                    let dst_size = zstd_safe::decompress(&mut dst, buf)
                        .unwrap_or_else(|err| panic!("st:{chunk_st} - zstd_err:{err}"));
                    dst_size
                }).collect::<Vec<_>>()

            }).flatten().collect_vec();

        println!("t:{THREAD_COUNT:?}\nelapsed: {:?}", now.elapsed());

        println!("text chunk len: {}", sizes.len());
        println!("text chunk size max: {}", sizes.iter().max().unwrap());
        println!("text chunk size mean: {}", sizes.iter().sum::<usize>() as f64 / sizes.len() as f64);

        Ok(())
    }

    /// now i am thinking, is this really needed ???
    pub fn category_list(&self) -> HashSet<String> {
        self.chunk_map(*oc_get(&THREAD_COUNT), |ck_st, zstd_buf| {

            println!("{ck_st}");
            let iters = zstd_buf.into_par_iter().map(|(st, buf, v)| {

                let mut dst = vec![0; 200_000_000];
                let dst_size = zstd_safe::decompress(&mut dst, buf)
                    .unwrap_or_else(|err| panic!("st:{ck_st} - zstd_err:{err}"));
                let pages = de::from_reader::<_, Vec<PageText>>(&dst[..dst_size])
                    .unwrap_or_else(|err| panic!("st:{ck_st} - xml_de_err:{err}"));

                let cmt_re = Regex::new(r"<!--.*?-->").unwrap();
                let pre_re = Regex::new(r"(?s)<pre>.*?</pre>").unwrap();
                let ref_re = Regex::new(r"(?s)<ref>.*?</ref>").unwrap();
                let code_re = Regex::new(r"(?s)<code>.*?</code>").unwrap();
                let nowiki_re = Regex::new(r"(?s)<nowiki>.*?</nowiki>").unwrap();
                let noinclude_re = Regex::new(r"(?s)<noinclude>.*?</noinclude>").unwrap();
                let includeonly_re = Regex::new(r"(?s)<includeonly>.*?</includeonly>").unwrap();
                // // let nowiki_re = Regex::new(r"<nowiki>(.*?)</nowiki>").unwrap();
                // // let category_re = Regex::new(r"\[\[Category:([^|\]]+)\]\]").unwrap();

                // let nowiki_re = Regex::new(
                //     r"<!--.*?-->|<pre>.*?<\/pre>|<nowiki>.*?<\/nowiki>|<noinclude>.*?<\/noinclude>|<includeonly>.*?<\/includeonly>|"
                // ).unwrap();

                let space_re = Regex::new(r"  +").unwrap();

                let category_re = Regex::new(r"\[\[Category:([^|\]]*)").unwrap();

                pages.into_iter()
                    .filter_map(move |mut p| if let (None, Some(mut xml_text)) = (p.redirect, p.revisions.text.text) {
                        if p.title.starts_with("Module:") || p.title.starts_with("Template:") {
                            None
                        } else {
                            unsafe {
                                let text_buf = xml_text.as_bytes_mut();
                                let ranges = nowiki_re
                                    .find_iter(std::str::from_utf8_unchecked(text_buf))
                                    .chain(cmt_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    // .chain(pre_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(ref_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(code_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(noinclude_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .chain(includeonly_re.find_iter(std::str::from_utf8_unchecked(text_buf)))
                                    .map(|m| (m.start(), m.end()))
                                    .collect_vec();

                                ranges.into_iter()
                                    .for_each(|(s, e)| {
                                        for i in s..e {
                                            text_buf[i] = 0x20;
                                        }
                                    });
                            }
                            let stage = category_re.captures_iter(&xml_text)
                                .map(|c| c.get(1).map(|m| trim_s(m.as_str())))
                                .flatten()
                                .filter(|s| memchr::memchr(b'\n', s.as_bytes()).is_none())
                                .map(|s| space_re.replace_all(s, " "))
                                .map(|s| s.to_string())
                                .collect_vec();
                            Some(stage)
                        }
                    } else {
                        None
                    })
                    .flatten()

            }).collect::<Vec<_>>();

            iters.into_iter().flatten().collect::<HashSet<_>>()

        }).fold(HashSet::new(), |mut a, b| {
            a.extend(b);
            a
        })
    }

    pub fn save_category_index(&self, dst_path: &str) -> PyResult<()> {
        let fd = fs::File::create(dst_path)?;
        let mut writer = BufWriter::new(fd);
        self.category_index(&mut writer);
        Ok(())
    }

}

//Parishes in Skye