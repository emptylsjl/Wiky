
use std::collections::{HashMap, HashSet};
use std::{fs, io, process, thread, time, vec};
use std::fmt::{Display, format, Formatter};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, SeekFrom};
use std::io::prelude::*;
use std::iter::once;
use std::ops::{Add, Deref, Shr, Sub};
use std::path::{Path, PathBuf};
use std::process::exit;
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
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::reader::Reader;
use pyo3::exceptions::PyRuntimeError;
use quick_xml::name::QName;
use quick_xml::utils::is_whitespace;
use quick_xml::Writer;
use quick_xml::de;
use serde::Deserialize;
use utils::*;
use zstd_safe::{CCtx, DCtx, create_cdict, CDict, DDict, WriteBuf};
use chrono::{DateTime, Utc, TimeZone};
use mysql::{Conn, OptsBuilder};
use regex::Regex;
use crate::misc::*;
use crate::setup::*;
use crate::constant::*;
use crate::wiky_source::*;


const TEST_STR: &str = "  <page>
    <title>AfghanistanGeography</title>
    <ns>0</ns>
    <id>14</id>
    <redirect title=\"Geography of Afghanistan\" />
    <revision>
      <id>233198</id>
      <timestamp>2001-01-21T23:00:32Z</timestamp>
      <contributor>
        <username>LinusTolke</username>
        <id>32609824</id>
      </contributor>
      <comment>*</comment>
      <origin>233198</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"1663\" sha1=\"gbgp40xyf1evutj2bcq1zc60bfyf68j\" location=\"tt:233198\" id=\"233198\" />
      <sha1>gbgp40xyf1evutj2bcq1zc60bfyf68j</sha1>
    </revision>
    <revision>
      <id>233199</id>
      <parentid>233198</parentid>
      <timestamp>2001-04-26T13:46:25Z</timestamp>
      <contributor>
        <username>Malcolm Farmer</username>
        <id>135</id>
      </contributor>
      <comment>*</comment>
      <origin>233199</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"37\" sha1=\"qco8zfsvwwxqosga8gb0s7epq1p89hq\" location=\"tt:233199\" id=\"233199\" />
      <sha1>qco8zfsvwwxqosga8gb0s7epq1p89hq</sha1>
    </revision>
    <revision>
      <id>15898949</id>
      <parentid>407008306</parentid>
      <timestamp>2002-02-25T15:43:11Z</timestamp>
      <contributor>
        <username>Conversion script</username>
        <id>1226483</id>
      </contributor>
      <minor/>
      <comment>Automated conversion</comment>
      <origin>15898949</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"40\" sha1=\"tbqxriha3tgljrcbi0bxsx66r3z9vsf\" location=\"tt:15898949\" id=\"15898949\" />
      <sha1>tbqxriha3tgljrcbi0bxsx66r3z9vsf</sha1>
    </revision>
    <revision>
      <id>74466619</id>
      <parentid>15898949</parentid>
      <timestamp>2006-09-08T04:15:36Z</timestamp>
      <contributor>
        <username>Rory096</username>
        <id>750223</id>
      </contributor>
      <comment>cat rd</comment>
      <origin>74466619</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"59\" sha1=\"0uwuuhiam59ufbu0uzt9lookwtx9f4r\" location=\"tt:74089562\" id=\"74089562\" />
      <sha1>0uwuuhiam59ufbu0uzt9lookwtx9f4r</sha1>
    </revision>
    <revision>
      <id>407008306</id>
      <parentid>74466619</parentid>
      <timestamp>2001-12-02T17:03:03Z</timestamp>
      <contributor>
        <username>Oskar Flordal</username>
        <id>273</id>
      </contributor>
      <minor/>
      <comment>dubble redirect</comment>
      <origin>407008306</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"38\" sha1=\"12ujjwzr2soe1ukl1kmnf4vyd99x11i\" location=\"tt:408568136\" id=\"408568136\" />
      <sha1>12ujjwzr2soe1ukl1kmnf4vyd99x11i</sha1>
    </revision>
    <revision>
      <id>407008307</id>
      <parentid>74466619</parentid>
      <timestamp>2011-01-10T03:56:19Z</timestamp>
      <contributor>
        <username>Graham87</username>
        <id>194203</id>
      </contributor>
      <minor/>
      <comment>1 revision from [[:nost:AfghanistanGeography]]: import old edit, see [[User:Graham87/Import]]</comment>
      <origin>407008307</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"59\" sha1=\"0uwuuhiam59ufbu0uzt9lookwtx9f4r\" location=\"tt:74089562\" id=\"74089562\" />
      <sha1>0uwuuhiam59ufbu0uzt9lookwtx9f4r</sha1>
    </revision>
    <revision>
      <id>783865160</id>
      <parentid>407008307</parentid>
      <timestamp>2017-06-05T04:18:23Z</timestamp>
      <contributor>
        <username>Tom.Reding</username>
        <id>9784415</id>
      </contributor>
      <minor/>
      <comment>+{{Redirect category shell}} using [[Project:AWB|AWB]]</comment>
      <origin>783865160</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"92\" sha1=\"39r4w8qg62iexlyskf0ga3tblagtl8x\" location=\"tt:793011212\" id=\"793011212\" />
      <sha1>39r4w8qg62iexlyskf0ga3tblagtl8x</sha1>
    </revision>
  </page>

  <page>
    <title>AfghanistanCommunications</title>
    <ns>0</ns>
    <id>18</id>
    <redirect title=\"Communications in Afghanistan\" />
    <revision>
      <id>215878</id>
      <parentid>233203</parentid>
      <timestamp>2002-02-25T15:43:11Z</timestamp>
      <contributor>
        <username>Conversion script</username>
        <id>1226483</id>
      </contributor>
      <minor/>
      <comment>Automated conversion</comment>
      <origin>215878</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"41\" sha1=\"awxndxm5j6si9wny2e1jewspu7ly6wi\" location=\"tt:215878\" id=\"215878\" />
      <sha1>awxndxm5j6si9wny2e1jewspu7ly6wi</sha1>
    </revision>
    <revision>
      <id>233203</id>
      <timestamp>2001-01-21T23:02:29Z</timestamp>
      <contributor>
        <username>LinusTolke</username>
        <id>32609824</id>
      </contributor>
      <comment>*</comment>
      <origin>233203</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"1159\" sha1=\"hssqyt4lp4fvkbkrpo3az4r813js99x\" location=\"tt:233203\" id=\"233203\" />
      <sha1>hssqyt4lp4fvkbkrpo3az4r813js99x</sha1>
    </revision>
    <revision>
      <id>15898952</id>
      <parentid>215878</parentid>
      <timestamp>2002-09-13T13:39:26Z</timestamp>
      <contributor>
        <username>Andre Engels</username>
        <id>300</id>
      </contributor>
      <minor/>
      <comment>indirect redirect</comment>
      <origin>15898952</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"43\" sha1=\"kmgvt2txxk7o430izy4xryol0byi93y\" location=\"tt:15898952\" id=\"15898952\" />
      <sha1>kmgvt2txxk7o430izy4xryol0byi93y</sha1>
    </revision>
    <revision>
      <id>74466499</id>
      <parentid>15898952</parentid>
      <timestamp>2006-09-08T04:14:42Z</timestamp>
      <contributor>
        <username>Rory096</username>
        <id>750223</id>
      </contributor>
      <comment>cat rd</comment>
      <origin>74466499</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"64\" sha1=\"2gt5g76thwz1sgiig4ju2qz3r2qvwko\" location=\"tt:74089443\" id=\"74089443\" />
      <sha1>2gt5g76thwz1sgiig4ju2qz3r2qvwko</sha1>
    </revision>
    <revision>
      <id>783865299</id>
      <parentid>74466499</parentid>
      <timestamp>2017-06-05T04:19:45Z</timestamp>
      <contributor>
        <username>Tom.Reding</username>
        <id>9784415</id>
      </contributor>
      <minor/>
      <comment>+{{Redirect category shell}} using [[Project:AWB|AWB]]</comment>
      <origin>783865299</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes=\"97\" sha1=\"q8gdi8070w6yitd4hqev68pn5niog3x\" location=\"tt:793011351\" id=\"793011351\" />
      <sha1>q8gdi8070w6yitd4hqev68pn5niog3x</sha1>
    </revision>
  </page> ";


const TEST_STR_2: &str = "";

pub fn setup_dump_compact<P: AsRef<Path>, Q: AsRef<Path>, O: AsRef<Path>, R: AsRef<Path>>(
    src_bz2: P, src_index: Q, dst_zstd: O, dst_index: R
) -> Result<()> {

    let mut wiki_bz2 = fs::File::open(src_bz2).context("can not open file")?;
    let wiki_index = fs::read(src_index).context("can not open file")?;
    let wiki_bz2_len = wiki_bz2.metadata().unwrap().len();
    println!("{wiki_bz2_len} - {}", memchr::Memchr::new(b'\n', &wiki_index).count());

    let (offsets, offset_map) = get_wiki_bz2_offsets(&wiki_index, wiki_bz2_len)?;

    let mut base_st = 550;
    let mb_100 = 104857600;

    let mut new_i = 0;
    let mut new_offsets = vec![vec![]];

    for &(st, ed) in &offsets {
        new_offsets[new_i].push((st, ed));
        if (ed - base_st) > mb_100 {
            base_st = ed;
            new_i += 1;
            new_offsets.push(vec![])
        }
    }

    let mut chunk_index = 0;
    let chunk_size = *THREAD_COUNT.get().unwrap_or(&4) * 10;
    let mut bz2_raw_buf = vec![0; mb_100 as usize * 2];
    // let mut remapped_index = vec![];

    let mut offset_st_remapped = 0;

    if dst_zstd.as_ref().exists() {
        fs::remove_file(&dst_zstd)?;
    }
    let mut zstd_fd = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&dst_zstd)?;

    for ranges in new_offsets {

        let (ck_st, ck_ed) = (ranges[0].0, ranges[ranges.len()-1].1);
        println!("--- {ck_st}:{ck_ed} - {}  - {:7.4}%", ranges.len(), (ck_ed as f64 / wiki_bz2_len as f64) * 100.0);

        wiki_bz2.seek(SeekFrom::Start(ck_st)).unwrap();
        wiki_bz2.read_exact(&mut bz2_raw_buf[..(ck_ed-ck_st) as usize]).unwrap();

        let text_vec = ranges.par_iter().flat_map(|(st_o, ed_o)| {
            let (st, ed) = ((st_o-ck_st) as usize, (ed_o-ck_st) as usize);

            let mut decompressed_buf = Vec::with_capacity(60_000_000);
            let status = decompress_bz2(&bz2_raw_buf[st..ed], &mut decompressed_buf)
                .unwrap_or_else(|err| panic!("{st} - {ed} - {err}"));

            if let Err(e) = validate_xml(std::str::from_utf8(&decompressed_buf).unwrap()) {
                println!("xml_err at: {} - {e}", st_o);
            };

            decompressed_buf
        }).collect::<Vec<_>>();
    }

    Ok(())
}

pub fn xml_page_tags<R: BufRead>(reader: R) -> Result<Vec<String>> {

    let mut reader = Reader::from_reader(reader);
    // let mut reader = Reader::from(&dst[..dst_size]);
    // reader.config_mut().trim_text(true);

    let mut pages = vec![vec![]; 1];
    let mut idx = 0;
    let mut depth = 1;
    let mut xml_buf = vec![];

    loop {
        xml_buf.clear();
        match reader.read_event_into(&mut xml_buf) {
            Ok(Event::Start(e)) => {
                if e.name() == QName(b"page") {
                    depth += 1;
                }
                pages[idx].extend_from_slice(b"<");
                // pages[idx].extend_from_slice(e.name().0.as_slice());
                // e.attributes().for_each(|s| {
                //     pages[idx].push(b' ');
                //     pages[idx].extend_from_slice(s.as_ref().unwrap().key.into_inner());
                // });
                pages[idx].extend_from_slice(e.as_slice());
                pages[idx].extend_from_slice(b">");
            }
            Ok(Event::End(e)) => {
                pages[idx].extend_from_slice(b"</");
                pages[idx].extend_from_slice(e.name().0.as_slice());
                pages[idx].extend_from_slice(b">");
                if e.name() == QName(b"page") {
                    fs::write(format!("C:/a/enwiki/debug/meta_{idx}.xml"), &pages[idx]).unwrap();
                    input("").expect("uhm");
                    idx += 1;
                    depth -= 1;
                    pages.push(vec![]);
                }
            }
            Ok(Event::Eof) => break,
            Ok(Event::Empty(e)) => {
                pages[idx].extend_from_slice(b"<");
                // pages[idx].extend_from_slice(e.name().0.as_slice());
                // e.attributes().for_each(|s| {
                //     pages[idx].push(b' ');
                //     pages[idx].extend_from_slice(s.as_ref().unwrap().key.into_inner());
                // });
                pages[idx].extend_from_slice(e.as_slice());
                pages[idx].extend_from_slice(b"/>");
            }
            Ok(Event::Text(e)) => { pages[idx].extend_from_slice(e.as_slice()); }
            // Ok(Event::Text(e)) => { if !in_tag { pages[pages_index].extend_from_slice(e.as_slice());} }
            // Ok(Event::Text(e)) => { if e.iter().take(depth*10).copied().all(is_whitespace) { pages[idx].extend_from_slice(e.as_slice());} }
            // Ok(Event::CData(e)) => {}
            // Ok(Event::Comment(e)) => {}
            // Ok(Event::Decl(e)) => {}
            // Ok(Event::PI(e)) => {}
            // Ok(Event::DocType(e)) => {}

            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => {}
        }
    };
    let pages2 = pages.into_iter().map(String::from_utf8).collect::<std::result::Result<Vec<String>, _>>();
    // let uha = pages2.clone().unwrap();
    // let c = 0;
    // let d = 0;
    pages2.context("collect page failed")
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

pub fn title_dict_train(index_path: &str) -> Vec<u8> {

    let wiki_index = fs::File::open(index_path).expect("can not open index");

    let mut titles = Vec::with_capacity(600_000_000);
    let mut nb_sample = Vec::with_capacity(24_000_000);

    BufReader::new(wiki_index).split(b'\n')
        .flatten()
        .for_each(|line_text| {
            let line = line_text.splitn(3, |x| x == &b':').last().unwrap();
            nb_sample.push(line.len());
            titles.extend_from_slice(line);
        });

    train_zstd_dict(&titles, &nb_sample).unwrap_or_else(|e| panic!("zstd train fail: {e}"))
}

pub fn collect_tag_mapping(dst_index: &str, dst_zstd: &str) {

    let ws = WikySource::from_path(
        dst_index,
        dst_zstd
    ).unwrap();

    let result = ws.chunk_map(*THREAD_COUNT.get().unwrap_or(&4) * 10, |ck_st, zstd_buf| {

        // println!("{}", zstd_buf.len());
        zstd_buf.iter().map(|(st, buf, v)| {

            let mut dst = vec![0; 200_000_000];
            let dst_size = zstd_safe::decompress(&mut dst, buf)
                .unwrap_or_else(|err| panic!("st:{ck_st} - zstd_err:{err}"));
            // let text = std::str::from_utf8(&dst[..dst_size]).unwrap();
            let a = xml_page_tags(&dst[..dst_size]).unwrap();
            let  c = 0;
            a

        }).flatten().collect_vec()

    }).flatten().collect::<HashSet<_>>();

    for i in result.iter().sorted() {
        println!("{i}");;
    }
}

pub fn get_category_list() {

}

pub fn init() -> Result<()> {

    // {
    //     let nowiki_re = Regex::new(r"<nowiki>.*?</nowiki>").unwrap();
    //     // let nowiki_re = Regex::new(r"<nowiki>(.*?)</nowiki>").unwrap();
    //     let category_re = Regex::new(r"\[\[Category:([^|\]]+)\]\]").unwrap();
    //
    //     let mut raw = fs::read("src/test.mediawiki")?;
    //     let mut txt = String::from_utf8(raw)?;
    //
    //     unsafe {
    //         let text_buf = txt.as_bytes_mut();
    //         let ranges = nowiki_re
    //             .find_iter(std::str::from_utf8_unchecked(text_buf))
    //             .map(|m| (m.start(), m.end()))
    //             .collect_vec();
    //
    //         ranges.into_iter()
    //             .for_each(|(s, e)| {
    //                 for i in s..e {
    //                     text_buf[i] = 0;
    //                 }
    //             });
    //     }
    //     let a = category_re.captures_iter(&txt)
    //         .map(|c| c.get(1).map(|m| m.as_str()))
    //         .flatten()
    //         .collect::<HashSet<_>>();
    // }

    set_thread(22);

    println!("23");

    let src_bz2_simple = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream-simple.xml.bz2";
    let src_index_simple = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream-index-simple.txt";
    let src_bz2 = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream.xml.bz2";
    let src_index = "C:/a/enwiki/dump/enwiki-20240601-pages-articles-multistream-index.txt";

    let dst_zstd_simple = "C:/a/enwiki/enwiki-20240601-pages-simple.xml.zstd";
    let dst_index_simple = "C:/a/enwiki/enwiki-20240601-index-remapped-simple.txt";
    let dst_zstd = "C:/a/enwiki/enwiki-20240601-pages.xml.zstd";
    let dst_index = "C:/a/enwiki/enwiki-20240601-index-remapped.txt";

    let dst_title_dict = "C:/a/enwiki/enwiki-20240601-index-zstd-dict.cdict";

    let dst_category_list = "C:/a/enwiki/enwiki-20240601-category-list.txt";

    let dst_category_index = "C:/a/enwiki/enwiki-20240601-category-index.txt";

    let wss = WikySource::from_path(
        dst_index_simple,
        dst_zstd_simple
    ).unwrap();

    let opts = OptsBuilder::new()
        .user(Some("root"))     // change username maybe
        .db_name(Some("wiky_base"));
    let mut conn = Conn::new(opts)?;

    // let ws = WikySource::from_path(
    //     dst_index,
    //     dst_zstd
    // ).unwrap();

    // {
    //     let result = ws.category_list();
    //     fs::write(dst_category_list, result.iter().join("\n")).unwrap();
    // }

    // ws.save_category_index(dst_category_index).unwrap();


    // let page_index = fs::read(dst_category_index)?;
    let page_index = String::from_utf8(fs::read(dst_category_index)?)?.to_lowercase();
    let cate_links = load_category(&page_index);

    insert_zstd_range(&mut conn, &wss).unwrap();
    insert_wiky_index(&mut conn, &wss).unwrap();
    insert_wiky_category(&mut conn, &cate_links).unwrap();

    // let cate_map = cate_links.iter().enumerate().map(|(i, (_, title, _))| (*title, i)).collect::<HashMap<_, _>>();
    // let cate_links2 = cate_links.iter().map(|(id, title, cates)| {
    //     (
    //         *cate_map.get(title).unwrap(),
    //         cates.iter().map(|s| cate_map.get(s)).flatten().copied().collect_vec()
    //     )
    // }).collect_vec();

    exit(0);
    let page_links = load_page(&page_index);


    let mut seen = HashSet::new();

    // let canada_id = cate_map.get("canada").unwrap();
    seen.insert("canada");
    let cad_cates0 = cate_links.iter().filter(|c| c.cates.contains(&"canada")).collect_vec();
    seen.extend(cad_cates0.iter().map(|x| x.title));
    let mut cates_level = vec![cad_cates0];

    for i in 0..5  {
        let cates = {
            let last_cates = cates_level.last().unwrap();
            cate_links.par_iter()
                .filter(|c| !seen.contains(&c.title) && c.cates
                    .iter()
                    .any(|x| last_cates.iter().any(|lc| &lc.title == x))
                )
                .collect::<Vec<_>>()
        };
        seen.extend(cates.iter().map(|x| x.title));
        cates_level.push(cates);
    }

    let a = cates_level.iter().map(|c| { println!("{}", c.len()); c.len() }).sum::<usize>();
    println!("{a}\n");


    let mx = cate_links.iter().map(|c| c.title.len()).max();
    println!("{mx:?}");

    // let sub_page_links = page_links.iter().filter(|(t, v)| v.iter().any(|x| seen.contains(x))).collect_vec();



    // let cad_cates2 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates1.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates2);
    // let cad_cates3 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates2.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates3);
    // let cad_cates4 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates3.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates4);
    // let cad_cates5 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates4.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates5);
    // let cad_cates6 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates5.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates6);
    // let cad_cates7 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates6.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates7);
    // let cad_cates8 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates7.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates8);
    // let cad_cates9 = cate_links.iter().filter(|c| !seen.contains(c.title) && c.cates.iter().any(|x| cad_cates8.contains(x))).map(|x| x.title).collect_vec();
    // seen.extend(&cad_cates9);
    // // for i in cates
    //
    // let len0 = cad_cates0.len();
    // let len1 = cad_cates1.len();
    // let len2 = cad_cates2.len();
    // let len3 = cad_cates3.len();
    // let len4 = cad_cates4.len();
    // let len5 = cad_cates5.len();
    // let len6 = cad_cates6.len();
    // let len7 = cad_cates7.len();
    // let len8 = cad_cates8.len();
    // let len9 = cad_cates9.len();


    // let mut out_text = Vec::with_capacity(500_000_000);

    // xml_page_tags(buf_reader).unwrap();

    // let dict = title_dict_train(src_index);
    // fs::write(dst_title_dict, dict).expect("write dict failed");

    // let t = b"List of minor planets: 699001-700000".as_slice();
    // println!("{}", t.len());
    //
    // let d = fs::read(dst_title_dict).unwrap();
    // let cd = zstd_safe::CDict::create(&d, 9);
    // let dd = zstd_safe::DDict::create(&d);
    //
    // let e = compress_cdict(t, &cd).unwrap();
    // println!("{}", e.len());
    // let r = decompress_ddict(&e, &dd).unwrap();
    // println!("{}", r.len());
    // println!("{}", std::str::from_utf8(t).unwrap());

    // site_info(
    //     src_bz2,
    //     "C:/a/enwiki/enwiki-20240601-site-info.txt",
    //     550
    // )?;
    //
    // setup_dump_chunk(
    //     src_bz2,
    //     src_index,
    //     dst_zstd,
    //     dst_index,
    // )?;

    // ws.validate_index_dump().unwrap();

    // bench_bz2(src_bz2, src_index).unwrap();
    // ws.bench_zstd().unwrap();

    // let opts = OptsBuilder::new()
    //     .user(Some("root"))     // change username maybe
    //     .db_name(Some("wiky_base"));
    // let mut conn = Conn::new(opts)?;
    //
    // insert_zstd_range(&mut conn, &ws).unwrap();
    // insert_wiky_index(&mut conn, &ws).unwrap();


    Ok(())
}