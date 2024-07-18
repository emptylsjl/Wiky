


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
use anyhow::{anyhow, Context, Result};
use itertools::Itertools;
use nohash_hasher::BuildNoHashHasher;
use pyo3::prelude::*;
use pyo3::PyErrArguments;
use quickxml_to_serde::Config;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use utils::*;
use zstd_safe::{CCtx, CDict, DCtx, DDict};
use crate::setup::*;
use crate::constant::*;
use crate::wiky_source::*;

fn create_dict(sample: &[u8], nb_sample: &[usize]) -> Result<Vec<u8>> {
    let mut dict = vec![0u8; 1_240_000];
    let size = zstd_safe::train_from_buffer(&mut dict, sample, nb_sample)
        .map_err(|e| anyhow!("zstd_err:{e}"))?;
    dict.truncate(size);
    Ok(dict)
}
fn compress_string(s: &str, dict: &[u8]) -> Result<Vec<u8>> {
    let mut cctx = CCtx::create();
    let cdict = CDict::create(dict, 9); // Compression level 3
    cctx.ref_cdict(&cdict).expect("ref failed");

    let mut compressed = vec![0u8; zstd_safe::compress_bound(s.len())];
    let compressed_size = cctx.compress2(&mut compressed, s.as_bytes())
        .map_err(|e| anyhow!("zstd_err:{e}"))?;
    compressed.truncate(compressed_size);
    Ok(compressed)
}

fn decompress_string(compressed: &[u8], dict: &[u8]) -> Result<Vec<u8>> {
    let mut dctx = DCtx::create();
    let ddict = DDict::create(dict);
    let a = dctx.ref_ddict(&ddict).expect("uh");

    let mut decompressed = vec![0u8; 60_000_000];
    let decompressed_size = dctx.decompress(&mut decompressed, compressed)
        .map_err(|e| anyhow!("zstd_err:{e}"))?;
    decompressed.truncate(decompressed_size);
    Ok(decompressed)
}

pub fn zstd_comp_bound(src: usize) -> usize {
    (src) + ((src)>>8) + (if (src) < (128<<10) { ((128<<10) - (src)) >> 11 } else {0})
}

pub fn decompress_bz2(src_buf: &[u8], dst_buf: &mut Vec<u8>) -> Result<bzip2::Status> {
    let mut decompresser = Decompress::new(false);
    decompresser
        .decompress_vec(src_buf, dst_buf)
        .context("decompress chunk failed")
}

pub fn validate_xml(text: &str) -> Result<()> {
    let xml_contents = format!("<root>{text}</root>");
    let mut reader = Reader::from_str(&xml_contents);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Err(e) => { return Err(e.into()) },
            Ok(Event::Eof) => break,
            _ => {}
        }
    }
    Ok(())
}

pub fn py_err<S: PyErrArguments + Send + Sync + 'static>(s: S) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(s)
}

pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for anyhow::Result<T> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(|e| {
            py_err(e.to_string())
        })
    }
}
