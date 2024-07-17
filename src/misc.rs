


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

use utils::*;

use crate::setup::*;
use crate::constant::*;
use crate::wiky_source::*;

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

pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for anyhow::Result<T> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(|e| {
            let mut error_message = e.to_string();

            // if let Some(backtrace) = e.backtrace() {
            //     writeln!(error_message, " - Backtrace:\n{}", backtrace).unwrap();
            // }
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(error_message)
        })
    }
}
