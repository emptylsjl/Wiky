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
use nohash_hasher::BuildNoHashHasher;
use quickxml_to_serde::Config;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use pyo3::prelude::*;

use utils::*;

use misc::*;
use setup::*;
use constant::*;
use wiky_source::*;

#[pyfunction]
pub fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pymodule]
#[pyo3(name = "wiky")]
mod pyo3_wiki {
    use super::*;

    #[pymodule_export]
    use super::setup::set_thread;

    #[pyfunction]
    fn site_info(src_bz2: &str, src_index: &str, offset: usize) -> PyResult<()> {
        setup::site_info(src_bz2, src_index, offset).into_py_result()
    }

    #[pyfunction]
    fn setup_dump(src_bz2: &str, src_index: &str, dst_zstd: &str, dst_index: &str) -> PyResult<()> {
        setup::setup_dump(src_bz2, src_index, dst_zstd, dst_index).into_py_result()
    }

    #[pyfunction]
    fn bench_bz2(src_bz2: &str, src_index: &str) -> PyResult<()> {
        setup::bench_bz2(src_bz2, src_index).into_py_result()
    }

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<WikySource>()?;
        Ok(())
    }
}