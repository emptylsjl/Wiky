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

/// Formats the sum of two numbers as string.
#[pyfunction]
pub fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn double(x: usize) -> usize {
    x * 2
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
#[pyo3(name = "wiky")]
mod pyo3_wiki {
    use super::*;

    #[pymodule_export]
    use sum_as_string;

    #[pymodule_export]
    use double;

    #[pyfunction] // This will be part of the module
    fn triple(x: usize) -> usize {
        x * 3
    }

    #[pyclass]
    struct Unit;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<WikySource>()?;
        Ok(())
    }
}