#![feature(core_panic)]
#![feature(type_ascription)]
#![feature(test)]
#![feature(iter_collect_into)]
#![feature(slice_as_chunks)]
#![feature(array_chunks)]
#![feature(result_option_inspect)]
#![feature(windows_by_handle)]
#![feature(const_trait_impl)]

#![allow(dead_code)]
#![allow(unused)]


extern crate test;
extern crate core;

use core::panicking::panic;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, BufRead, Write};
use std::mem::MaybeUninit;
use std::{fs, io, mem, ptr, string, thread, time};
use std::collections::HashSet;
use std::fmt::Debug;
use std::time::{Duration, Instant, SystemTime};
use std::process::Command;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use aes_gcm::{AeadInPlace, Aes256Gcm, Key, KeyInit, Nonce};
use glam::*;
use num_traits::{clamp, Float};
use anyhow::{anyhow, Result};
use rand::Rng;
use itertools::Itertools;
use once_cell::sync::Lazy;
use rand::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use criterion::black_box;
use windows::*;
use windows::Win32::Foundation::*;
use windows::Win32::System::LibraryLoader::*;
use windows::Win32::UI::WindowsAndMessaging::*;
use windows::Win32::UI::Input::KeyboardAndMouse::*;


fn main() -> Result<()> {

    ThreadPoolBuilder::new().num_threads(16).build_global().unwrap();



    Ok(())

}