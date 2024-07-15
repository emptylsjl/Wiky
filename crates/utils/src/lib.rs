// #![feature(core_panic)]
// #![feature(type_ascription)]
// #![feature(test)]
// #![feature(iter_collect_into)]
// #![feature(slice_as_chunks)]
// #![feature(array_chunks)]
// #![feature(result_option_inspect)]
// #![feature(windows_by_handle)]
// #![feature(const_trait_impl)]

#![allow(dead_code)]
#![allow(unused)]


mod defines;
mod macros;

pub use defines::*;
pub use macros::*;

// extern crate test;
extern crate core;

// use core::panicking::panic;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, BufRead, Write};
use std::mem::MaybeUninit;
use std::{fmt, fs, io, mem, ptr, slice, string, thread, time};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::{Debug, Display};
use std::time::{Duration, Instant, SystemTime};
use std::process::Command;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use aes_gcm::{AeadInPlace, Aes256Gcm, Key, KeyInit, Nonce};
use glam::*;
use num_traits::{clamp, Float};
use anyhow::{anyhow, Context, Result};
use rand::Rng;
use itertools::Itertools;
use once_cell::sync::Lazy;
use rand::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use criterion::black_box;
use hex::{ToHex, FromHex};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use windows::*;
use windows::Win32::Foundation::*;
use windows::Win32::System::LibraryLoader::*;
use windows::Win32::UI::WindowsAndMessaging::*;
use windows::Win32::UI::Input::KeyboardAndMouse::*;
use winreg::{RegKey, RegValue};
pub type DynResult<T> = Result<T, Box<dyn std::error::Error>>;

pub fn rand_from<Q: Copy, W: FromIterator<Q>>(li: &[Q], len: usize) -> W {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| li[rng.gen_range(0..li.len())])
        .collect()
}

pub fn gen_pwd(len: usize) -> String {
    let li = (
        "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM".to_owned() +
            "!@#$%&*" +
            "`~^_-+=|\\:;,./?" +
            "()<>{}[]'\""
    ).chars().collect_vec();

    rand_from(&li, len)
}

/// gen_ascii_allowed(size, "0aA!;")
pub fn gen_ascii_allowed<S: Into<String>>(len: usize, allow: Option<S>) -> String {

    let allow = allow.map(|s| s.into()).unwrap_or("0aA!;".into());
    let mut s = "".to_string();
    if allow.contains('0') { s += "1234567890" }
    if allow.contains('a') { s += "abcdefghijklmnopqrstuvwxyz" }
    if allow.contains('A') { s += "ABCDEFGHIJKLMNOPQRSTUVWXYZ" }
    if allow.contains('!') { s += "!@#$%&*" }
    if allow.contains(';') { s += ";:,.~`^_-+=|\\/?()<>{}[]'\"" }
    if s.is_empty() { return "".to_string() }

    let li = s.chars().collect_vec();
    rand_from(&li, len)
}

pub fn to_sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new_with_prefix(data);
    let hash = hasher.finalize();
    hash.encode_hex::<String>()
}

pub fn to_sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new_with_prefix(data);
    let hash = hasher.finalize();
    hash.to_vec()
}

pub fn walk<P: AsRef<Path>>(dir: P) -> impl Iterator<Item = PathBuf> {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .map(|entry| entry.unwrap().into_path())
}

pub fn fs_wait_read<P: AsRef<Path> + fmt::Debug>(path: &P, time_out: f64) -> DynResult<File> {
    match fs::File::open(path) {
        Ok(f) => Ok(f),
        Err(e) => {
            print!("{path:?} - {e}");
            let st = time::Instant::now();
            loop {
                thread::sleep(Duration::from_millis(100));
                if let Ok(fd) = fs::File::open(path) { return Ok(fd)};
                if st.elapsed().as_secs_f64() > time_out { return Err("wait file time out".into()) }
            }
        }
    }
}

pub fn rm_empty_dir<P: AsRef<Path>>(dir: &P) -> DynResult<[u32; 2]> {
    if dir.as_ref().is_dir() {
        let mut a = [0; 2];
        walk(dir).for_each(|p| {
            if p.is_dir() && p.read_dir().map(|mut d| d.next().is_none()).unwrap_or(true) {
                a[fs::remove_dir(p).is_ok() as usize] += 1
            }
        });
        Ok(a)
    } else {
        Err("Not dir".into())
    }
}


/// util::print_mem_as(
///     struct.as_ptr() as *const f32,
///     4*4*3,
///     1, 8, " ",
///     |x| print!("{x:8.02}")
/// );
pub unsafe fn print_mem_as<T, F: Fn(&T), D: Display>(ptr: *const T, length: usize, spl: usize, ln: usize, splt: D, ptf: F) {
    let bytes_slice = slice::from_raw_parts(ptr, length);
    for i in 0..length {
        ptf(&bytes_slice[i]);
        if (i+1) % spl == 0 { print!("{}", splt) }
        if (i+1) % (ln*spl) == 0 || i == length - 1 { println!() }
    }
}

pub fn spin_sleep(time: f64) {
    let threshold = 0.0015;

    let st = Instant::now();
    if time > threshold {
        let bed_time = Duration::new(
            time.trunc() as _,
            ((time.fract()-threshold) * 1_000_000f64) as u32
        );
        thread::sleep(bed_time);
    }
    loop { if st.elapsed().as_secs_f64() > time { break } }
}

pub fn input<S: Display>(prefix: S) -> Result<String> {
    print!("{prefix}");
    std::io::stdout().flush();
    let s = std::io::stdin().lines().next().unwrap()?;
    Ok(s)
}

pub async fn tokio_input<S: Display>(prefix: S, timeout_sec: Option<f64>) -> Result<String> {
    // let prefix = prefix.into();
    // if let Some(timeout_sec) = timeout_sec {
    //     let timeout  = Duration::from_secs_f64(timeout_sec.into());
    //     tokio::time::timeout(timeout, async { input(prefix) }).await?
    // } else {
    //     tokio::spawn(async {
    //         input(prefix)
    //     }).await?
    // }
    print!("{prefix}");
    std::io::stdout().flush();
    let mut buf = vec![0; 100];
    if let Some(timeout_sec) = timeout_sec {
        let timeout  = Duration::from_secs_f64(timeout_sec.into());
        use std::io::BufReader;
        tokio::time::timeout(timeout, tokio::io::stdin().read(&mut buf)).await??;
    } else {
        tokio::io::stdin().read(&mut buf).await?;
    }
    String::from_utf8(
        buf.into_iter().filter(|&x| (x != b'\n') && (x != b'\r') && (x!= b'\0')).collect_vec()
    ).context("tokio_input error")

}

pub fn sleep(millis: u64) {
    thread::sleep(Duration::from_millis(millis))
}

pub async fn tokio_sleep(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await
}


// /// let (key, disp) = HKCU.create_subkey(r"software\test_0")?;
// /// println!("{disp:?}");
// /// key.set_value("a", &"written by Rust")?;
// /// key.set_value("b", &vec!["written", "by", "Rust"])?;
// /// key.set_value("c", &1234567890u32)?;
// /// let szval: String = key.get_value("a")?;
// /// let multi: Vec<String> = key.get_value("b")?;
// /// let dword = key.get_value::<u32, _>("c")?;
// /// println!("{:?}", szval);
// /// println!("{:?}", multi);
// /// println!("{:?}", dword);
// /// HKCU.delete_subkey_all(r"software\test_0")?;
// pub fn set_prog_id<>(parent_key: &RegKey, prog_id: &str, prog_cmd: &str, verb: &str, name: &str, icon: Option<&str>) -> DynResult<()>{
//     let (id_key, _) = parent_key.create_subkey(prog_id).context("create key progId")?;
//     let (verb_key, _) = id_key.create_subkey("Shell\\".to_owned() + verb).context("create key progId/verb")?;
//     let (cmd_key, _) = verb_key.create_subkey("Command").context("create key progId/verb/command")?;
//
//     cmd_key.set_value("", &prog_cmd)?;
//     id_key.set_value("", &name)?;
//
//     if let Some(ico) = icon {
//         verb_key.set_value("Icon", &ico).context("set progId icon")?;
//
//         let (ico_key, _) = id_key.create_subkey("DefaultIcon").context("create key progId/DefaultIcon")?;
//         ico_key.set_value("", &ico).context("set progId/DefaultIcon")?;
//     }
//
//     Ok(())
// }
//
// pub fn associate_ext<P: AsRef<OsStr> + Debug>(parent_key: &RegKey, prog_id: &str, exts: &[P]) -> DynResult<()> {
//     exts.iter()
//         .map(|ext| {
//             let (ext_key, _) = parent_key.create_subkey(ext).context(format!("create key {ext:?}"))?;
//             let (ow_key, _) = ext_key.create_subkey("OpenWithProgids").context(format!("create key {ext:?}/OpenWithProgids"))?;
//
//             ow_key.set_raw_value(prog_id, &RegValue::default()).context(format!("set progId for {ext:?}"))
//         })
//         .try_fold((), |_, b| b)?;
//     Ok(())
// }
//
// pub fn associate_ext_user_choice<P: AsRef<OsStr> + Debug>(parent_key: &RegKey, prog_id: &str, exts: &[P]) -> DynResult<()> {
//     exts.iter()
//         .map(|ext| {
//             let (ext_key, _) = parent_key.create_subkey(ext).context(format!("create key {ext:?}"))?;
//             let (ow_key, _) = ext_key.create_subkey("OpenWithProgids").context(format!("create key {ext:?}/OpenWithProgids"))?;
//             ow_key.set_raw_value(prog_id, &RegValue::default()).context(format!("set progId for {ext:?}"))?;
//
//             todo!("compute hash!");
//
//             let (uc_key, _) = ext_key.create_subkey("UserChoice").context(format!("create key {ext:?}/UserChoice"))?;
//             uc_key.set_value("Hash", &"78").context(format!("set {ext:?}/UserChoice/Hash"))?;
//             uc_key.set_value("ProgId", &prog_id).context(format!("set {ext:?}/UserChoice/ProgId"))
//         })
//         .try_fold((), |_, b| b)?;
//     Ok(())
// }
