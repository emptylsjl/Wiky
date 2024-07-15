

use std::time::Duration;
pub use platform_const::*;

pub const NS_PER_US: u32 = 1_000;
pub const NS_PER_MS: u32 = 1_000_000;
pub const NS_PER_S : u32 = 1_000_000_000;

// pub mod

pub const S : Duration = Duration::from_secs(1);
pub const MS: Duration = Duration::from_millis(1);
pub const US: Duration = Duration::from_micros(1);
pub const NS: Duration = Duration::from_nanos(1);

#[cfg(target_os = "windows")]
mod platform_const {
    use winreg::enums::*;
    use winreg::RegKey;
    pub static HKCU: RegKey = RegKey::predef(HKEY_CURRENT_USER);
    pub static HKCR: RegKey = RegKey::predef(HKEY_CLASSES_ROOT);
    pub static HKLM: RegKey = RegKey::predef(HKEY_LOCAL_MACHINE);
}


