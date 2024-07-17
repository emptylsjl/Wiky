use once_cell::sync::OnceCell;

pub static THREAD_COUNT: OnceCell<usize> = OnceCell::new();