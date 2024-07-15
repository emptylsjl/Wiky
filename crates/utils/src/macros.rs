
#[macro_export]
macro_rules! flusher {
    (true) => { std::io::stdout().flush(); };
    (true,) => { std::io::stdout().flush(); };
    (false) => { {} };
    (false,) => { {} };
}

#[macro_export]
macro_rules! sb {($($arg:tt)*) => { "" };}

#[macro_export]
macro_rules! get_flush2 {
    () => { {} };
    ((flush=$($bool:tt)*)$($no:tt)*) => { flusher!($($bool)*) };
    (($($no:tt)*)$($tk:tt)*) => { get_flush2!($($tk)*) };
}

#[macro_export]
macro_rules! get_end3 {
    ((end=$exp:expr)$($no:tt)*) => { print!("{}", $exp) };
    ((end=$exp:expr;)$($no:tt)*) => { print!("{:}", $exp) };
    ((end=$exp:expr;$($tk:tt)*)$($no:tt)*) => { print!(concat!("{:", $(stringify!($tk),)* "}"), $exp) };
    (($($no:tt)*)$($tk:tt)*) => { get_end3!($($tk)*) };
    () => { print!("\n") }
}

#[macro_export]
macro_rules! get_sep_e {
    ((sep=$exp:expr)$($no:tt)*) => { $exp };
    ((sep=$exp:expr;)$($no:tt)*) => { $exp };
    ((sep=$exp:expr;$($tk:tt)*)$($no:tt)*) => { $exp };
    (($($no:tt)*)$($tktail:tt)*) => { get_sep_e!($($tktail)*) };
    () => { ", " }
}

#[macro_export]
macro_rules! get_sep_f {
    ((sep=$exp:expr)$($no:tt)*) => { "{}" };
    ((sep=$exp:expr;)$($no:tt)*) => { "{}" };
    ((sep=$exp:expr;$($tk:tt)*)$($no:tt)*) => { concat!("{:", $(stringify!($tk),)* "}") };
    (($($no:tt)*)$($tktail:tt)*) => { get_sep_f!($($tktail)*) };
    () => { "{}" }
}

macro_rules! print_fmt {
    ((sep=$($no:tt)*)$($tk:tt)*) => { "" };
    ((end=$($no:tt)*)$($tk:tt)*) => { "" };
    ((flush=$($no:tt)*)$($tk:tt)*) => { "" };
    (($exp:expr)$($tk:tt)*) => { concat!("{}", get_sep_f!($($tk)*), fmt_str!($($tk)*)) };
    (($exp:expr;)$($tk:tt)*) => { concat!("{:}", get_sep_f!($($tk)*), fmt_str!($($tk)*)) };
    (($exp:expr;$($fmt_tk:tt)*)$($tk:tt)*) => { concat!("{:", $(stringify!($fmt_tk),)* "}", get_sep_f!($($tk)*), fmt_str!($($tk)*)) };
    () => { "" }
}

macro_rules! get_args {
    ({$($tkhead:tt)*}(sep=$($no:tt)*)$($tk:tt)*) => { {} };
    ({$($tkhead:tt)*}(end=$($no:tt)*)$($tk:tt)*) => { {} };
    ({$($tkhead:tt)*}(flush=$($no:tt)*)$($tk:tt)*) => { {} };
    ({$($tkhead:tt)*}($exp:expr)$($tk:tt)*) => { get_args!({$($tkhead)*, $exp, sep_expr__,}$($tk)*) };
    ({$($tkhead:tt)*}($exp:expr;)$($tk:tt)*) => { get_args!({$($tkhead)*, $exp, sep_expr__,}$($tk)*) };
    ({$($tkhead:tt)*}($exp:expr;$($fmt_tk:tt)*)$($tk:tt)*) => { get_args!({$($tkhead)*, $exp, sep_expr__,}$($tk)*) };
    ({$($tkhead:tt)*}) => { {} }
}

#[macro_export]
macro_rules! print_fmt2 {
    (ed ($exp:expr)) => {
        print!(concat!("{}"), $exp);
    };
    (ed ($exp:expr;)) => {
        print!(concat!("{:}"), $exp);
    };
    (ed ($exp:expr;$($fmt_tk:tt)*)) => {
        print!(concat!("{:", $(stringify!($fmt_tk),)* "}"), $exp);
    };
    (($exp:expr)$($tktail:tt)*) => {
        print!(concat!("{}", get_sep_f!($($tktail)*)), $exp, get_sep_e!($($tktail)*));
    };
    (($exp:expr;)$($tktail:tt)*) => {
        print!(concat!("{:}", get_sep_f!($($tktail)*)), $exp, get_sep_e!($($tktail)*));
    };
    (($exp:expr;$($fmt_tk:tt)*)$($tktail:tt)*) => {
        print!(concat!("{:", $(stringify!($fmt_tk),)* "}", get_sep_f!($($tktail)*)), $exp, get_sep_e!($($tktail)*));
    };
}

#[macro_export]
macro_rules! parse_args2 {
    (($($tk:tt)*)) => { print_fmt2!(ed ($($tk)*)); };
    (($($tk:tt)*)(sep=$($no:tt)*)$($no2:tt)*) => { print_fmt2!(ed ($($tk)*)); };
    (($($tk:tt)*)(end=$($no:tt)*)$($no2:tt)*) => { print_fmt2!(ed ($($tk)*)); };
    (($($tk:tt)*)(flush=$($no:tt)*)$($no2:tt)*) => { print_fmt2!(ed ($($tk)*)); };
    (($($tk:tt)*)$($tktail:tt)*) => {
        print_fmt2!(($($tk)*)$($tktail)*);
        parse_args2!($($tktail)*)
    };
    () => { "" }
    // (($exp:expr)$($tk:tt)*) => {
    //     // print!(concat!("{}", get_sep_f!($($tk)*)), $exp, sep_expr__);
    //     print_fmt!(($exp)$($tk)*);
    //     get_args2!($($tk)*)
    // };
    // (($exp:expr;)$($tk:tt)*) => {
    //     // print!(concat!("{}", get_sep_f!($($tk)*)), $exp, sep_expr__);
    //     print_fmt!(($exp)$($tk)*);
    //     get_args2!($($tk)*)
    // };
    // (($exp:expr;$($fmt_tk:tt)*)$($tk:tt)*) => {
    //     // print!(concat!("{:", $(stringify!($fmt_tk),)* "}", get_sep_f!($($tk)*)), $exp, sep_expr__);
    //
    //     print_fmt!(($exp;$($fmt_tk)*)$($tk)*);
    //     get_args2!($($tk)*)
    // };
}

#[macro_export]
macro_rules! print_fmt3 {
    (ed ($exp:expr)) => { "{}" };
    (ed ($exp:expr;)) => { "{:}" };
    (ed ($exp:expr;$($fmt_tk:tt)*)) => { concat!("{:", $(stringify!($fmt_tk),)* "}") };
    (($exp:expr)$($tk:tt)*) => { concat!("{}", get_sep_f!($($tk)*)) };
    (($exp:expr;)$($tk:tt)*) => { concat!("{:}", get_sep_f!($($tk)*)) };
    (($exp:expr;$($fmt_tk:tt)*)$($tk:tt)*) => { concat!("{:", $(stringify!($fmt_tk),)* "}", get_sep_f!($($tk)*)) }
}

#[macro_export]
macro_rules! print_exp {
    ($exp:expr) => { $exp };
    ($exp:expr;) => { $exp };
    ($exp:expr;$($fmt_tk:tt)*) => { $exp };
}

#[macro_export]
macro_rules! parse_args3 {
    (@($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*));
    };
    (@($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)(sep=$($no:tt)*)$($no2:tt)*) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*))
    };
    (@($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)(end=$($no:tt)*)$($no2:tt)*) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*))
    };
    (@($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)(flush=$($no:tt)*)$($no2:tt)*) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*))
    };

    (@($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)$($tktail:tt)*) => {
        parse_args3!(@($($fmt_tk)*, print_fmt3!(($($tk)*)$($tktail)*))($($exp_tk)*, print_exp!($($tk)*), get_sep_e!($($tktail)*))@$($tktail)*);
    };
    (($($tk:tt)*)) => {
        print!(print_fmt3!(ed ($($tk)*)), print_exp!($($tk)*));
    };
    (($($tk:tt)*)$($tktail:tt)*) => {
        parse_args3!(@(print_fmt3!(($($tk)*)$($tktail)*))(print_exp!($($tk)*), get_sep_e!($($tktail)*))@$($tktail)*);
    };
}

#[macro_export]
macro_rules! parse_args4 {
    (@($($s:tt)*)($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*));
    };
    (@($($s:tt)*)($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)(sep=$($no:tt)*)$($no2:tt)*) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*))
    };
    (@($($s:tt)*)($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)(end=$($no:tt)*)$($no2:tt)*) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*))
    };
    (@($($s:tt)*)($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)(flush=$($no:tt)*)$($no2:tt)*) => {
        print!(concat!($($fmt_tk)*, print_fmt3!(ed ($($tk)*))), $($exp_tk)*, print_exp!($($tk)*))
    };

    (@($($s:tt)*)($($fmt_tk:tt)*)($($exp_tk:tt)*)@($($tk:tt)*)$($tktail:tt)*) => {
        parse_args4!(@($($s)*)($($fmt_tk)*, print_fmt3!(($($tk)*)$($tktail)*))($($exp_tk)*, print_exp!($($tk)*), $($s)*)@$($tktail)*);
    };
    (($($s:tt)*)($($tk:tt)*)) => {
        print!(print_fmt3!(ed ($($tk)*)), print_exp!($($tk)*));
    };
    (($($s:tt)*)($($tk:tt)*)$($tktail:tt)*) => {
        parse_args4!(@($($s)*)(print_fmt3!(($($tk)*)$($tktail)*))(print_exp!($($tk)*), $($s)*)@$($tktail)*);
    };

    () => { "" }
}

#[macro_export]
macro_rules! vprintf {
    ($($tk:tt)*) => {{
        let sep_expr_ = get_sep_e!($($tk)*);
        // // a print! per arg
        // parse_args2!($($tk)*);

        // // independent sep expression
        // parse_args3!($($tk)*);

        // pre defined sep expression
        parse_args4!((sep_expr_)$($tk)*);

        get_end3!($($tk)*); // actually this is bad
        get_flush2!($($tk)*);
    }};
}

#[macro_export]
macro_rules! verbose {
    ($(($($tkp:tt)*))*{$($tkhead:tt)*}) => {vprintf!($(($($tkp)*))*($($tkhead)*))};
    ($(($($tkp:tt)*))*{$($tkhead:tt)*}$tk:tt) => {vprintf!($(($($tkp)*))*($($tkhead)*$tk))};
    ($(($($tkp:tt)*))*{$($tkhead:tt)*}$tk:tt,) => {vprintf!($(($($tkp)*))*($($tkhead)*$tk))};
    ($(($($tkp:tt)*))*{$($tkhead:tt)*},$tk:tt$($tktail:tt)*) => {verbose!($(($($tkp)*))*($($tkhead)*){$tk}$($tktail)*)};
    ($(($($tkp:tt)*))*{$($tkhead:tt)*}$tk:tt$($tktail:tt)*) => {verbose!($(($($tkp)*))*{$($tkhead)*$tk}$($tktail)*)};
    ({$($tkhead:tt)*},$tk:tt$($tktail:tt)*) => {verbose!(($($tkhead)*){$tk}$($tktail)*)};
    ({$($tkhead:tt)*}$tk:tt$($tktail:tt)*) => {verbose!({$($tkhead)*$tk}$($tktail)*)};
    ($tk0:tt$($tk:tt)*) => {verbose!({$tk0}$($tk)*)};
}


///```
/// use std::io::Write; // required by flush
/// use utils::*;
///
/// let a1 = "emm";
///
/// printf!("6");
///
/// printf!(1, "a", true);
///
/// printf!(12*12;X, "a";p, a1;?);
///
/// printf!("a";p, 38;04X, sep=" - ", end=" ha", flush=true);
///
/// printf!("\n", "b".to_owned() + "c", 38;04X, vec![1, 2];?, (233, a1);#?, end="ha";9, flush=false, sep="\n - \n");
/// ```
#[macro_export]
macro_rules! printf {
    () => { println!() };
    ($e:expr) => {println!("{:?}", $e)};

    // ($($e:expr),*, sep=$exp:expr) => {verbose!($($e,)*sep=$exp)};
    // ($($e:expr),*, sep=$exp:expr;$($tk:tt)*) => {verbose!($($e,)*sep=$exp;$($tk)*)};
    // ($($e:expr),*, sep=$exp:expr,$($tk:tt)*) => {verbose!($($e,)*sep=$exp,$($tk)*)};
    //
    // ($($e:expr),*, end=$exp:expr) => {verbose!($($e,)*end=$exp)};
    // ($($e:expr),*, end=$exp:expr;$($tk:tt)*) => {verbose!($($e,)*end=$exp;$($tk)*)};
    // ($($e:expr),*, end=$exp:expr,$($tk:tt)*) => {verbose!($($e,)*end=$exp,$($tk)*)};
    //
    // ($($e:expr),*, flush=$exp:expr) => {verbose!($($e,)*flush=$exp)};
    // ($($e:expr),*, flush=$exp:expr;$($tk:tt)*) => {verbose!($($e,)*flush=;$exp$($tk)*)};
    // ($($e:expr),*, flush=$exp:expr,$($tk:tt)*) => {verbose!($($e,)*flush=,$exp$($tk)*)};
    //
    // ($e0:expr, $($e:expr),*) => {println!(concat!("{:?}", $(", {:?}", sb!($e), )*), $e0, $($e,)*)};
    // ($e0:expr, $($e:expr,)*) => {println!(concat!("{:?}", $(", {:?}", sb!($e), )*), $e0, $($e,)*)};

    ($($tk:tt)*) => { verbose!($($tk)*) }
}