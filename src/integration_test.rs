#![cfg(test)]

use super::*;
use std::io::{BufReader, Write};
use std::fs;

fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[test]
fn test_integration() {
    init();
    let mut buf: Vec<u8> = vec![];
    let _ = buf.write_fmt(format_args!(
        r#"insert 1 "foo" "bar"
select
.exit
"#
    ));
    let mut buf: &[u8] = buf.as_ref();
    let mut r = BufReader::new(&mut buf);
    let mut w: Vec<u8> = vec![];
    let filename = "tmp/test_integration.db";
    let _ = fs::remove_file(&filename);
    _main(filename, &mut r, &mut w);
    let s = std::str::from_utf8(&w).unwrap();
    let mut row = vec![];
    cols_to_row(&mut row, 1, "foo", "bar");
    assert_eq!(s, format!("db > Executed\ndb > {:?}\nExecuted\ndb > ", display_row(&row)));
}

#[test]
fn test_keeps_data_after_closing_connection() {
    init();
    let mut buf: Vec<u8> = vec![];
    let _ = buf.write_fmt(format_args!(
        r#"insert 1 "foo" "bar"
.exit
"#
    ));
    let mut buf: &[u8] = buf.as_ref();
    let mut r = BufReader::new(&mut buf);
    let mut w: Vec<u8> = vec![];
    let filename = "tmp/test_keeps_data_after_closing_connection.db";
    let _ = fs::remove_file(&filename);
    _main(filename, &mut r, &mut w);
    let s = std::str::from_utf8(&w).unwrap();

    assert_eq!(s, format!("db > Executed\ndb > "));
    let mut buf: Vec<u8> = vec![];
    let _ = buf.write_fmt(format_args!(
        r#"select
.exit
"#
    ));
    let mut buf: &[u8] = buf.as_ref();
    let mut r = BufReader::new(&mut buf);
    let mut w: Vec<u8> = vec![];
    _main(filename, &mut r, &mut w);
    let s = std::str::from_utf8(&w).unwrap();
    let mut row = vec![];
    cols_to_row(&mut row, 1, "foo", "bar");
    let expected = format!(
        r#"db > {:?}
Executed
db > "#,
        display_row(&row)
    );
    assert_eq!(s, expected);
}
