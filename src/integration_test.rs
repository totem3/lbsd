use super::*;
use std::io::BufReader;

#[test]
fn test_integration() {
    let _ = env_logger::builder().is_test(true).try_init();
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
    log::error!("_main");
    _main(&mut r, &mut w);
    log::error!("done");
    let s = std::str::from_utf8(&w).unwrap();
    let row = Row {
        id: 1,
        username: *b"foo\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
        email: *b"bar\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
    };
    assert_eq!(s, format!("{:?}\n", row));
}
