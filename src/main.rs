#![allow(dead_code)]

extern crate byteorder;
extern crate env_logger;
extern crate log;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::exit;

use log::trace;

#[cfg(test)]
mod integration_test;

struct InputBuffer {
    buffer: String,
}

impl InputBuffer {
    fn new() -> Self {
        InputBuffer {
            buffer: "".to_string(),
        }
    }

    fn read_line(&mut self, input: &mut dyn io::BufRead) -> io::Result<usize> {
        let mut buf = String::new();
        let size = input.read_line(&mut buf)?;
        self.buffer = buf.trim_end().to_string();
        Ok(size)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum MetaCommandResult {
    Exit,
    UnrecognizedCommand,
}

fn do_meta_command(input: &str) -> Result<(), MetaCommandResult> {
    match input {
        ".exit" => Err(MetaCommandResult::Exit),
        _ => Err(MetaCommandResult::UnrecognizedCommand),
    }
}

#[derive(Debug)]
struct Statement {
    st_type: StatementType,
    row_to_insert: Option<Row>,
}

impl Statement {
    fn new(st_type: StatementType) -> Self {
        Statement {
            st_type,
            row_to_insert: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementType {
    Insert,
    Select,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrepareError {
    UnrecognizedStatement,
    SyntaxError,
}

#[derive(Clone)]
struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let username = match std::str::from_utf8(&self.username) {
            Ok(v) => v,
            Err(_) => "<username>",
        };
        let email = match std::str::from_utf8(&self.email) {
            Ok(v) => v,
            Err(_) => "<email>",
        };
        write!(
            f,
            "Row<id:{}, username:{}, email:{}>",
            self.id, username, email
        )
    }
}

impl Row {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.write_u32::<LittleEndian>(self.id).unwrap();
        buf.extend_from_slice(&self.username);
        buf.extend_from_slice(&self.email);
    }
    fn deserialize(input: &[u8]) -> Row {
        let mut rdr = Cursor::new(input);
        let id = rdr.read_u32::<LittleEndian>().unwrap();
        let mut username = [0u8; 32];
        let _ = rdr.read(&mut username).unwrap();
        let mut email = [0u8; 255];
        let _ = rdr.read(&mut email).unwrap();
        Row {
            id,
            username,
            email,
        }
    }
}

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; COLUMN_USERNAME_SIZE]>();
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; COLUMN_EMAIL_SIZE]>();
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_SIZE + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;
type Page = Vec<u8>;

struct Pager {
    file: File,
    file_length: usize,
    pages: Vec<Option<Page>>,
}

impl Pager {
    fn new(filename: impl AsRef<Path>) -> Result<Self, String> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&filename)
        {
            Ok(v) => v,
            Err(e) => return Err(format!("{}", e)),
        };
        let metadata = match fs::metadata(&filename) {
            Ok(v) => v,
            Err(e) => return Err(format!("{}", e)),
        };
        let file_length = metadata.len().try_into().unwrap();
        let pages = vec![None; TABLE_MAX_PAGES];
        Ok(Pager {
            file,
            file_length,
            pages,
        })
    }

    fn get_page(&mut self, page_num: usize) -> Option<&Page> {
        log::trace!("get_page called");
        if self.pages[page_num].is_some() {
            log::trace!("page is already on memory. return");
            return self.pages[page_num].as_ref();
        };
        log::trace!("page is not on memory. try to read from file");
        let mut num_pages = self.file_length / PAGE_SIZE;
        trace!("num_pages: {}", num_pages);
        if self.file_length % PAGE_SIZE != 0 {
            num_pages += 1;
        }
        if page_num <= num_pages {
            match self
                .file
                .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            {
                Ok(_) => {
                    trace!("seek to {}", page_num * PAGE_SIZE);
                }
                Err(e) => {
                    log::error!("seek failed! {}", e);
                    panic!("seek failed! {}", e);
                }
            };
        }
        let mut buf = vec![0u8; PAGE_SIZE];
        match self.file.read(&mut buf) {
            Ok(n) => {
                trace!("read from file succeeded. read {} bytes", n);
            }
            Err(e) => {
                log::error!("read failed! {}", e);
                panic!("read failed! {}", e);
            }
        };
        self.pages[page_num] = Some(buf);
        self.pages[page_num].as_ref()
    }

    fn get_page_mut(&mut self, page_num: usize) -> Option<&mut Page> {
        let _ = self.get_page(page_num);
        self.pages[page_num].as_mut()
    }

    fn flush(&mut self, num_rows: usize) -> Result<(), String> {
        let full_pages = num_rows / ROWS_PER_PAGE;
        for i in 0..full_pages {
            match &self.pages[i] {
                Some(page) => {
                    match self.file.write(&page[..]) {
                        Ok(n) => {log::trace!("write {} bytes to file", n)},
                        Err(e) => {
                            log::error!("failed to write file: {}", e);
                            return Err(format!("{}", e));
                        }
                    };
                },
                None => {}
            }
        }
        // TODO: 境界値の扱いが雑でうまく復元できていないので治す
        // 境界値の扱いの問題ではなくて、ファイルに書き込んだときに末尾が改行コードになっているっぽい？
        let additional_rows = num_rows % ROWS_PER_PAGE;
        match &self.pages[full_pages] {
            Some(page) => {
                for (cnt, u) in page[0..(additional_rows*ROW_SIZE)].iter().enumerate() {
                    if cnt % 16 == 0 {
                        print!("{:04}: ", cnt);
                    }
                    print!("{:02x} ", u);
                    if (cnt+1) % 16 == 0 {
                        println!();
                    }
                }
                println!();
                match self.file.write(&page[0..(additional_rows*ROW_SIZE)]) {
                    Ok(n) => {log::trace!("write {} bytes to file", n)},
                    Err(e) => {
                        log::error!("failed to write file: {}", e);
                        return Err(format!("{}", e));
                    }
                };
            }
            None => {}
        }
      Ok(())
    }

    fn get_num_page(&self) -> usize {
      self.file_length / ROW_SIZE
    }
}

struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    fn new<P>(filename: P) -> Result<Self, String>
    where
        P: AsRef<Path>,
    {
        let pager = Pager::new(&filename)?;
        let num_rows = pager.get_num_page();
        trace!("initialize Table for {:?}. num_rows: {}", &filename.as_ref().display(), num_rows);
        Ok(Table { num_rows, pager })
    }

    fn page_num(&self, row_num: usize) -> usize {
        row_num / ROWS_PER_PAGE
    }

    fn bytes_offset(&self, row_num: usize) -> usize {
        let rows = row_num % ROWS_PER_PAGE;
        rows * ROW_SIZE
    }

    fn is_full(&self) -> bool {
        self.num_rows >= TABLE_MAX_ROWS
    }

    fn close(&mut self) -> Result<(), String> {
        self.pager.flush(self.num_rows)
    }
}

fn prepare_statement(input: &InputBuffer) -> Result<Statement, PrepareError> {
    let lower = input.buffer.to_lowercase();
    if lower.starts_with("insert") {
        let mut statement = Statement::new(StatementType::Insert);
        let bytes = input.buffer.clone().into_bytes();
        let buf: &[u8] = bytes.as_ref();
        let mut buf = io::BufReader::new(buf);
        let _ = buf.read_until(b' ', &mut Vec::new());
        let mut id = vec![];
        let _ = buf.read_until(b' ', &mut id);
        let id_str = match std::str::from_utf8(id.trim()) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "id u8 to str conversion failed. input:{:?}, error:{}",
                    id.trim(),
                    e
                );
                return Err(PrepareError::SyntaxError);
            }
        };
        let id: u32 = match id_str.parse::<u32>() {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "id str -> u32 conversion failed. input:{}, error:{}",
                    id_str,
                    e
                );
                return Err(PrepareError::SyntaxError);
            }
        };
        log::trace!("id: {}", id);
        let _ = buf.read_until(b'"', &mut vec![]);
        let mut username = vec![];
        match buf.read_until(b'"', &mut username) {
            Ok(v) => {
                log::trace!("read {} bytes for username", v);
            }
            Err(e) => {
                log::error!("read username failed: {}", e);
                return Err(PrepareError::SyntaxError);
            }
        };
        let _ = username.pop(); // 末尾の"を取り除く
        #[allow(unused)]
        let username_str = match std::str::from_utf8(&username) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "username u8 -> str conversion failed. input: {:?}, error: {}",
                    username,
                    e
                );
                return Err(PrepareError::SyntaxError);
            }
        };
        log::trace!("username: {}", username_str);
        let _ = buf.read_until(b'"', &mut vec![]);
        let mut email = vec![];
        match buf.read_until(b'"', &mut email) {
            Ok(v) => {
                log::trace!("read {} bytes for email", v);
            }
            Err(e) => {
                log::error!("read email failed: {}", e);
                return Err(PrepareError::SyntaxError);
            }
        };
        let _ = email.pop(); // 末尾の"を取り除く
        #[allow(unused)]
        let email_str = match std::str::from_utf8(&email) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "email u8 -> str conversion failed. input: {:?}, error: {}",
                    email,
                    e
                );
                return Err(PrepareError::SyntaxError);
            }
        };
        log::trace!("email: {}", email_str);
        let mut u = [0u8; 32];
        u[0..(username.len())].copy_from_slice(&username);
        let mut e = [0u8; 255];
        e[0..(email.len())].copy_from_slice(&email);
        let row = Row {
            id,
            username: u,
            email: e,
        };
        statement.row_to_insert = Some(row);
        return Ok(statement);
    }
    if lower.starts_with("select") {
        let statement = Statement::new(StatementType::Select);
        return Ok(statement);
    }
    Err(PrepareError::UnrecognizedStatement)
}

#[derive(Debug, PartialEq, Eq)]
enum ExecuteResult {
    TableFull,
    InvalidStatement,
    PageMutFailure,
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteResult> {
    if table.is_full() {
        return Err(ExecuteResult::TableFull);
    }
    let row_to_insert = match &statement.row_to_insert {
        Some(s) => s,
        None => {
            return Err(ExecuteResult::InvalidStatement);
        }
    };
    let mut buf = Vec::with_capacity(ROW_SIZE);
    row_to_insert.serialize(&mut buf);
    trace!("serialized size: {}", buf.len());
    let mut cursor = TCursor::table_end(table);
    match cursor.get_mut() {
        Some(v) => v.copy_from_slice(buf.as_ref()),
        None => {
            log::error!("cannot get mutable reference to page!");
            return Err(ExecuteResult::PageMutFailure);
        }
    };
    table.num_rows += 1;
    Ok(())
}

fn execute_select(_statement: &Statement, table: &mut Table) -> Result<Vec<Row>, ExecuteResult> {
    let mut rows = Vec::new();
    let mut cursor = TCursor::table_start(table);
    while !cursor.end_of_table {
        let bytes = match cursor.get() {
            Some(v) => v.to_vec(),
            None => vec![0u8; ROW_SIZE],
        };
        cursor.advance();
        log::error!("bytes: {:?}", bytes);
        let row = Row::deserialize(&bytes);
        log::trace!("{:?}", row);
        rows.push(row);
    }
    Ok(rows)
}

// テストのため一時的にVec<Row>を返すようにしておく
fn execute_statement(statement: &Statement, table: &mut Table) -> Result<Vec<Row>, ExecuteResult> {
    match statement.st_type {
        StatementType::Insert => {
            // テストのためselectでRowsを返したいので一時的に合わせておく
            match execute_insert(statement, table) {
                Ok(_) => Ok(vec![]),
                Err(e) => Err(e),
            }
        }
        StatementType::Select => {
            execute_select(statement, table)
        }
    }
}

struct TCursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

impl<'a> TCursor<'a> {
    fn table_start(table: &'a mut Table) -> Self {
        let end_of_table = table.num_rows == 0;
        TCursor {
            table,
            row_num: 0,
            end_of_table
        }
    }

    fn table_end(table: &'a mut Table) -> Self {
        let row_num = table.num_rows;
        TCursor {
            table,
            row_num,
            end_of_table: true,
        }
    }

    fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true
        }
    }

    fn get_mut(&mut self) -> Option<&mut [u8]> {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        let bytes_offset = self.table.bytes_offset(row_num);
        match self.table.pager.get_page_mut(page_num) {
            Some(page) => {
                Some(&mut page[bytes_offset..(bytes_offset+ROW_SIZE)])
            }
            None => None
        }
    }

    fn get(&mut self) -> Option<&[u8]> {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        let bytes_offset = self.table.bytes_offset(row_num);
        match self.table.pager.get_page(page_num) {
            Some(page) => {
                Some(&page[bytes_offset..(bytes_offset+ROW_SIZE)])
            }
            None => None
        }
    }
}

fn main() {
    env_logger::init();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    let mut w = io::stdout();
    let filename = match std::env::args().nth(1) {
        Some(v) => v,
        None => {
            eprintln!("pass filename");
            std::process::exit(1);
        }
    };
    let result = _main(filename, &mut stdin, &mut w);
    exit(result);
}

fn _main<P: AsRef<Path>>(filename: P, r: &mut impl io::BufRead, w: &mut impl io::Write) -> i32 {
    let mut input_buffer = InputBuffer::new();
    let mut table = match Table::new(filename) {
        Ok(v) => v,
        Err(e) => {
            let _ = writeln!(w, "failed to initialize table: {}", e);
            return 1;
        }
    };
    loop {
        print_prompt(w);
        match input_buffer.read_line(r) {
            Ok(n) => {
                trace!("read {} bytes", n);
                if input_buffer.buffer.starts_with('.') {
                    match do_meta_command(&input_buffer.buffer) {
                        Ok(_) => {}
                        Err(MetaCommandResult::Exit) => {
                            match table.close() {
                                Ok(_) => {},
                                Err(e) => {
                                    log::error!("failed to close db: {}", e);
                                }
                            };
                            return 0;
                        }
                        Err(MetaCommandResult::UnrecognizedCommand) => {
                            let _ = writeln!(w, "Unrecognized command '{}'", &input_buffer.buffer);
                        }
                    }
                    continue;
                }
                let statement = prepare_statement(&input_buffer);
                match statement {
                    Ok(statement) => {
                        match execute_statement(&statement, &mut table) {
                            Ok(rows) => {
                                if !rows.is_empty() {
                                    for row in rows {
                                        let _ = writeln!(w, "{:?}", row);
                                    }
                                }
                                let _ = writeln!(w, "Executed");
                            }
                            Err(ExecuteResult::TableFull) => {
                                let _ = writeln!(w, "table is full");
                                break;
                            }
                            Err(ExecuteResult::InvalidStatement) => {
                                let _ = writeln!(w, "invalid statement");
                                break;
                            }
                            Err(ExecuteResult::PageMutFailure) => {
                                let _ = writeln!(w, "get page mutable ref failed");
                                break;
                            }
                        };
                    }
                    Err(PrepareError::UnrecognizedStatement) => {
                        let _ = writeln!(
                            w,
                            "Unrecognized keyword at start of '{}'",
                            &input_buffer.buffer
                        );
                        continue;
                    }
                    Err(PrepareError::SyntaxError) => {
                        let _ = writeln!(w, "Syntax error at '{}'", &input_buffer.buffer);
                        continue;
                    }
                }
            }
            Err(e) => {
                let _ = writeln!(w, "error: {:?}", e);
                return 1;
            }
        }
    }
    0
}

fn print_prompt(w: &mut impl io::Write) {
    let _ = write!(w, "db > ");
    w.flush().unwrap();
}

trait SliceExt {
    fn trim(&self) -> &Self;
}

impl SliceExt for [u8] {
    fn trim(&self) -> &[u8] {
        fn is_whitespace(c: u8) -> bool {
            c == b'\t' || c == b' '
        }

        fn is_not_whitespace(c: u8) -> bool {
            !is_whitespace(c)
        }

        if let Some(first) = self.iter().position(|&x| is_not_whitespace(x)) {
            if let Some(last) = self.iter().rposition(|&x| is_not_whitespace(x)) {
                &self[first..last + 1]
            } else {
                unreachable!();
            }
        } else {
            &[]
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_unrecognized_meta_command() {
        init();
        let res = do_meta_command(".foo");
        let err = res.err().unwrap();
        assert_eq!(err, MetaCommandResult::UnrecognizedCommand);
    }

    #[test]
    fn test_prepare_statement_insert() {
        init();
        let input = InputBuffer {
            buffer: r#"insert 1 "foo" "bar""#.to_string(),
        };
        let stmt = prepare_statement(&input).unwrap();
        assert_eq!(stmt.st_type, StatementType::Insert);
    }

    #[test]
    fn test_prepare_statement_large_insert() {
        init();
        let input = InputBuffer {
            buffer: r#"INSERT 1 "foo" "bar"#.to_string(),
        };
        let stmt = prepare_statement(&input).unwrap();
        assert_eq!(stmt.st_type, StatementType::Insert);
    }

    #[test]
    fn test_prepare_statement_select() {
        init();
        let input = InputBuffer {
            buffer: "select".to_string(),
        };
        let stmt = prepare_statement(&input).unwrap();
        assert_eq!(stmt.st_type, StatementType::Select);
    }

    #[test]
    fn test_prepare_statement_large_select() {
        init();
        let input = InputBuffer {
            buffer: "SELECT".to_string(),
        };
        let stmt = prepare_statement(&input).unwrap();
        assert_eq!(stmt.st_type, StatementType::Select);
    }

    #[test]
    fn test_prepare_statement_unknown() {
        init();
        let input = InputBuffer {
            buffer: "HOGE".to_string(),
        };
        let stmt = prepare_statement(&input);
        assert!(stmt.is_err());
        let err = stmt.err().unwrap();
        assert_eq!(err, PrepareError::UnrecognizedStatement);
    }

    #[test]
    fn test_serialize_row() {
        init();
        let id = 1;
        let username_bytes: &[u8] = b"totem3";
        let mut username: [u8; 32] = [0; 32];
        for (i, b) in username_bytes.iter().enumerate() {
            username[i] = *b;
        }
        let email_bytes: &[u8] = b"totem3@totem3.com";
        let mut email: [u8; 255] = [0; 255];
        for (i, b) in email_bytes.iter().enumerate() {
            email[i] = *b;
        }
        let row = Row {
            id,
            username,
            email,
        };
        let mut buffer = vec![];
        row.serialize(&mut buffer);
        let mut expected = vec![];
        let _ = expected.write_u32::<byteorder::LittleEndian>(row.id);
        expected.extend_from_slice(&[
            116, 111, 116, 101, 109, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ]);
        expected.extend_from_slice(&[
            116, 111, 116, 101, 109, 51, 64, 116, 111, 116, 101, 109, 51, 46, 99, 111, 109, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ]);
        assert_eq!(expected, buffer)
    }

    #[test]
    fn test_page_num() {
        let table = Table::new("tmp/test.db").unwrap();
        assert_eq!(table.page_num(12), 0);
        assert_eq!(table.page_num(13), 0);
        assert_eq!(table.page_num(14), 1);
        assert_eq!(table.page_num(15), 1);
    }

    fn test_bytes_offset() {
        let table = Table::new("tmp/test.db").unwrap();
        assert_eq!(table.bytes_offset(12), 291 * 12);
        assert_eq!(table.bytes_offset(13), 291 * 13);
        assert_eq!(table.bytes_offset(14), 0);
        assert_eq!(table.bytes_offset(15), 291);
    }

    #[test]
    fn test_execute_statement_insert_into_full_table() {
        let mut table = Table {
            num_rows: TABLE_MAX_ROWS,
            pager: Pager::new("tmp/test.db").unwrap(),
        };
        let stmt = Statement::new(StatementType::Insert);
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err, ExecuteResult::TableFull);
    }

    #[test]
    fn test_execute_statement_insert_without_row() {
        let mut table = Table {
            num_rows: 0,
            pager: Pager::new("tmp/test.db").unwrap(),
        };
        let stmt = Statement::new(StatementType::Insert);
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err, ExecuteResult::InvalidStatement);
    }

    #[test]
    fn test_execute_statement_insert() {
        init();
        let mut table = Table {
            num_rows: 0,
            pager: Pager::new("tmp/test.db").unwrap(),
        };
        let id = 1;
        let username_bytes: &[u8] = b"totem3";
        let mut username: [u8; 32] = [0; 32];
        for (i, b) in username_bytes.iter().enumerate() {
            username[i] = *b;
        }
        let email_bytes: &[u8] = b"totem3@totem3.com";
        let mut email: [u8; 255] = [0; 255];
        for (i, b) in email_bytes.iter().enumerate() {
            email[i] = *b;
        }
        let row = Row {
            id,
            username,
            email,
        };
        let stmt = Statement {
            st_type: StatementType::Insert,
            row_to_insert: Some(row.clone()),
        };
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_ok());
        let mut expected = Vec::with_capacity(ROW_SIZE);
        row.serialize(&mut expected);
        let buf = match table.pager.get_page(0) {
            Some(p) => Vec::from(&p[0..ROW_SIZE]),
            None => vec![0u8; ROW_SIZE],
        };
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_execute_select() {
        let mut table = Table::new("tmp/test.db").unwrap();
        let stmt = Statement::new(StatementType::Select);
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_insert_and_select() {
        init();
        let mut table = Table::new("tmp/test.db").unwrap();
        let id = 1;
        let username_bytes: &[u8] = b"totem3";
        let mut username: [u8; 32] = [0; 32];
        for (i, b) in username_bytes.iter().enumerate() {
            username[i] = *b;
        }
        let email_bytes: &[u8] = b"totem3@totem3.com";
        let mut email: [u8; 255] = [0; 255];
        for (i, b) in email_bytes.iter().enumerate() {
            email[i] = *b;
        }
        let row = Row {
            id,
            username,
            email,
        };
        let stmt = Statement {
            st_type: StatementType::Insert,
            row_to_insert: Some(row),
        };
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_ok());

        let stmt = Statement::new(StatementType::Select);
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_ok());
    }
}
