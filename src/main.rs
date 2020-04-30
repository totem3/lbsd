#![allow(dead_code)]

extern crate byteorder;
extern crate env_logger;
extern crate log;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::{self, BufRead, Cursor, Read, Write};
use std::process::exit;

use log::trace;

mod integration_test;

struct InputBuffer {
    buffer: String,
}

impl InputBuffer {
    fn new() -> Self {
        return InputBuffer {
            buffer: "".to_string(),
        };
    }

    fn read_line(&mut self, input: &mut dyn io::BufRead) -> io::Result<usize> {
        let mut buf = String::new();
        let size = input.read_line(&mut buf)?;
        self.buffer = buf.trim_end().to_string();
        return Ok(size);
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum MetaCommandResult {
    Exit,
    UnrecognizedCommand,
}

fn do_meta_command(input: &str) -> Result<(), MetaCommandResult> {
    match input {
        ".exit" => return Err(MetaCommandResult::Exit),
        _ => return Err(MetaCommandResult::UnrecognizedCommand),
    }
}

#[derive(Debug)]
struct Statement {
    st_type: StatementType,
    row_to_insert: Option<Row>,
}

impl Statement {
    fn new(st_type: StatementType) -> Self {
        return Statement {
            st_type,
            row_to_insert: None,
        };
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
    fn deserialize(input: &Vec<u8>) -> Row {
        let mut rdr = Cursor::new(input);
        let id = rdr.read_u32::<LittleEndian>().unwrap();
        let mut username = [0u8; 32];
        let _ = rdr.read(&mut username).unwrap();
        let mut email = [0u8; 255];
        let _ = rdr.read(&mut email).unwrap();
        return Row {
            id,
            username,
            email,
        };
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

type Page = [u8; PAGE_SIZE];
struct Table {
    num_rows: usize,
    pages: [Page; TABLE_MAX_PAGES],
}

impl Default for Table {
    fn default() -> Self {
        Table {
            num_rows: 0,
            pages: [[0; PAGE_SIZE]; TABLE_MAX_PAGES],
        }
    }
}

impl Table {
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
    return Err(PrepareError::UnrecognizedStatement);
}

#[derive(Debug, PartialEq, Eq)]
enum ExecuteResult {
    TableFull,
    InvalidStatement,
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
    let page_num = table.page_num(table.num_rows);
    let bytes_offset = table.bytes_offset(table.num_rows);
    table.pages[page_num][bytes_offset..(bytes_offset + ROW_SIZE)].copy_from_slice(buf.as_ref());
    table.num_rows += 1;
    return Ok(());
}

fn execute_select(_statement: &Statement, table: &mut Table) -> Result<Vec<Row>, ExecuteResult> {
    let mut rows = Vec::new();
    for i in 0..table.num_rows {
        let page_num = table.page_num(i);
        let bytes_offset = table.bytes_offset(i);
        let bytes = Vec::from(&table.pages[page_num][bytes_offset..bytes_offset + ROW_SIZE]);
        log::error!("bytes: {:?}", bytes);
        let row = Row::deserialize(&bytes);
        log::trace!("{:?}", row);
        rows.push(row);
    }
    return Ok(rows);
}

// テストのため一時的にVec<Row>を返すようにしておく
fn execute_statement(statement: &Statement, table: &mut Table) -> Result<Vec<Row>, ExecuteResult> {
    match statement.st_type {
        StatementType::Insert => {
            // テストのためselectでRowsを返したいので一時的に合わせておく
            match execute_insert(statement, table) {
                Ok(_) => return Ok(vec![]),
                Err(e) => return Err(e),
            };
        }
        StatementType::Select => {
            return execute_select(statement, table);
        }
    }
}

fn main() {
    env_logger::init();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    let mut w = io::stdout();
    let result = _main(&mut stdin, &mut w);
    exit(result);
}

fn _main(r: &mut impl io::BufRead, w: &mut impl io::Write) -> i32 {
    let mut input_buffer = InputBuffer::new();
    let mut table = Table::default();
    loop {
        trace!("write prompt");
        print_prompt(w);
        trace!("input_buffer");
        match input_buffer.read_line(r) {
            Ok(n) => {
                trace!("read {} bytes", n);
                if input_buffer.buffer.starts_with(".") {
                    match do_meta_command(&input_buffer.buffer) {
                        Ok(_) => {}
                        Err(MetaCommandResult::Exit) => {
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
    return 0;
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
        fn is_whitespace(c: &u8) -> bool {
            c == &b'\t' || c == &b' '
        }

        fn is_not_whitespace(c: &u8) -> bool {
            !is_whitespace(c)
        }

        if let Some(first) = self.iter().position(is_not_whitespace) {
            if let Some(last) = self.iter().rposition(is_not_whitespace) {
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
        let table = Table::default();
        assert_eq!(table.page_num(12), 0);
        assert_eq!(table.page_num(13), 0);
        assert_eq!(table.page_num(14), 1);
        assert_eq!(table.page_num(15), 1);
    }

    fn test_bytes_offset() {
        let table = Table::default();
        assert_eq!(table.bytes_offset(12), 291 * 12);
        assert_eq!(table.bytes_offset(13), 291 * 13);
        assert_eq!(table.bytes_offset(14), 0);
        assert_eq!(table.bytes_offset(15), 291 * 1);
    }

    #[test]
    fn test_execute_statement_insert_into_full_table() {
        let mut table = Table {
            num_rows: TABLE_MAX_ROWS,
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
        let buf = Vec::from(&table.pages[0][0..ROW_SIZE]);
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_execute_select() {
        let mut table = Table::default();
        let stmt = Statement::new(StatementType::Select);
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_insert_and_select() {
        init();
        let mut table = Table {
            ..Default::default()
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

        let stmt = Statement::new(StatementType::Select);
        let result = execute_statement(&stmt, &mut table);
        assert!(result.is_ok());
    }
}
