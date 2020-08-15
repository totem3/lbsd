#![allow(dead_code)]

extern crate byteorder;
extern crate env_logger;
extern crate log;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::fmt;
use std::io::{self, BufRead, Read};
use std::path::Path;
use std::process::exit;

use log::trace;
use crate::tree::{BTreeNode, BTreeLeafNode};
use crate::table::{Table, Cursor};

pub mod tree;
pub mod table;

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
    TableNotGiven,
    UnrecognizedCommand,
}

struct MetaCommandArgs<'a> {
    input: &'a str,
    table: Option<&'a mut Table>,
}

fn do_meta_command(args: MetaCommandArgs) -> Result<(), MetaCommandResult> {
    match args.input {
        ".exit" => Err(MetaCommandResult::Exit),
        ".btree" => {
            show_btree(args.table)
        }
        ".constants" => {
            show_constants()
        }
        _ => Err(MetaCommandResult::UnrecognizedCommand),
    }
}

fn show_btree(table: Option<&mut Table>) -> Result<(), MetaCommandResult> {
    if let Some(table) = table {
        println!("Tree:");
        let page_num = table.root_page_num;
        let _ = show_btree_node(table, page_num, "");
        Ok(())
    } else {
        Err(MetaCommandResult::TableNotGiven)
    }
}

fn show_btree_node(table: &mut Table, page_num: usize, indent: &str) -> Result<(), MetaCommandResult> {
    let page = table.pager.get_page(page_num).clone();
    let values = if let Some(node) = page {
        match node {
            BTreeNode::Leaf(node) => {
                println!("{}leaf (size {})", indent, node.num_cells);
                for (i, key_value) in node.key_values.iter().enumerate() {
                    println!("{} - {} : {}", indent, i, key_value.key);
                }
                None
            }
            BTreeNode::Internal(node) => {
                println!("{}internal (size {})", indent, node.num_keys);
                Some((node.key_children.clone(), node.right_child))
            }
        }
    } else {
        return Ok(());
    };

    if let Some(values) = values {
        for kc in values.0 {
            let _ = show_btree_node(table, kc.child as usize, &(indent.to_owned() + "  "));
            println!("{} - key : {}", indent, kc.key);
        }
        let right_child = values.1;
        let _ = show_btree_node(table, right_child as usize, &(indent.to_owned() + "  "));
    }
    Ok(())
}

fn show_constants() -> Result<(), MetaCommandResult> {
    println!("Constants:");
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("NODE_HEADER_SIZE: {}", BTreeLeafNode::NODE_HEADER_SIZE);
    println!("NODE_CELL_SIZE: {}", BTreeLeafNode::NODE_CELL_SIZE);
    println!("NODE_SPACE_FOR_CELLS: {}", BTreeLeafNode::NODE_SPACE_FOR_CELLS);
    println!("NODE_MAX_CELLS: {}", BTreeLeafNode::NODE_MAX_CELLS);
    Ok(())
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

impl Default for Row {
    fn default() -> Self {
        Row { id: 0, username: [0; COLUMN_USERNAME_SIZE], email: [0; COLUMN_EMAIL_SIZE] }
    }
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
        let mut rdr = io::Cursor::new(input);
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
    InvalidStatement,
    PageMutFailure,
    PageNotFound,
    // RootNodeIsInternal,
    DuplicateKey,
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteResult> {
    trace!("execute_insert");
    let node = match table.pager.get_page(table.root_page_num) {
        Some(page) => page,
        None => { return Err(ExecuteResult::PageNotFound); }
    };
    match node {
        BTreeNode::Leaf(page) => {
            let num_cells = page.num_cells;
            trace!("execute_insert: num_cells: {}", num_cells);
            let row_to_insert = match &statement.row_to_insert {
                Some(s) => s,
                None => {
                    return Err(ExecuteResult::InvalidStatement);
                }
            };
            let is_max = page.is_max();
            let key_to_insert = row_to_insert.id;
            trace!("execute_insert: key_to_insert: {}", key_to_insert);
            let mut cursor = Cursor::find_insert_position(table, table.root_page_num, key_to_insert);
            trace!("execute_insert: cursor.cell_num: {}", cursor.cell_num);
            let cell_num = cursor.cell_num;

            if is_max {
                log::debug!("table is full");
                if let Some(root) = cursor.split_and_insert(key_to_insert, row_to_insert.clone()) {
                    cursor.table.root_page_num = root;
                }
                return Ok(());
            }
            match cursor.get_page_mut() {
                Some(BTreeNode::Leaf(page)) => {
                    if cell_num < num_cells.try_into().unwrap() {
                        let key_at_index = page.key_values[cell_num].key;
                        if key_at_index == key_to_insert {
                            return Err(ExecuteResult::DuplicateKey);
                        }
                    }
                    log::trace!("row inserted");
                    page.insert_at(cell_num, row_to_insert.id, row_to_insert.clone());
                }
                Some(BTreeNode::Internal(_)) => {}
                None => {
                    log::error!("cannot get mutable reference to page!");
                    return Err(ExecuteResult::PageMutFailure);
                }
            };
        }
        BTreeNode::Internal(_) => {}
    }
    Ok(())
}

fn execute_select(_statement: &Statement, table: &mut Table, w: &mut impl io::Write) -> Result<Vec<Row>, ExecuteResult> {
    trace!("execute_select");
    let mut cursor = Cursor::table_start(table);
    select_all(&mut cursor, w);
    Ok(vec![])
}

fn select_all(cursor: &mut Cursor, w: &mut impl io::Write) {
    trace!("select_all");
    while !cursor.end_of_table {
        match cursor.get_page() {
            Some(BTreeNode::Leaf(_)) => {
                trace!("select_all: node is leaf");
                if let Some(row) = cursor.get_row() {
                    let _ = writeln!(w, "{:?}", row);
                }
                cursor.advance();
            }
            Some(BTreeNode::Internal(page)) => {
                trace!("select_all: node is internal");
                let kc = page.key_children.first().expect("select_all: no children");
                trace!("select_all: key = {}", kc.key);
                cursor.page_num = kc.child as usize;
                cursor.cell_num = 0;
                trace!("select_all: page_num = {}", cursor.page_num);
                select_all(cursor, w);
            }
            None => {
                cursor.advance();
            }
        };
    }
}

// テストのため一時的にVec<Row>を返すようにしておく
fn execute_statement(statement: &Statement, table: &mut Table, w: &mut impl io::Write) -> Result<Vec<Row>, ExecuteResult> {
    match statement.st_type {
        StatementType::Insert => {
            // テストのためselectでRowsを返したいので一時的に合わせておく
            match execute_insert(statement, table) {
                Ok(_) => Ok(vec![]),
                Err(e) => Err(e),
            }
        }
        StatementType::Select => {
            execute_select(statement, table, w)
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
                    let args = MetaCommandArgs {
                        input: &input_buffer.buffer,
                        table: Some(&mut table),
                    };
                    match do_meta_command(args) {
                        Ok(_) => {}
                        Err(MetaCommandResult::Exit) => {
                            match table.close() {
                                Ok(_) => {}
                                Err(e) => {
                                    log::error!("failed to close db: {}", e);
                                }
                            };
                            return 0;
                        }
                        Err(MetaCommandResult::UnrecognizedCommand) => {
                            let _ = writeln!(w, "Unrecognized command '{}'", &input_buffer.buffer);
                        }
                        Err(MetaCommandResult::TableNotGiven) => {
                            let _ = writeln!(w, "called meta command '{}' that requires table, but table not given", &input_buffer.buffer);
                        }
                    }
                    continue;
                }
                let statement = prepare_statement(&input_buffer);
                match statement {
                    Ok(statement) => {
                        match execute_statement(&statement, &mut table, w) {
                            Ok(rows) => {
                                if !rows.is_empty() {
                                    for row in rows {
                                        let _ = writeln!(w, "{:?}", row);
                                    }
                                }
                                let _ = writeln!(w, "Executed");
                            }
                            Err(ExecuteResult::InvalidStatement) => {
                                let _ = writeln!(w, "invalid statement");
                                break;
                            }
                            Err(ExecuteResult::PageMutFailure) => {
                                let _ = writeln!(w, "get page mutable ref failed");
                                break;
                            }
                            Err(ExecuteResult::PageNotFound) => {
                                let _ = writeln!(w, "page not found error");
                                break;
                            }
                            Err(ExecuteResult::DuplicateKey) => {
                                let _ = writeln!(w, "duplicate key error");
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
    use crate::table::Pager;
    use crate::tree::KV;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_unrecognized_meta_command() {
        init();
        let args = MetaCommandArgs { input: ".foo", table: None };
        let res = do_meta_command(args);
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
        let mut pager = Pager::new("tmp/test.db").unwrap();
        if let Some(BTreeNode::Leaf(page)) = pager.get_page_mut(0) {
            page.num_cells = BTreeLeafNode::NODE_MAX_CELLS as u32;
            page.key_values = vec![
                KV{key:0,value:Row::default()},
                KV{key:1,value:Row::default()},
                KV{key:2,value:Row::default()},
                KV{key:3,value:Row::default()},
                KV{key:4,value:Row::default()},
                KV{key:5,value:Row::default()},
                KV{key:6,value:Row::default()},
            ];
        }
        let mut table = Table {
            pager,
            root_page_num: 0,
        };
        let mut stmt = Statement::new(StatementType::Insert);
        stmt.row_to_insert = Some(Row::default());
        let mut buf = vec![];
        let result = execute_statement(&stmt, &mut table, &mut buf);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_statement_insert_without_row() {
        let mut table = Table {
            pager: Pager::new("tmp/test.db").unwrap(),
            root_page_num: 0,
        };
        let stmt = Statement::new(StatementType::Insert);
        let mut buf = vec![];
        let result = execute_statement(&stmt, &mut table, &mut buf);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err, ExecuteResult::InvalidStatement);
    }

    #[test]
    fn test_execute_statement_insert() {
        init();
        let mut table = Table {
            pager: Pager::new("tmp/test.db").unwrap(),
            root_page_num: 0,
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
        let mut buf = vec![];
        let result = execute_statement(&stmt, &mut table, &mut buf);
        assert!(result.is_ok());
        let mut expected = Vec::with_capacity(ROW_SIZE);
        row.serialize(&mut expected);
        let mut buf = vec![];
        match table.pager.get_page(0) {
            Some(BTreeNode::Leaf(leaf)) => {
                leaf.key_values.first().and_then(|kv| {
                    kv.value.serialize(&mut buf);
                    Some(())
                });
            }
            Some(BTreeNode::Internal(_)) => { unimplemented!() }
            None => {}
        };
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_execute_select() {
        let mut table = Table::new("tmp/test.db").unwrap();
        let stmt = Statement::new(StatementType::Select);
        let mut buf = vec![];
        let result = execute_statement(&stmt, &mut table, &mut buf);
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
        let mut buf = vec![];
        let result = execute_statement(&stmt, &mut table, &mut buf);
        assert!(result.is_ok());

        let stmt = Statement::new(StatementType::Select);
        let mut buf = vec![];
        let result = execute_statement(&stmt, &mut table, &mut buf);
        assert!(result.is_ok());
    }
}
