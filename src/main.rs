extern crate log;

use std::io::{self, Write};
use std::process::exit;

use log::trace;

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
    UnrecognizedCommand,
}

fn do_meta_command(input: &str) -> Result<(), MetaCommandResult> {
    match input {
        ".exit" => exit(0),
        _ => return Err(MetaCommandResult::UnrecognizedCommand),
    }
}

#[test]
fn test_unrecognized_meta_command() {
    let res = do_meta_command(".foo");
    let err = res.err().unwrap();
    assert_eq!(err, MetaCommandResult::UnrecognizedCommand);
}

#[derive(Debug)]
struct Statement {
    st_type: StatementType,
}

impl Statement {
    fn new(st_type: StatementType) -> Self {
        return Statement { st_type };
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
}

fn prepare_statement(input: &InputBuffer) -> Result<Statement, PrepareError> {
    let lower = input.buffer.to_lowercase();
    if lower.starts_with("insert") {
        let statement = Statement::new(StatementType::Insert);
        return Ok(statement);
    }
    if lower.starts_with("select") {
        let statement = Statement::new(StatementType::Select);
        return Ok(statement);
    }
    return Err(PrepareError::UnrecognizedStatement);
}

#[test]
fn test_prepare_statement_insert() {
    let input = InputBuffer {
        buffer: "insert".to_string(),
    };
    let stmt = prepare_statement(&input).unwrap();
    assert_eq!(stmt.st_type, StatementType::Insert);
}

#[test]
fn test_prepare_statement_large_insert() {
    let input = InputBuffer {
        buffer: "INSERT".to_string(),
    };
    let stmt = prepare_statement(&input).unwrap();
    assert_eq!(stmt.st_type, StatementType::Insert);
}

#[test]
fn test_prepare_statement_select() {
    let input = InputBuffer {
        buffer: "select".to_string(),
    };
    let stmt = prepare_statement(&input).unwrap();
    assert_eq!(stmt.st_type, StatementType::Select);
}

#[test]
fn test_prepare_statement_large_select() {
    let input = InputBuffer {
        buffer: "SELECT".to_string(),
    };
    let stmt = prepare_statement(&input).unwrap();
    assert_eq!(stmt.st_type, StatementType::Select);
}

#[test]
fn test_prepare_statement_unknown() {
    let input = InputBuffer {
        buffer: "HOGE".to_string(),
    };
    let stmt = prepare_statement(&input);
    assert!(stmt.is_err());
    let err = stmt.err().unwrap();
    assert_eq!(err, PrepareError::UnrecognizedStatement);
}

fn execute_statement(statement: &Statement) {
    match statement.st_type {
        StatementType::Insert => {
            println!("this is where we would do insert");
        }
        StatementType::Select => {
            println!("this is where we would do select");
        }
    }
}

fn main() {
    let mut input_buffer = InputBuffer::new();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    loop {
        print_prompt();
        match input_buffer.read_line(&mut stdin) {
            Ok(n) => {
                trace!("read {} bytes", n);
                if input_buffer.buffer.starts_with(".") {
                    match do_meta_command(&input_buffer.buffer) {
                        Ok(_) => {}
                        Err(MetaCommandResult::UnrecognizedCommand) => {
                            println!("Unrecognized command '{}'", &input_buffer.buffer);
                        }
                    }
                    continue;
                }
                let statement = prepare_statement(&input_buffer);
                match statement {
                    Ok(statement) => {
                        execute_statement(&statement);
                    }
                    Err(PrepareError::UnrecognizedStatement) => {
                        println!(
                            "Unrecognized keyword at start of '{}'",
                            &input_buffer.buffer
                        );
                        continue;
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
                exit(1);
            }
        }
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}
