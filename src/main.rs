extern crate log;

use std::io::{self, Write};
use std::process::exit;

use log::trace;

struct InputBuffer {
    buffer: String,
    length: usize,
}

impl InputBuffer {
    fn new() -> Self {
        return InputBuffer {
            buffer: "".to_string(),
            length: 0,
        };
    }

    fn reset(&mut self) {
        self.buffer = "".to_string();
        self.length = 0;
    }

    fn read_line(&mut self, input: &mut dyn io::BufRead) -> io::Result<usize> {
        let mut buf = String::new();
        let size = input.read_line(&mut buf)?;
        self.buffer = buf.trim_end().to_string();
        return Ok(size);
    }
}

fn main() {
    let mut input_buffer = InputBuffer::new();
    let stdin = io::stdin();
    let mut stdin = stdin.lock();
    print_prompt();
    loop {
        match input_buffer.read_line(&mut stdin) {
            Ok(n) => {
                trace!("read {} bytes", n);
                if &input_buffer.buffer == ".exit" {
                    exit(0);
                } else {
                    print!("Unrecognized command {}.\n", input_buffer.buffer);
                }
                input_buffer.reset();
                print_prompt();
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
