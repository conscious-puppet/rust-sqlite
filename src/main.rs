use std::{
    io::{self, Write},
    process,
};

type InputBuffer = String;

pub enum SqliteErr {
    UnrecognizedCommand,
}

pub enum StatementType {
    INSERT,
    SELECT,
}

pub struct Statement {
    _type: StatementType,
}

impl Statement {
    pub fn new(_type: StatementType) -> Self {
        Self { _type }
    }
}

fn main() {
    let mut input_buffer = InputBuffer::new();
    loop {
        print_prompt();
        read_input(&mut input_buffer);

        // TODO: implement parser combinator here, or regex instead of doing this
        let buffer: Vec<char> = input_buffer.chars().collect();

        if buffer[0] == '.' {
            match do_meta_command(&input_buffer) {
                Ok(_) => continue,
                Err(_) => {
                    println!("Unrecognized command '{}'.", input_buffer);
                    continue;
                }
            }
        }

        let statement = match prepare_statement(&input_buffer) {
            Ok(statement) => statement,
            Err(_) => {
                println!("Unrecognized keyword at start of '{}'.", input_buffer);
                continue;
            }
        };

        execute_statement(&statement);
        println!("Executed.");
    }
}

fn print_prompt() {
    print!("db > ");
    let _ = io::stdout().flush();
}

fn read_input(input_buffer: &mut InputBuffer) {
    input_buffer.clear();
    if let Err(_) = io::stdin().read_line(input_buffer) {
        panic!("Error while reading input");
    }

    // Ignore trailing newline
    *input_buffer = input_buffer.trim_end().to_owned();
}

fn do_meta_command(input_buffer: &InputBuffer) -> Result<(), SqliteErr> {
    if input_buffer == ".exit" {
        process::exit(0);
    } else {
        Err(SqliteErr::UnrecognizedCommand)
    }
}

fn prepare_statement(input_buffer: &InputBuffer) -> Result<Statement, SqliteErr> {
    match input_buffer.to_lowercase() {
        buffer if buffer.starts_with("select") => Ok(Statement::new(StatementType::SELECT)),
        buffer if buffer.starts_with("insert") => Ok(Statement::new(StatementType::INSERT)),
        _ => Err(SqliteErr::UnrecognizedCommand),
    }
}

fn execute_statement(statement: &Statement) {
    match statement._type {
        StatementType::INSERT => {
            println!("This is where we would do an insert.");
        }
        StatementType::SELECT => {
            println!("This is where we would do a select.");
        }
    }
}
