use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, Write},
    process,
};

type InputBuffer = String;

#[derive(Debug)]
pub enum SqliteErr {
    UnrecognizedCommand,
    PrepareSyntaxError,
    ExecuteTableFull,
    BufferOverflow,
}

pub enum Statement {
    Select,
    Insert(Row),
}

impl Statement {
    pub fn prepare_statement(input_buffer: &InputBuffer) -> Result<Self, SqliteErr> {
        match input_buffer.to_lowercase() {
            buffer if buffer == "select" => {
                let statement = Statement::Select;
                Ok(statement)
            }
            buffer if buffer.starts_with("insert") => {
                let args: Vec<_> = buffer[7..].split_ascii_whitespace().collect();
                if args.len() < 3 {
                    return Err(SqliteErr::PrepareSyntaxError);
                }

                let Ok(id) = args[0].parse::<usize>() else {
                    return Err(SqliteErr::PrepareSyntaxError);
                };

                let username = args[1].to_owned();
                let email = args[2].to_owned();

                let row = Row::new(id, username, email)?;
                let statement = Statement::Insert(row);
                Ok(statement)
            }
            _ => Err(SqliteErr::UnrecognizedCommand),
        }
    }

    fn execute_statement(self, table: &mut Table) -> Result<(), SqliteErr> {
        match self {
            Statement::Select => table.execute_select(),
            Statement::Insert(row) => table.execute_insert(row),
        }
    }
}

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

pub struct Row {
    id: usize,
    username: String,
    email: String,
}

impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.username, self.email)
    }
}

impl Row {
    pub fn new(id: usize, mut username: String, mut email: String) -> Result<Self, SqliteErr> {
        if username.len() > COLUMN_USERNAME_SIZE {
            return Err(SqliteErr::BufferOverflow);
        }

        if email.len() > COLUMN_EMAIL_SIZE {
            return Err(SqliteErr::BufferOverflow);
        }

        username.shrink_to(COLUMN_USERNAME_SIZE);
        email.shrink_to(COLUMN_EMAIL_SIZE);

        Ok(Self {
            id,
            username,
            email,
        })
    }
}

const ID_SIZE: usize = size_of::<usize>();
const USERNAME_SIZE: usize = size_of::<String>() + COLUMN_USERNAME_SIZE * size_of::<char>();
const EMAIL_SIZE: usize = size_of::<String>() + COLUMN_EMAIL_SIZE * size_of::<char>();
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table(HashMap<usize, Vec<Row>>);

impl Table {
    pub fn new() -> Self {
        Self(HashMap::with_capacity(TABLE_MAX_PAGES))
    }

    pub fn row_slot(&mut self, row_num: usize) -> Option<&Row> {
        let page_num = row_num / ROWS_PER_PAGE;

        let page = self
            .0
            .entry(page_num)
            .or_insert(Vec::with_capacity(ROWS_PER_PAGE));

        let row_offset = row_num % ROWS_PER_PAGE;

        page.get(row_offset)
    }

    pub fn execute_select(&self) -> Result<(), SqliteErr> {
        for (_, page) in &self.0 {
            for row in page {
                println!("{row}");
            }
        }
        Ok(())
    }

    pub fn execute_insert(&mut self, row: Row) -> Result<(), SqliteErr> {
        if self.0.len() >= TABLE_MAX_ROWS {
            return Err(SqliteErr::ExecuteTableFull);
        }

        let page_num = self.0.len() / ROWS_PER_PAGE;

        let page = self
            .0
            .entry(page_num)
            .or_insert(Vec::with_capacity(ROWS_PER_PAGE));
        page.push(row);
        Ok(())
    }
}

fn main() {
    let mut table = Table::new();

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

        let statement = match Statement::prepare_statement(&input_buffer) {
            Ok(statement) => statement,
            Err(SqliteErr::PrepareSyntaxError) => {
                println!("Syntax error: Could not parse statement.");
                continue;
            }
            Err(SqliteErr::UnrecognizedCommand) => {
                println!("Unrecognized keyword at start of '{}'.", input_buffer);
                continue;
            }
            Err(err) => {
                println!("Unexpected error: {:?}", err);
                continue;
            }
        };

        match statement.execute_statement(&mut table) {
            Ok(()) => println!("Executed."),
            Err(SqliteErr::ExecuteTableFull) => println!("Error: Table full"),
            Err(err) => println!("Unexpected error: {:?}", err),
        }
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
