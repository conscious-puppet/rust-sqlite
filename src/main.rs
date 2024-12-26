use std::{
    collections::HashMap,
    fmt,
    io::{self, Write},
    process,
};

type InputBuffer = String;

#[derive(Debug)]
pub struct SqliteErr {
    _type: SqliteErrType,
    message: String,
}

impl SqliteErr {
    pub fn new(_type: SqliteErrType, message: String) -> Self {
        Self { _type, message }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SqliteErrType {
    UnrecognizedCommand,
    PrepareSyntaxError,
    PrepareStringTooLong,
    PrepareNegativeId,
    ExecuteTableFull,
}

impl fmt::Display for SqliteErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
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
                    return Err(SqliteErr::new(
                        SqliteErrType::PrepareSyntaxError,
                        "Syntax error: Could not parse statement.".to_owned(),
                    ));
                }

                let Ok(id) = args[0].parse::<i32>() else {
                    return Err(SqliteErr::new(
                        SqliteErrType::PrepareSyntaxError,
                        "Syntax error: Could not parse statement.".to_owned(),
                    ));
                };

                let username = args[1].to_owned();
                let email = args[2].to_owned();

                let row = Row::new(id, username, email)?;
                let statement = Statement::Insert(row);
                Ok(statement)
            }
            _ => Err(SqliteErr::new(
                SqliteErrType::UnrecognizedCommand,
                format!("Unrecognized keyword at start of '{}'.", input_buffer),
            )),
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
    id: i32,
    username: String,
    email: String,
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.username, self.email)
    }
}

impl Row {
    pub fn new(id: i32, mut username: String, mut email: String) -> Result<Self, SqliteErr> {
        if id < 0 {
            return Err(SqliteErr::new(
                SqliteErrType::PrepareNegativeId,
                "ID must be positive.".to_owned(),
            ));
        }

        if username.len() > COLUMN_USERNAME_SIZE {
            return Err(SqliteErr::new(
                SqliteErrType::PrepareStringTooLong,
                "String is too long.".to_owned(),
            ));
        }

        if email.len() > COLUMN_EMAIL_SIZE {
            return Err(SqliteErr::new(
                SqliteErrType::PrepareStringTooLong,
                "String is too long.".to_owned(),
            ));
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

const ID_SIZE: usize = size_of::<i32>();
const USERNAME_SIZE: usize = size_of::<String>() + COLUMN_USERNAME_SIZE * size_of::<char>();
const EMAIL_SIZE: usize = size_of::<String>() + COLUMN_EMAIL_SIZE * size_of::<char>();
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    num_rows: usize,
    pages: HashMap<usize, Vec<Row>>,
}

impl Table {
    pub fn new() -> Self {
        Self {
            num_rows: 0,
            pages: HashMap::with_capacity(TABLE_MAX_PAGES),
        }
    }

    pub fn row_slot(&mut self, row_num: usize) -> Option<&Row> {
        let page_num = row_num / ROWS_PER_PAGE;

        let page = self
            .pages
            .entry(page_num)
            .or_insert(Vec::with_capacity(ROWS_PER_PAGE));

        let row_offset = row_num % ROWS_PER_PAGE;

        page.get(row_offset)
    }

    pub fn execute_select(&self) -> Result<(), SqliteErr> {
        for (_, page) in &self.pages {
            for row in page {
                println!("{row}");
            }
        }
        Ok(())
    }

    pub fn execute_insert(&mut self, row: Row) -> Result<(), SqliteErr> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(SqliteErr::new(
                SqliteErrType::ExecuteTableFull,
                "Error: Table full.".to_owned(),
            ));
        }

        let page_num = self.num_rows / ROWS_PER_PAGE;

        let page = self
            .pages
            .entry(page_num)
            .or_insert(Vec::with_capacity(ROWS_PER_PAGE));

        page.push(row);

        self.num_rows += 1;

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
                Err(SqliteErr { _type, .. }) if _type == SqliteErrType::PrepareSyntaxError => {
                    println!("Unrecognized command '{}'.", input_buffer);
                    continue;
                }
                Err(err) => {
                    println!("{}", err);
                    continue;
                }
            }
        }

        let statement = match Statement::prepare_statement(&input_buffer) {
            Ok(statement) => statement,
            Err(err) => {
                println!("{}", err);
                continue;
            }
        };

        match statement.execute_statement(&mut table) {
            Ok(()) => println!("Executed."),
            Err(err) => println!("{}", err),
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
        Err(SqliteErr::new(
            SqliteErrType::UnrecognizedCommand,
            format!("Unrecognized keyword at start of '{}'.", input_buffer),
        ))
    }
}
