use std::borrow::Cow;
use std::fmt;

use crate::row::Row;
use crate::table::{Table, TABLE_MAX_ROWS};
use crate::InputBuffer;

pub enum Statement {
    Select,
    Insert(Row),
}

pub enum PrepareStatementErr<'a> {
    SyntaxError,
    StringTooLong,
    InvalidID,
    UnrecognizedStatement(Cow<'a, str>),
}

impl<'a> fmt::Display for PrepareStatementErr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            &PrepareStatementErr::SyntaxError => {
                write!(f, "Syntax error: Could not parse statement.")
            }
            &PrepareStatementErr::StringTooLong => {
                write!(f, "String is too long.")
            }
            &PrepareStatementErr::InvalidID => {
                write!(f, "ID is invalid.")
            }
            &PrepareStatementErr::UnrecognizedStatement(input_buffer) => {
                write!(f, "Unrecognized keyword at start of '{}'.", input_buffer)
            }
        }
    }
}

pub enum ExecuteErr {
    ExecuteTableFull,
}

impl fmt::Display for ExecuteErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            &ExecuteErr::ExecuteTableFull => {
                write!(f, "Error: Table full.")
            }
        }
    }
}

impl Statement {
    pub fn prepare_statement(input_buffer: &InputBuffer) -> Result<Self, PrepareStatementErr> {
        match input_buffer.to_lowercase() {
            buffer if buffer == "select" => {
                let statement = Statement::Select;
                Ok(statement)
            }
            buffer if buffer.starts_with("insert") => {
                let row = buffer[7..].parse::<Row>()?;
                let statement = Statement::Insert(row);
                Ok(statement)
            }
            _ => Err(PrepareStatementErr::UnrecognizedStatement(Cow::Borrowed(
                &input_buffer[..],
            ))),
        }
    }

    pub fn execute_statement(self, table: &mut Table) -> Result<(), ExecuteErr> {
        match self {
            Statement::Select => Self::execute_select(table),
            Statement::Insert(row) => Self::execute_insert(row, table),
        }
    }

    fn execute_select(table: &Table) -> Result<(), ExecuteErr> {
        print!("{table}");
        Ok(())
    }

    fn execute_insert(row: Row, table: &mut Table) -> Result<(), ExecuteErr> {
        if table.num_rows() >= TABLE_MAX_ROWS {
            return Err(ExecuteErr::ExecuteTableFull);
        }
        let row = row.serialize();
        table.put_row_slot(row);
        Ok(())
    }
}
