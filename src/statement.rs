use std::borrow::Cow;
use std::fmt;

use crate::cursor::Cursor;
use crate::node::{LEAF_NODE_KEY_SIZE, LEAF_NODE_NUM_CELLS_SIZE};
use crate::row::Row;
use crate::table::Table;
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
    TableFull,
    DuplicateKey,
}

impl fmt::Display for ExecuteErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            &ExecuteErr::TableFull => {
                write!(f, "Error: Table full.")
            }
            &ExecuteErr::DuplicateKey => {
                write!(f, "Error: Duplicate key.")
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

    fn execute_select(table: &mut Table) -> Result<(), ExecuteErr> {
        let mut cursor = Cursor::table_start(table);
        while !cursor.end_of_table {
            let row = Row::deserialize(cursor.value());
            cursor.advance();
            println!("{}", row);
        }
        Ok(())
    }

    fn execute_insert(row: Row, table: &mut Table) -> Result<(), ExecuteErr> {
        let node = table.pager.get_page(table.root_page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        let key_to_insert = row.id;
        let mut cursor = Cursor::table_find(table, key_to_insert);
        let cell_num = cursor.cell_num;

        if cell_num < num_cells {
            let node = cursor.table.pager.get_page(cursor.table.root_page_num);

            let mut key_at_index_bytes = [0; LEAF_NODE_KEY_SIZE];
            key_at_index_bytes.copy_from_slice(node.leaf_node_key(cell_num));
            let key_at_index = u32::from_le_bytes(key_at_index_bytes);

            if key_at_index == key_to_insert {
                return Err(ExecuteErr::DuplicateKey);
            }
        }

        cursor.leaf_node_insert(row.id, row);
        Ok(())
    }
}
