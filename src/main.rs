use std::io::{self, Write};

use node::{
    NodeProxy, COMMON_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE,
    LEAF_NODE_MAX_CELLS, LEAF_NODE_SPACE_FOR_CELLS,
};
use row::ROW_SIZE;
use statement::Statement;
use table::Table;

pub mod cursor;
pub mod node;
pub mod pager;
pub mod row;
pub mod statement;
pub mod table;

type InputBuffer = String;

pub enum MetaCommandErr {
    UnrecognizedCommand,
}

pub enum MetaCommandOk {
    ExitSuccess,
    CommandSuccess,
}

pub struct ExitSuccess;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        panic!("Must supply a database filename.");
    }

    let mut table = Table::db_open(&args[1]);

    let mut input_buffer = InputBuffer::new();
    loop {
        print_prompt();
        read_input(&mut input_buffer);

        // TODO: implement parser combinator here, or regex instead of doing this
        let buffer: Vec<char> = input_buffer.chars().collect();

        if buffer[0] == '.' {
            match do_meta_command(&input_buffer, &mut table) {
                Ok(MetaCommandOk::ExitSuccess) => {
                    drop(table);
                    break;
                }
                Ok(MetaCommandOk::CommandSuccess) => {
                    continue;
                }
                Err(MetaCommandErr::UnrecognizedCommand) => {
                    println!("Unrecognized keyword at start of '{}'.", input_buffer);
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

fn do_meta_command(
    input_buffer: &InputBuffer,
    table: &mut Table,
) -> Result<MetaCommandOk, MetaCommandErr> {
    if input_buffer == ".exit" {
        Ok(MetaCommandOk::ExitSuccess)
    } else if input_buffer == ".constants" {
        println!("Constants:");
        print_constants();
        Ok(MetaCommandOk::CommandSuccess)
    } else if input_buffer == ".btree" {
        println!("Tree:");
        let node = table.pager.get_page(0);
        let node_proxy = NodeProxy::new(node);
        print!("{node_proxy}");
        Ok(MetaCommandOk::CommandSuccess)
    } else {
        Err(MetaCommandErr::UnrecognizedCommand)
    }
}

fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}
