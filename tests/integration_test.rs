use std::env;
use std::io::Write;
use std::process::{Command, Stdio};
use std::str;

#[test]
fn insert_and_retrieve_row() {
    let input = vec![
        "insert 1 user1 person1@example.com".to_owned(),
        "select".to_owned(),
        ".exit".to_owned(),
    ];

    let tempfile = TempFile::new();
    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > Executed.".to_owned(),
        "db > (1, user1, person1@example.com)".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ];

    assert_eq!(output, expected_output);
}

// #[test]
// FIXME: the test is failing because TABLE_FULL error has been removed.
fn _print_error_when_row_is_full() {
    let mut input: Vec<_> = (0..=1400)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(".exit".to_owned());

    let tempfile = TempFile::new();
    let output = spawn_rust_sqlite(&tempfile, input);
    let output = &output[output.len() - 2];

    let expected_output = "db > Error: Table full.";

    assert_eq!(output, expected_output);
}

#[test]
fn allow_inserting_string_at_maximum_length() {
    let username = ['a'; 32].iter().cloned().collect::<String>();
    let email = ['a'; 255].iter().cloned().collect::<String>();
    let input = vec![
        format!("insert 1 {username} {email}"),
        "select".to_owned(),
        ".exit".to_owned(),
    ];

    let tempfile = TempFile::new();
    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > Executed.".to_owned(),
        format!("db > (1, {username}, {email})"),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ];

    assert_eq!(output, expected_output);
}

#[test]
fn prints_error_message_if_string_are_too_long() {
    let username = ['a'; 33].iter().cloned().collect::<String>();
    let email = ['a'; 255].iter().cloned().collect::<String>();
    let input = vec![
        format!("insert 1 {username} {email}"),
        "select".to_owned(),
        ".exit".to_owned(),
    ];

    let tempfile = TempFile::new();
    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > String is too long.".to_owned(),
        "db > Executed.".to_owned(),
        "db > ".to_owned(),
    ];

    assert_eq!(output, expected_output);

    let username = ['a'; 32].iter().cloned().collect::<String>();
    let email = ['a'; 256].iter().cloned().collect::<String>();
    let input = vec![
        format!("insert 1 {username} {email}"),
        "select".to_owned(),
        ".exit".to_owned(),
    ];

    let tempfile = TempFile::new();
    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > String is too long.".to_owned(),
        "db > Executed.".to_owned(),
        "db > ".to_owned(),
    ];

    assert_eq!(output, expected_output);
}

#[test]
fn prints_error_message_if_id_is_negative() {
    let input = vec![
        "insert -1 foo bar@email.com".to_owned(),
        "select".to_owned(),
        ".exit".to_owned(),
    ];

    let tempfile = TempFile::new();
    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > ID is invalid.".to_owned(),
        "db > Executed.".to_owned(),
        "db > ".to_owned(),
    ];

    assert_eq!(output, expected_output);
}

#[test]
fn keeps_data_after_closing_connection() {
    let tempfile = TempFile::new();

    let input = vec![
        "insert 1 user1 person1@example.com".to_owned(),
        ".exit".to_owned(),
    ];
    let output = spawn_rust_sqlite(&tempfile, input);
    let expected_output = vec!["db > Executed.".to_owned(), "db > ".to_owned()];
    assert_eq!(output, expected_output);

    let input = vec!["select".to_owned(), ".exit".to_owned()];
    let output = spawn_rust_sqlite(&tempfile, input);
    let expected_output = vec![
        "db > (1, user1, person1@example.com)".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output, expected_output);
    drop(tempfile);
}

#[test]
fn prints_constants() {
    let tempfile = TempFile::new();

    let input = vec![".constants".to_owned(), ".exit".to_owned()];
    let output = spawn_rust_sqlite(&tempfile, input);
    let expected_output = vec![
        "db > Constants:".to_owned(),
        "ROW_SIZE: 291".to_owned(),
        "COMMON_NODE_HEADER_SIZE: 6".to_owned(),
        "LEAF_NODE_HEADER_SIZE: 10".to_owned(),
        "LEAF_NODE_CELL_SIZE: 295".to_owned(),
        "LEAF_NODE_SPACE_FOR_CELLS: 4086".to_owned(),
        "LEAF_NODE_MAX_CELLS: 13".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output, expected_output);
}

#[test]
fn allows_printing_out_the_structure_of_a_one_node_btree() {
    let tempfile = TempFile::new();

    let mut input: Vec<_> = [3, 1, 2]
        .iter()
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(".btree".to_owned());
    input.push(".exit".to_owned());
    let output = spawn_rust_sqlite(&tempfile, input);
    let expected_output = vec![
        "db > Executed.".to_owned(),
        "db > Executed.".to_owned(),
        "db > Executed.".to_owned(),
        "db > Tree:".to_owned(),
        "- leaf (size 3)".to_owned(),
        " - 1".to_owned(),
        " - 2".to_owned(),
        " - 3".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output, expected_output);
}

#[test]
fn allows_printing_out_the_structure_of_a_3_leaf_node_btree() {
    let tempfile = TempFile::new();

    let mut input: Vec<_> = (1..=14)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(".btree".to_owned());
    input.push("insert 15 user15 person15@example.com".to_owned());
    input.push(".exit".to_owned());

    let output = spawn_rust_sqlite(&tempfile, input);

    println!("output: {:?}", output);

    let expected_output = vec![
        "db > Tree:".to_owned(),
        "- internal (size 1)".to_owned(),
        " - leaf (size 7)".to_owned(),
        "  - 1".to_owned(),
        "  - 2".to_owned(),
        "  - 3".to_owned(),
        "  - 4".to_owned(),
        "  - 5".to_owned(),
        "  - 6".to_owned(),
        "  - 7".to_owned(),
        " - key 7".to_owned(),
        " - leaf (size 7)".to_owned(),
        "  - 8".to_owned(),
        "  - 9".to_owned(),
        "  - 10".to_owned(),
        "  - 11".to_owned(),
        "  - 12".to_owned(),
        "  - 13".to_owned(),
        "  - 14".to_owned(),
        "db > Executed.".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output[14..], expected_output);
}

#[test]
fn prints_an_error_message_if_there_is_a_duplicate_id() {
    let tempfile = TempFile::new();

    let input = vec![
        "insert 1 user1 person1@example.com".to_owned(),
        "insert 1 user1 person1@example.com".to_owned(),
        "select".to_owned(),
        ".exit".to_owned(),
    ];
    let output = spawn_rust_sqlite(&tempfile, input);
    let expected_output = vec![
        "db > Executed.".to_owned(),
        "db > Error: Duplicate key.".to_owned(),
        "db > (1, user1, person1@example.com)".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output, expected_output);
}

fn spawn_rust_sqlite(tempfile: &TempFile, input: Vec<String>) -> Vec<String> {
    let mut process = rust_sqlite_exe()
        .arg(&tempfile.filepath)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Unable to start the process.");

    let stdin = process
        .stdin
        .as_mut()
        .expect("Unable to pipe stdin to process.");

    for line in input {
        stdin
            .write_all(format!("{}\n", line).as_bytes())
            .expect(&format!("Unable to write command `{}`", line));
    }

    let output = process
        .wait_with_output()
        .expect("Unable to get output from the process.");

    str::from_utf8(&output.stdout)
        .expect("Could not get process output.")
        .lines()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

// refer:
// https://github.com/rust-lang/cargo/blob/485670b3983b52289a2f353d589c57fae2f60f82/tests/testsuite/support/mod.rs#L507
// https://github.com/assert-rs/assert_cmd/blob/5036880699a8d01d56db132b81de84253e134166/src/cargo.rs#L206
fn rust_sqlite_exe() -> Command {
    let target_dir = env::current_exe()
        .ok()
        .map(|mut path| {
            path.pop();
            if path.ends_with("deps") {
                path.pop();
            }
            path
        })
        .expect("this should only be used where a `current_exe` can be set");
    let rust_sqlite_exe = target_dir.join(format!("{}{}", "rust-sqlite", env::consts::EXE_SUFFIX,));
    Command::new(rust_sqlite_exe)
}

struct TempFile {
    filepath: String,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Err(_) = std::fs::remove_file(&self.filepath) {
            println!("Could not delete the tempfile {}", self.filepath);
        }
    }
}

impl TempFile {
    pub fn new() -> Self {
        let tempdir = std::env::temp_dir();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Unable to create a random tempfile name.")
            .as_nanos();
        let filename = format!("file_{}.db", now);
        let filepath = tempdir
            .join(filename)
            .to_str()
            .expect("Unable to create a random tempfile name.")
            .to_owned();
        Self { filepath }
    }
}
