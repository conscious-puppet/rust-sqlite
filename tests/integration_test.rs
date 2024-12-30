use std::env;
use std::io::Write;
use std::process::{Command, Stdio};
use std::str;

#[test]
fn insert_and_retrieve_row() {
    let input = Vec::from([
        "insert 1 user1 person1@example.com".to_owned(),
        "select".to_owned(),
        ".exit".to_owned(),
    ]);

    let output = spawn_rust_sqlite(input);

    let expected_output = Vec::from([
        "db > Executed.".to_owned(),
        "db > (1, user1, person1@example.com)".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ]);

    assert_eq!(output, expected_output);
}

#[test]
fn print_error_when_row_is_full() {
    let mut input: Vec<_> = (0..=1400)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(".exit".to_owned());

    let output = spawn_rust_sqlite(input);
    let output = &output[output.len() - 2];

    let expected_output = "db > Error: Table full.";

    assert_eq!(output, expected_output);
}

#[test]
fn allow_inserting_string_at_maximum_length() {
    let username = ['a'; 32].iter().cloned().collect::<String>();
    let email = ['a'; 255].iter().cloned().collect::<String>();
    let input = Vec::from([
        format!("insert 1 {username} {email}"),
        "select".to_owned(),
        ".exit".to_owned(),
    ]);

    let output = spawn_rust_sqlite(input);

    let expected_output = Vec::from([
        "db > Executed.".to_owned(),
        format!("db > (1, {username}, {email})"),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ]);

    assert_eq!(output, expected_output);
}

#[test]
fn prints_error_message_if_string_are_too_long() {
    let username = ['a'; 33].iter().cloned().collect::<String>();
    let email = ['a'; 255].iter().cloned().collect::<String>();
    let input = Vec::from([
        format!("insert 1 {username} {email}"),
        "select".to_owned(),
        ".exit".to_owned(),
    ]);

    let output = spawn_rust_sqlite(input);

    let expected_output = Vec::from([
        "db > String is too long.".to_owned(),
        "db > ".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ]);

    assert_eq!(output, expected_output);

    let username = ['a'; 32].iter().cloned().collect::<String>();
    let email = ['a'; 256].iter().cloned().collect::<String>();
    let input = Vec::from([
        format!("insert 1 {username} {email}"),
        "select".to_owned(),
        ".exit".to_owned(),
    ]);

    let output = spawn_rust_sqlite(input);

    let expected_output = Vec::from([
        "db > String is too long.".to_owned(),
        "db > ".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ]);

    assert_eq!(output, expected_output);
}

#[test]
fn prints_error_message_if_id_is_negative() {
    let input = Vec::from([
        "insert -1 foo bar@email.com".to_owned(),
        "select".to_owned(),
        ".exit".to_owned(),
    ]);

    let output = spawn_rust_sqlite(input);

    let expected_output = Vec::from([
        "db > ID is invalid.".to_owned(),
        "db > ".to_owned(),
        "Executed.".to_owned(),
        "db > ".to_owned(),
    ]);

    assert_eq!(output, expected_output);
}

fn spawn_rust_sqlite(input: Vec<String>) -> Vec<String> {
    let mut process = rust_sqlite_exe()
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
    let rust_sqlite_exe = target_dir.join(format!("{}{}", "rust-sqlite", env::consts::EXE_SUFFIX));
    Command::new(rust_sqlite_exe)
}
