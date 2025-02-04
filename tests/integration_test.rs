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

#[test]
#[should_panic]
fn print_error_when_row_is_full() {
    let mut input: Vec<_> = (0..=1937)
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
fn keeps_data_after_closing_connection2() {
    let tempfile = TempFile::new();

    let mut input: Vec<_> = (1..=15)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push(".exit".to_owned());

    let output = spawn_rust_sqlite(&tempfile, input);
    let expected_output = vec!["db > Executed.".to_owned(), "db > ".to_owned()];
    assert_eq!(output[14..], expected_output);

    let input = vec!["select".to_owned(), ".exit".to_owned()];
    let output = spawn_rust_sqlite(&tempfile, input);

    let mut expected_output: Vec<_> = (2..=15)
        .map(|i| format!("({i}, user{i}, person{i}@example.com)"))
        .collect();
    expected_output.insert(0, "db > (1, user1, person1@example.com)".to_owned());
    expected_output.push("Executed.".to_owned());
    expected_output.push("db > ".to_owned());

    assert_eq!(output, expected_output);
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
        "LEAF_NODE_HEADER_SIZE: 14".to_owned(),
        "LEAF_NODE_CELL_SIZE: 295".to_owned(),
        "LEAF_NODE_SPACE_FOR_CELLS: 4082".to_owned(),
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
fn prints_all_rows_in_a_multi_level_tree() {
    let tempfile = TempFile::new();

    let mut input: Vec<_> = (1..=15)
        .map(|i| format!("insert {i} user{i} person{i}@example.com"))
        .collect();
    input.push("select".to_owned());
    input.push(".exit".to_owned());

    let output = spawn_rust_sqlite(&tempfile, input);
    let mut expected_output: Vec<_> = (2..=15)
        .map(|i| format!("({i}, user{i}, person{i}@example.com)"))
        .collect();
    expected_output.insert(0, "db > (1, user1, person1@example.com)".to_owned());
    expected_output.push("Executed.".to_owned());
    expected_output.push("db > ".to_owned());

    assert_eq!(output[15..], expected_output);
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

#[test]
fn allows_printing_out_the_structure_of_a_4_leaf_node_btree() {
    let tempfile = TempFile::new();

    let input = vec![
        "insert 18 user18 person18@example.com".to_owned(),
        "insert 7 user7 person7@example.com".to_owned(),
        "insert 10 user10 person10@example.com".to_owned(),
        "insert 29 user29 person29@example.com".to_owned(),
        "insert 23 user23 person23@example.com".to_owned(),
        "insert 4 user4 person4@example.com".to_owned(),
        "insert 14 user14 person14@example.com".to_owned(),
        "insert 30 user30 person30@example.com".to_owned(),
        "insert 15 user15 person15@example.com".to_owned(),
        "insert 26 user26 person26@example.com".to_owned(),
        "insert 22 user22 person22@example.com".to_owned(),
        "insert 19 user19 person19@example.com".to_owned(),
        "insert 2 user2 person2@example.com".to_owned(),
        "insert 1 user1 person1@example.com".to_owned(),
        "insert 21 user21 person21@example.com".to_owned(),
        "insert 11 user11 person11@example.com".to_owned(),
        "insert 6 user6 person6@example.com".to_owned(),
        "insert 20 user20 person20@example.com".to_owned(),
        "insert 5 user5 person5@example.com".to_owned(),
        "insert 8 user8 person8@example.com".to_owned(),
        "insert 9 user9 person9@example.com".to_owned(),
        "insert 3 user3 person3@example.com".to_owned(),
        "insert 12 user12 person12@example.com".to_owned(),
        "insert 27 user27 person27@example.com".to_owned(),
        "insert 17 user17 person17@example.com".to_owned(),
        "insert 16 user16 person16@example.com".to_owned(),
        "insert 13 user13 person13@example.com".to_owned(),
        "insert 24 user24 person24@example.com".to_owned(),
        "insert 25 user25 person25@example.com".to_owned(),
        "insert 28 user28 person28@example.com".to_owned(),
        ".btree".to_owned(),
        ".exit".to_owned(),
    ];

    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > Tree:".to_owned(),
        "- internal (size 3)".to_owned(),
        " - leaf (size 7)".to_owned(),
        "  - 1".to_owned(),
        "  - 2".to_owned(),
        "  - 3".to_owned(),
        "  - 4".to_owned(),
        "  - 5".to_owned(),
        "  - 6".to_owned(),
        "  - 7".to_owned(),
        " - key 7".to_owned(),
        " - leaf (size 8)".to_owned(),
        "  - 8".to_owned(),
        "  - 9".to_owned(),
        "  - 10".to_owned(),
        "  - 11".to_owned(),
        "  - 12".to_owned(),
        "  - 13".to_owned(),
        "  - 14".to_owned(),
        "  - 15".to_owned(),
        " - key 15".to_owned(),
        " - leaf (size 7)".to_owned(),
        "  - 16".to_owned(),
        "  - 17".to_owned(),
        "  - 18".to_owned(),
        "  - 19".to_owned(),
        "  - 20".to_owned(),
        "  - 21".to_owned(),
        "  - 22".to_owned(),
        " - key 22".to_owned(),
        " - leaf (size 8)".to_owned(),
        "  - 23".to_owned(),
        "  - 24".to_owned(),
        "  - 25".to_owned(),
        "  - 26".to_owned(),
        "  - 27".to_owned(),
        "  - 28".to_owned(),
        "  - 29".to_owned(),
        "  - 30".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output[30..], expected_output);
}

#[test]
fn allows_printing_out_the_structure_of_a_7_leaf_node_btree() {
    let tempfile = TempFile::new();

    let input = vec![
        "insert 58 user58 person58@example.com".to_owned(),
        "insert 56 user56 person56@example.com".to_owned(),
        "insert 8 user8 person8@example.com".to_owned(),
        "insert 54 user54 person54@example.com".to_owned(),
        "insert 77 user77 person77@example.com".to_owned(),
        "insert 7 user7 person7@example.com".to_owned(),
        "insert 25 user25 person25@example.com".to_owned(),
        "insert 71 user71 person71@example.com".to_owned(),
        "insert 13 user13 person13@example.com".to_owned(),
        "insert 22 user22 person22@example.com".to_owned(),
        "insert 53 user53 person53@example.com".to_owned(),
        "insert 51 user51 person51@example.com".to_owned(),
        "insert 59 user59 person59@example.com".to_owned(),
        "insert 32 user32 person32@example.com".to_owned(),
        "insert 36 user36 person36@example.com".to_owned(),
        "insert 79 user79 person79@example.com".to_owned(),
        "insert 10 user10 person10@example.com".to_owned(),
        "insert 33 user33 person33@example.com".to_owned(),
        "insert 20 user20 person20@example.com".to_owned(),
        "insert 4 user4 person4@example.com".to_owned(),
        "insert 35 user35 person35@example.com".to_owned(),
        "insert 76 user76 person76@example.com".to_owned(),
        "insert 49 user49 person49@example.com".to_owned(),
        "insert 24 user24 person24@example.com".to_owned(),
        "insert 70 user70 person70@example.com".to_owned(),
        "insert 48 user48 person48@example.com".to_owned(),
        "insert 39 user39 person39@example.com".to_owned(),
        "insert 15 user15 person15@example.com".to_owned(),
        "insert 47 user47 person47@example.com".to_owned(),
        "insert 30 user30 person30@example.com".to_owned(),
        "insert 86 user86 person86@example.com".to_owned(),
        "insert 31 user31 person31@example.com".to_owned(),
        "insert 68 user68 person68@example.com".to_owned(),
        "insert 37 user37 person37@example.com".to_owned(),
        "insert 66 user66 person66@example.com".to_owned(),
        "insert 63 user63 person63@example.com".to_owned(),
        "insert 40 user40 person40@example.com".to_owned(),
        "insert 78 user78 person78@example.com".to_owned(),
        "insert 19 user19 person19@example.com".to_owned(),
        "insert 46 user46 person46@example.com".to_owned(),
        "insert 14 user14 person14@example.com".to_owned(),
        "insert 81 user81 person81@example.com".to_owned(),
        "insert 72 user72 person72@example.com".to_owned(),
        "insert 6 user6 person6@example.com".to_owned(),
        "insert 50 user50 person50@example.com".to_owned(),
        "insert 85 user85 person85@example.com".to_owned(),
        "insert 67 user67 person67@example.com".to_owned(),
        "insert 2 user2 person2@example.com".to_owned(),
        "insert 55 user55 person55@example.com".to_owned(),
        "insert 69 user69 person69@example.com".to_owned(),
        "insert 5 user5 person5@example.com".to_owned(),
        "insert 65 user65 person65@example.com".to_owned(),
        "insert 52 user52 person52@example.com".to_owned(),
        "insert 1 user1 person1@example.com".to_owned(),
        "insert 29 user29 person29@example.com".to_owned(),
        "insert 9 user9 person9@example.com".to_owned(),
        "insert 43 user43 person43@example.com".to_owned(),
        "insert 75 user75 person75@example.com".to_owned(),
        "insert 21 user21 person21@example.com".to_owned(),
        "insert 82 user82 person82@example.com".to_owned(),
        "insert 12 user12 person12@example.com".to_owned(),
        "insert 18 user18 person18@example.com".to_owned(),
        "insert 60 user60 person60@example.com".to_owned(),
        "insert 44 user44 person44@example.com".to_owned(),
        ".btree".to_owned(),
        ".exit".to_owned(),
    ];

    let output = spawn_rust_sqlite(&tempfile, input);

    let expected_output = vec![
        "db > Tree:".to_owned(),
        "- internal (size 1)".to_owned(),
        " - internal (size 3)".to_owned(),
        "  - leaf (size 7)".to_owned(),
        "   - 1".to_owned(),
        "   - 2".to_owned(),
        "   - 4".to_owned(),
        "   - 5".to_owned(),
        "   - 6".to_owned(),
        "   - 7".to_owned(),
        "   - 8".to_owned(),
        "  - key 8".to_owned(),
        "  - leaf (size 11)".to_owned(),
        "   - 9".to_owned(),
        "   - 10".to_owned(),
        "   - 12".to_owned(),
        "   - 13".to_owned(),
        "   - 14".to_owned(),
        "   - 15".to_owned(),
        "   - 18".to_owned(),
        "   - 19".to_owned(),
        "   - 20".to_owned(),
        "   - 21".to_owned(),
        "   - 22".to_owned(),
        "  - key 22".to_owned(),
        "  - leaf (size 8)".to_owned(),
        "   - 24".to_owned(),
        "   - 25".to_owned(),
        "   - 29".to_owned(),
        "   - 30".to_owned(),
        "   - 31".to_owned(),
        "   - 32".to_owned(),
        "   - 33".to_owned(),
        "   - 35".to_owned(),
        "  - key 35".to_owned(),
        "  - leaf (size 12)".to_owned(),
        "   - 36".to_owned(),
        "   - 37".to_owned(),
        "   - 39".to_owned(),
        "   - 40".to_owned(),
        "   - 43".to_owned(),
        "   - 44".to_owned(),
        "   - 46".to_owned(),
        "   - 47".to_owned(),
        "   - 48".to_owned(),
        "   - 49".to_owned(),
        "   - 50".to_owned(),
        "   - 51".to_owned(),
        " - key 51".to_owned(),
        " - internal (size 3)".to_owned(),
        "  - leaf (size 12)".to_owned(),
        "   - 36".to_owned(),
        "   - 37".to_owned(),
        "   - 39".to_owned(),
        "   - 40".to_owned(),
        "   - 43".to_owned(),
        "   - 44".to_owned(),
        "   - 46".to_owned(),
        "   - 47".to_owned(),
        "   - 48".to_owned(),
        "   - 49".to_owned(),
        "   - 50".to_owned(),
        "   - 51".to_owned(),
        "  - key 51".to_owned(),
        "  - leaf (size 11)".to_owned(),
        "   - 52".to_owned(),
        "   - 53".to_owned(),
        "   - 54".to_owned(),
        "   - 55".to_owned(),
        "   - 56".to_owned(),
        "   - 58".to_owned(),
        "   - 59".to_owned(),
        "   - 60".to_owned(),
        "   - 63".to_owned(),
        "   - 65".to_owned(),
        "   - 66".to_owned(),
        "  - key 66".to_owned(),
        "  - leaf (size 7)".to_owned(),
        "   - 67".to_owned(),
        "   - 68".to_owned(),
        "   - 69".to_owned(),
        "   - 70".to_owned(),
        "   - 71".to_owned(),
        "   - 72".to_owned(),
        "   - 75".to_owned(),
        "  - key 75".to_owned(),
        "  - leaf (size 8)".to_owned(),
        "   - 76".to_owned(),
        "   - 77".to_owned(),
        "   - 78".to_owned(),
        "   - 79".to_owned(),
        "   - 81".to_owned(),
        "   - 82".to_owned(),
        "   - 85".to_owned(),
        "   - 86".to_owned(),
        "db > ".to_owned(),
    ];
    assert_eq!(output[64..], expected_output);
}

fn spawn_rust_sqlite(tempfile: &TempFile, input: Vec<String>) -> Vec<String> {
    let mut process = rust_sqlite_exe()
        .arg(&tempfile.filepath)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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
