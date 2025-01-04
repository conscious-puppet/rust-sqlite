use std::cell::RefCell;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

use crate::node::Node;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    file: File,
    file_length: u64,
    // TODO: is this required? can be derived from pages.len()
    pub num_pages: u32,
    pages: Vec<Node>,
}

impl Pager {
    pub fn pager_open(filename: &str) -> Self {
        let Ok(file) = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
        else {
            panic!("Unable to open file.");
        };

        let Ok(metadata) = file.metadata() else {
            panic!("Unable to get file metadata.");
        };

        let file_length = metadata.len();
        let num_pages = (file_length / PAGE_SIZE as u64) as u32;

        if file_length % PAGE_SIZE as u64 != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.")
        }

        let pages = Vec::new();

        Self {
            file,
            file_length,
            num_pages,
            pages,
        }
    }

    pub fn file_length(&self) -> u64 {
        self.file_length
    }

    fn validate_page_num(page_num: u32) {
        if page_num as usize > TABLE_MAX_PAGES {
            panic!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
        }
    }

    pub fn get_page(&mut self, page_num: u32) -> &mut Node {
        Self::validate_page_num(page_num);

        // Cache miss. Allocate memory and load from file.
        if self.pages.get(page_num as usize).is_none() {
            self.pages.push(Node::initialize_leaf_node());

            let page = &mut self.pages[page_num as usize];

            let mut num_pages = self.file_length / PAGE_SIZE as u64;

            // We might have partial page at the end of the file
            if self.file_length % PAGE_SIZE as u64 != 0 {
                num_pages += 1;
            }

            if page_num as u64 <= num_pages {
                let offset = page_num as usize * PAGE_SIZE;
                self.file
                    .seek(std::io::SeekFrom::Start(offset as u64))
                    .expect("Unable to seek file.");

                let mut buffer = [0; PAGE_SIZE];

                self.file
                    .read(&mut buffer)
                    .expect("Unable to read file to a buffer.");

                *page = Node::from_bytes(&buffer);
            }

            if page_num as u32 >= self.num_pages {
                self.num_pages = page_num + 1;
            }
        }

        &mut self.pages[page_num as usize]
    }

    pub fn pager_flush(&mut self, page_num: u32) {
        Self::validate_page_num(page_num);

        // Load page, if not loaded previously
        // TODO: should not be needed, we could skip flushing these pages
        let _ = self.get_page(page_num);

        let offset = page_num as usize * PAGE_SIZE;
        self.file
            .seek(std::io::SeekFrom::Start(offset as u64))
            .expect("Unable to seek file.");

        let page = &self.pages[page_num as usize];
        self.file
            .write(&page.to_bytes())
            .expect("Unable to write to file.");
    }

    // Until we start recycling free pages, new pages will always
    // go onto the end of the database file
    pub fn get_unused_page_num(&self) -> u32 {
        self.num_pages
    }
}

pub struct PagerProxy<'a>(RefCell<&'a mut Pager>);

impl<'a> PagerProxy<'a> {
    pub fn new(node: &'a mut Pager) -> Self {
        Self(RefCell::new(node))
    }
}

impl<'a> fmt::Display for PagerProxy<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn indent(f: &mut fmt::Formatter, level: usize) -> fmt::Result {
            for _ in 0..level {
                write!(f, " ")?;
            }
            Ok(())
        }

        fn print_tree<'a>(
            f: &mut fmt::Formatter,
            pager: &'a mut Pager,
            page_num: u32,
            indentation_level: usize,
        ) -> fmt::Result {
            let node = pager.get_page(page_num);

            match *node {
                Node::Leaf { num_cells, .. } => {
                    indent(f, indentation_level)?;
                    writeln!(f, "- leaf (size {num_cells})")?;

                    for i in 0..num_cells {
                        indent(f, indentation_level + 1)?;
                        let leaf_node_key = node.leaf_node_key(i);
                        writeln!(f, "- {leaf_node_key}")?;
                    }
                }
                Node::Internal {
                    num_keys,
                    right_child_pointer,
                    ..
                } => {
                    indent(f, indentation_level)?;
                    writeln!(f, "- internal (size {num_keys})")?;

                    for i in 0..num_keys {
                        let node = pager.get_page(page_num);
                        let child_page_num = *node.internal_node_child(i);
                        print_tree(f, pager, child_page_num, indentation_level + 1)?;

                        indent(f, indentation_level + 1)?;

                        let node = pager.get_page(page_num);
                        let internal_node_key = node.internal_node_key(i);
                        writeln!(f, "- key {}", internal_node_key)?;
                    }
                    print_tree(f, pager, right_child_pointer, indentation_level + 1)?;
                }
            }

            Ok(())
        }

        let mut pager = self.0.borrow_mut();
        print_tree(f, &mut pager, 0, 0)?;

        Ok(())
    }
}
