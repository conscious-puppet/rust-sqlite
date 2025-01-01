use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

use crate::node::Node;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    file: File,
    file_length: u64,
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

                self.file
                    .read(&mut page.0)
                    .expect("Unable to read file to a buffer.");
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
        let _ = self.get_page(page_num);

        let offset = page_num as usize * PAGE_SIZE;
        self.file
            .seek(std::io::SeekFrom::Start(offset as u64))
            .expect("Unable to seek file.");

        let page = &self.pages[page_num as usize];
        self.file.write(&page.0).expect("Unable to write to file.");
    }
}
