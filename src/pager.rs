use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

use crate::row::ROW_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Pager {
    file: File,
    file_length: u64,
    pages: Box<[Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES]>,
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

        // TODO: not efficient. the memory is preallocated here.
        let pages = Box::new([None; TABLE_MAX_PAGES]);

        Self {
            file,
            file_length,
            pages,
        }
    }

    pub fn file_length(&self) -> u64 {
        self.file_length
    }

    fn validate_page_num(page_num: usize) {
        if page_num > TABLE_MAX_PAGES {
            panic!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Option<&mut [u8; PAGE_SIZE]> {
        Self::validate_page_num(page_num);

        // TODO: memory is preallocated.
        // in case of Cache miss, Allocate memory and load from file.
        let mut page = self.pages[page_num].take().unwrap_or([0; PAGE_SIZE]);

        let mut num_pages = self.file_length / PAGE_SIZE as u64;

        // We might have partial page at the end of the file
        if self.file_length % PAGE_SIZE as u64 != 0 {
            num_pages += 1;
        }

        if page_num as u64 <= num_pages {
            let offset = page_num * PAGE_SIZE;
            self.file
                .seek(std::io::SeekFrom::Current(offset as i64))
                .expect("Unable to seek file.");

            self.file
                .read(&mut page)
                .expect("Unable to read file to a buffer.");
        }

        self.pages[page_num] = Some(page);

        self.pages[page_num].as_mut()
    }

    pub fn pager_flush(&mut self, page_num: usize, size: usize) {
        Self::validate_page_num(page_num);

        if self.pages[page_num].is_none() {
            panic!("Tried to flush null page.");
        }

        let offset = page_num * PAGE_SIZE;
        self.file
            .seek(std::io::SeekFrom::Current(offset as i64))
            .expect("Unable to seek file.");

        let default_page = [0; PAGE_SIZE];
        let page = self.pages[page_num].unwrap_or(default_page);
        let page = &page[..size];
        self.file.write(page).expect("Unable to write to file.");
        self.pages[page_num] = None;
    }
}
