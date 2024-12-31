use std::cell::RefCell;
use std::fmt;

use crate::pager::Pager;
use crate::row::{Row, ROW_SIZE};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    num_rows: usize,
    pager: RefCell<Pager>,
}

pub struct RowSlot {
    pub page_num: usize,
    pub byte_offset: usize,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for i in 0..self.num_rows {
            if let Some(row) = self.get_row(i) {
                writeln!(f, "{}", row)?;
            }
        }
        Ok(())
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.db_close();
    }
}

impl Table {
    pub fn db_open(filename: &str) -> Self {
        let pager = Pager::pager_open(filename);
        let num_rows = (pager.file_length() / ROW_SIZE as u64) as usize;
        let pager = RefCell::new(pager);

        Self { num_rows, pager }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn row_slot(row_num: usize) -> RowSlot {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        RowSlot {
            page_num,
            byte_offset,
        }
    }

    pub fn get_row(&self, row_num: usize) -> Option<Row> {
        let row_slot = Self::row_slot(row_num);
        let mut pager = self.pager.borrow_mut();
        let page = pager.get_page(row_slot.page_num)?;
        let mut row: [u8; ROW_SIZE] = [0; ROW_SIZE];
        row.copy_from_slice(&page[row_slot.byte_offset..row_slot.byte_offset + ROW_SIZE]);
        Some(Row::deserialize(row))
    }

    pub fn put_row(&mut self, row: Row) {
        let row_num = self.num_rows;
        let row_slot = Self::row_slot(row_num);
        let mut pager = self.pager.borrow_mut();
        let page = pager.get_page(row_slot.page_num);
        let row = row.serialize();
        page.map(|p| {
            p[row_slot.byte_offset..row_slot.byte_offset + ROW_SIZE].copy_from_slice(&row)
        });
        self.num_rows += 1;
    }

    fn db_close(&mut self) {
        let mut pager = self.pager.borrow_mut();
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            if let None = pager.get_page(i) {
                continue;
            }
            pager.pager_flush(i, PAGE_SIZE);
        }

        // There may be a partial page to write to the end of the file
        // This should not be needed after we switch to a B-tree
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;

        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if let None = pager.get_page(page_num) {
                return;
            }
            pager.pager_flush(page_num, num_additional_rows * ROW_SIZE);
        }
    }
}
