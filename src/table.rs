use crate::row::{Row, ROW_SIZE};
use std::{cell::RefCell, fmt};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    num_rows: usize,
    pages: Box<RefCell<[Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES]>>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for i in 0..self.num_rows {
            let row = Row::deserialize(self.get_row_slot(i));
            write!(f, "{}", row)?;
        }
        Ok(())
    }
}

impl Table {
    pub fn new() -> Self {
        Self {
            num_rows: 0,
            pages: Box::new(RefCell::new([None; TABLE_MAX_PAGES])),
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn get_row_slot(&self, row_num: usize) -> [u8; ROW_SIZE] {
        let page_num = row_num / ROWS_PER_PAGE;

        let page = match self.pages.borrow_mut()[page_num].take() {
            Some(page) => page,
            None => [0; PAGE_SIZE],
        };

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let mut row_slot: [u8; ROW_SIZE] = [0; ROW_SIZE];
        row_slot.copy_from_slice(&page[byte_offset..byte_offset + ROW_SIZE]);

        self.pages.borrow_mut()[page_num] = Some(page);

        row_slot
    }

    pub fn put_row_slot(&mut self, row_slot: [u8; ROW_SIZE]) {
        let row_num = self.num_rows;

        let page_num = row_num / ROWS_PER_PAGE;

        let mut page = match self.pages.borrow_mut()[page_num].take() {
            Some(page) => page,
            None => [0; PAGE_SIZE],
        };

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        page[byte_offset..byte_offset + ROW_SIZE].copy_from_slice(&row_slot);
        self.pages.borrow_mut()[page_num] = Some(page);
        self.num_rows += 1;
    }
}
