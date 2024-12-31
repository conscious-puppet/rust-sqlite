use crate::pager::Pager;
use crate::row::ROW_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    pub num_rows: usize,
    pub pager: Pager,
}

pub struct RowSlot {
    pub page_num: usize,
    pub byte_offset: usize,
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
        Self { num_rows, pager }
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

    fn db_close(&mut self) {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            if let None = self.pager.get_page(i) {
                continue;
            }
            self.pager.pager_flush(i, PAGE_SIZE);
        }

        // There may be a partial page to write to the end of the file
        // This should not be needed after we switch to a B-tree
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;

        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if let None = self.pager.get_page(page_num) {
                return;
            }
            self.pager
                .pager_flush(page_num, num_additional_rows * ROW_SIZE);
        }
    }
}
