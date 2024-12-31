use crate::{
    row::ROW_SIZE,
    table::{Table, ROWS_PER_PAGE},
};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    row_num: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        let row_num = 0;
        let end_of_table = table.num_rows == 0;

        Self {
            table,
            row_num,
            end_of_table,
        }
    }

    pub fn table_end(table: &'a mut Table) -> Self {
        let row_num = table.num_rows;
        let end_of_table = true;

        Self {
            table,
            row_num,
            end_of_table,
        }
    }

    pub fn value(&mut self) -> Option<&mut [u8]> {
        let page_num = self.row_num / ROWS_PER_PAGE;
        let row_offset = self.row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let page = self.table.pager.get_page(page_num)?;
        let page = &mut page[byte_offset..byte_offset + ROW_SIZE];
        Some(page)
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }
}
