use crate::pager::Pager;

pub struct Table {
    pub root_page_num: u32,
    pub pager: Pager,
}

impl Drop for Table {
    fn drop(&mut self) {
        self.db_close();
    }
}

impl Table {
    pub fn db_open(filename: &str) -> Self {
        let pager = Pager::pager_open(filename);
        let root_page_num = 0;

        Self {
            root_page_num,
            pager,
        }
    }

    fn db_close(&mut self) {
        for i in 0..self.pager.num_pages {
            self.pager.pager_flush(i);
        }
    }
}
