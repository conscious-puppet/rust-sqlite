use crate::{
    node::{LEAF_NODE_MAX_CELLS, LEAF_NODE_NUM_CELLS_SIZE},
    row::Row,
    table::Table,
};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    page_num: u32,
    cell_num: u32,
    pub end_of_table: bool, // Indicates a position one past the last element
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        let page_num = table.root_page_num;
        let cell_num = 0;

        let root_node = table.pager.get_page(page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(root_node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        let end_of_table = num_cells == 0;

        Self {
            table,
            page_num,
            cell_num,
            end_of_table,
        }
    }

    pub fn table_end(table: &'a mut Table) -> Self {
        let page_num = table.root_page_num;

        let root_node = table.pager.get_page(page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(root_node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        let cell_num = num_cells;

        let end_of_table = true;

        Self {
            table,
            page_num,
            cell_num,
            end_of_table,
        }
    }

    pub fn value(&mut self) -> &mut [u8] {
        let page_num = self.page_num;
        let page = self.table.pager.get_page(page_num as u32);
        page.leaf_node_value(self.cell_num)
    }

    pub fn advance(&mut self) {
        let node = self.table.pager.get_page(self.page_num);

        self.cell_num += 1;

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        if self.cell_num >= num_cells {
            self.end_of_table = true;
        }
    }

    pub fn leaf_node_insert(&mut self, key: u32, row: Row) {
        let node = self.table.pager.get_page(self.page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        if num_cells as usize >= LEAF_NODE_MAX_CELLS {
            panic!("Need to implement splitting a leaf node.");
        }

        let num_cells = num_cells + 1;

        node.leaf_node_num_cells()
            .copy_from_slice(&num_cells.to_le_bytes());

        node.leaf_node_key(self.cell_num)
            .copy_from_slice(&key.to_le_bytes());

        row.serialize(node.leaf_node_value(self.cell_num));
    }
}
