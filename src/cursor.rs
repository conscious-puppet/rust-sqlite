use crate::{
    node::{
        NodeType, INTERNAL_NODE_CHILD_SIZE, INTERNAL_NODE_KEY_SIZE, INTERNAL_NODE_NUM_KEYS_SIZE,
        LEAF_NODE_CELL_SIZE, LEAF_NODE_KEY_SIZE, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
        LEAF_NODE_NEXT_LEAF_SIZE, LEAF_NODE_NUM_CELLS_SIZE, LEAF_NODE_RIGHT_SPLIT_COUNT,
    },
    row::Row,
    table::Table,
};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    page_num: u32,
    pub cell_num: u32,      // Indicates the row num
    pub end_of_table: bool, // Indicates a position one past the last element
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        let cursor = Cursor::table_find(table, 0);

        let page_num = cursor.page_num;
        let node = table.pager.get_page(page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        let mut cursor = Cursor::table_find(table, 0);
        cursor.end_of_table = num_cells == 0;

        cursor
    }

    /// Return the position of the given key.
    /// If the key is not present, return the position
    /// where it should be inserted
    pub fn table_find(table: &'a mut Table, key: u32) -> Self {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num);

        if root_node.get_node_type() == NodeType::Leaf {
            Cursor::leaf_node_find(table, root_page_num, key)
        } else {
            Cursor::internal_node_find(table, root_page_num, key)
        }
    }

    fn leaf_node_find(table: &'a mut Table, page_num: u32, key: u32) -> Self {
        let node = table.pager.get_page(page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        // Binary search
        let mut min_index = 0;
        let mut one_past_max_index = num_cells;
        let mut cell_num = None;

        while one_past_max_index != min_index {
            let index = (min_index + one_past_max_index) / 2;

            let mut key_at_index_bytes = [0; LEAF_NODE_KEY_SIZE];
            key_at_index_bytes.copy_from_slice(node.leaf_node_key(index));
            let key_at_index = u32::from_le_bytes(key_at_index_bytes);

            if key == key_at_index {
                cell_num = Some(index);
                break;
            }
            if key < key_at_index {
                one_past_max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        let cell_num = cell_num.unwrap_or(min_index);

        Self {
            table,
            page_num,
            cell_num,
            end_of_table: false,
        }
    }

    fn internal_node_find(table: &'a mut Table, page_num: u32, key: u32) -> Self {
        let node = table.pager.get_page(page_num);

        let mut num_keys_bytes = [0; INTERNAL_NODE_NUM_KEYS_SIZE];
        num_keys_bytes.copy_from_slice(node.internal_node_num_keys());
        let num_keys = u32::from_le_bytes(num_keys_bytes);

        // Binary search to find index of child to search
        let mut min_index = 0;
        let mut max_index = num_keys; // there is one more child than key

        while min_index != max_index {
            let index = (min_index + max_index) / 2;

            let mut key_to_right_bytes = [0; INTERNAL_NODE_KEY_SIZE];
            key_to_right_bytes.copy_from_slice(node.internal_node_key(index));
            let key_to_right = u32::from_le_bytes(key_to_right_bytes);

            if key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        let mut child_num_bytes = [0; INTERNAL_NODE_CHILD_SIZE];
        child_num_bytes.copy_from_slice(node.internal_node_child(min_index));
        let child_num = u32::from_le_bytes(child_num_bytes);

        let child = table.pager.get_page(child_num);

        match child.get_node_type() {
            NodeType::Leaf => Cursor::leaf_node_find(table, child_num, key),
            NodeType::Internal => Cursor::internal_node_find(table, child_num, key),
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
            // Advance to next leaf node
            let mut next_page_num_bytes = [0; LEAF_NODE_NEXT_LEAF_SIZE];
            next_page_num_bytes.copy_from_slice(node.leaf_node_next_leaf());
            let next_page_num = u32::from_le_bytes(next_page_num_bytes);

            if next_page_num == 0 {
                // This is the right most leaf
                self.end_of_table = true;
            } else {
                self.page_num = next_page_num;
                self.cell_num = 0;
            }
        }
    }

    pub fn leaf_node_insert(&mut self, key: u32, row: Row) {
        let node = self.table.pager.get_page(self.page_num);

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);

        if num_cells as usize >= LEAF_NODE_MAX_CELLS {
            self.leaf_node_split_and_insert(key, row);
            return;
        }

        if self.cell_num < num_cells {
            // Make room for new cell
            let mut i = num_cells;
            while i > self.cell_num {
                let mut prev = [0; LEAF_NODE_CELL_SIZE];
                prev.copy_from_slice(node.leaf_node_cell(i - 1));
                node.leaf_node_cell(i).copy_from_slice(&prev);
                i -= 1;
            }
        }

        let num_cells = num_cells + 1;

        node.leaf_node_num_cells()
            .copy_from_slice(&num_cells.to_le_bytes());

        node.leaf_node_key(self.cell_num)
            .copy_from_slice(&key.to_le_bytes());

        row.serialize(node.leaf_node_value(self.cell_num));
    }

    /// Create a new node and move half the cells over.
    /// Insert the new value in one of the two nodes.
    /// Update parent or create a new parent.
    fn leaf_node_split_and_insert(&mut self, key: u32, row: Row) {
        let new_page_num = self.table.pager.get_unused_page_num();

        let old_node = self.table.pager.get_page(self.page_num);
        let mut next_node = [0; LEAF_NODE_NEXT_LEAF_SIZE];
        next_node.copy_from_slice(old_node.leaf_node_next_leaf());
        old_node
            .leaf_node_next_leaf()
            .copy_from_slice(&new_page_num.to_le_bytes());
        let new_node = self.table.pager.get_page(new_page_num);
        new_node.leaf_node_next_leaf().copy_from_slice(&next_node);

        // All existing keys plus new key should be divided
        // evenly between old (left) and new (right) nodes.
        // Starting from the right, move each key to correct position.
        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            let mut old_node = self.table.pager.get_page(self.page_num).clone();
            let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                self.table.pager.get_page(new_page_num)
            } else {
                self.table.pager.get_page(self.page_num)
            };

            let index_within_node = (i % LEAF_NODE_LEFT_SPLIT_COUNT) as u32;
            let destination = destination_node.leaf_node_cell(index_within_node);

            if i == self.cell_num as usize {
                let destination = destination_node.leaf_node_value(index_within_node);
                row.serialize(destination);
                destination_node
                    .leaf_node_key(index_within_node)
                    .copy_from_slice(&key.to_le_bytes());
            } else if i > self.cell_num as usize {
                destination.copy_from_slice(old_node.leaf_node_cell(i as u32 - 1));
            } else {
                destination.copy_from_slice(old_node.leaf_node_cell(i as u32));
            }
        }

        // Update cell count on both leaf nodes
        let right_split_num_cells_bytes = (LEAF_NODE_RIGHT_SPLIT_COUNT as u32).to_le_bytes();
        let new_node = self.table.pager.get_page(new_page_num);
        new_node
            .leaf_node_num_cells()
            .copy_from_slice(&right_split_num_cells_bytes);

        let left_split_num_cells_bytes = (LEAF_NODE_LEFT_SPLIT_COUNT as u32).to_le_bytes();
        let old_node = self.table.pager.get_page(self.page_num);
        old_node
            .leaf_node_num_cells()
            .copy_from_slice(&left_split_num_cells_bytes);

        if old_node.is_node_root() {
            self.table.create_new_root(new_page_num);
        } else {
            todo!("Need to implement updating parent after split.")
        }
    }
}
