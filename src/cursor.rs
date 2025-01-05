use crate::{
    node::{
        LeafNodeCell, Node, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
        LEAF_NODE_RIGHT_SPLIT_COUNT,
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
        let num_cells = *node.leaf_node_num_cells();
        let end_of_table = num_cells == 0;

        let mut cursor = Cursor::table_find(table, 0);
        cursor.end_of_table = end_of_table;

        cursor
    }

    /// Return the position of the given key.
    /// If the key is not present, return the position
    /// where it should be inserted
    pub fn table_find(table: &'a mut Table, key: u32) -> Self {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num);

        match root_node {
            Node::Leaf { .. } => Cursor::leaf_node_find(table, root_page_num, key),
            Node::Internal { .. } => Cursor::internal_node_find(table, root_page_num, key),
        }
    }

    fn leaf_node_find(table: &'a mut Table, page_num: u32, key: u32) -> Self {
        let node = table.pager.get_page(page_num);

        let num_cells = node.leaf_node_num_cells();

        // Binary search
        let mut min_index = 0;
        let mut one_past_max_index = *num_cells;
        let mut cell_num = None;

        while one_past_max_index != min_index {
            let index = (min_index + one_past_max_index) / 2;
            let key_at_index = node.leaf_node_key(index);

            if key == *key_at_index {
                cell_num = Some(index);
                break;
            } else if key < *key_at_index {
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

        let child_index = node.internal_node_find_child(key);
        let child_num = *node.internal_node_child(child_index);
        let child = table.pager.get_page(child_num);

        match child {
            Node::Leaf { .. } => Cursor::leaf_node_find(table, child_num, key),
            Node::Internal { .. } => Cursor::internal_node_find(table, child_num, key),
        }
    }

    pub fn value(&mut self) -> &mut Row {
        let page_num = self.page_num;
        let page = self.table.pager.get_page(page_num as u32);
        page.leaf_node_value(self.cell_num)
    }

    pub fn advance(&mut self) {
        let node = self.table.pager.get_page(self.page_num);
        self.cell_num += 1;
        let num_cells = *node.leaf_node_num_cells();

        if self.cell_num >= num_cells {
            // Advance to next leaf node
            let next_page_num = *node.leaf_node_next_leaf();

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
        let num_cells = *node.leaf_node_num_cells();

        if num_cells as usize >= LEAF_NODE_MAX_CELLS {
            self.leaf_node_split_and_insert(key, row);
            return;
        }

        if self.cell_num < num_cells {
            // Make room for new cell
            let mut i = num_cells;
            while i > self.cell_num {
                let prev = std::mem::replace(node.leaf_node_cell(i - 1), LeafNodeCell::new());
                *node.leaf_node_cell(i) = prev;
                i -= 1;
            }
        }

        *node.leaf_node_num_cells() += 1;
        *node.leaf_node_key(self.cell_num) = key;
        *node.leaf_node_value(self.cell_num) = row;
    }

    /// Create a new node and move half the cells over.
    /// Insert the new value in one of the two nodes.
    /// Update parent or create a new parent.
    fn leaf_node_split_and_insert(&mut self, key: u32, row: Row) {
        let new_page_num = self.table.pager.get_unused_page_num();

        let old_max = self.table.pager.get_node_max_key(self.page_num);
        let old_node = self.table.pager.get_page(self.page_num);
        let next_node = *old_node.leaf_node_next_leaf();
        let old_node_parent = *old_node.parent();
        *old_node.leaf_node_next_leaf() = new_page_num;

        let new_node = self.table.pager.get_page(new_page_num);
        *new_node.leaf_node_next_leaf() = next_node;
        *new_node.parent() = old_node_parent;

        // All existing keys plus new key should be divided
        // evenly between old (left) and new (right) nodes.
        // Starting from the right, move each key to correct position.
        for i in (self.cell_num as usize + 1..=LEAF_NODE_MAX_CELLS).rev() {
            let old_node = self.table.pager.get_page(self.page_num);
            let old_leaf_node_cell =
                std::mem::replace(old_node.leaf_node_cell(i as u32 - 1), LeafNodeCell::new());
            let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                self.table.pager.get_page(new_page_num)
            } else {
                self.table.pager.get_page(self.page_num)
            };
            let index_within_node = (i % LEAF_NODE_LEFT_SPLIT_COUNT) as u32;
            let destination = destination_node.leaf_node_cell(index_within_node);
            *destination = old_leaf_node_cell;
        }

        let destination_node = if self.cell_num as usize >= LEAF_NODE_LEFT_SPLIT_COUNT {
            self.table.pager.get_page(new_page_num)
        } else {
            self.table.pager.get_page(self.page_num)
        };

        let index_within_node = (self.cell_num as usize % LEAF_NODE_LEFT_SPLIT_COUNT) as u32;
        *destination_node.leaf_node_value(index_within_node) = row;
        *destination_node.leaf_node_key(index_within_node) = key;

        for i in (0..self.cell_num as usize).rev() {
            let old_node = self.table.pager.get_page(self.page_num);
            let old_leaf_node_cell =
                std::mem::replace(old_node.leaf_node_cell(i as u32), LeafNodeCell::new());
            let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                self.table.pager.get_page(new_page_num)
            } else {
                self.table.pager.get_page(self.page_num)
            };
            let index_within_node = (i % LEAF_NODE_LEFT_SPLIT_COUNT) as u32;
            let destination = destination_node.leaf_node_cell(index_within_node);
            *destination = old_leaf_node_cell;
        }

        // Update cell count on both leaf nodes
        let new_node = self.table.pager.get_page(new_page_num);
        *new_node.leaf_node_num_cells() = LEAF_NODE_RIGHT_SPLIT_COUNT as u32;

        let old_node = self.table.pager.get_page(self.page_num);
        *old_node.leaf_node_num_cells() = LEAF_NODE_LEFT_SPLIT_COUNT as u32;

        if old_node.is_node_root() {
            self.table.create_new_root(new_page_num);
        } else {
            let parent_page_num = *old_node.parent();
            let new_max = self.table.pager.get_node_max_key(self.page_num);
            let parent = self.table.pager.get_page(parent_page_num);
            parent.update_internal_node_key(old_max, new_max);
            self.table
                .internal_node_insert(parent_page_num, new_page_num);
        }
    }
}
