use crate::{
    node::{InternalNodeCell, Node, INTERNAL_NODE_MAX_CELLS},
    pager::Pager,
};

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
        let mut pager = Pager::pager_open(filename);
        let root_page_num = 0;

        if pager.num_pages == 0 {
            // New database file. Initialize page 0 as leaf node.
            let root_node = pager.get_page(0);
            root_node.set_node_root(true);
        }

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

    pub fn create_new_root(&mut self, right_child_page_num: u32) {
        // Handle splitting the root.
        // Old root copied to new page, becomes left child.
        // Address of right child passed in.
        // Re-initialize root page to contain the new root node.
        // New root node points to two children.

        let root = self.pager.get_page(self.root_page_num);
        let new_left_child = std::mem::replace(root, Node::initialize_internal_node());

        let left_child_page_num = self.pager.get_unused_page_num();
        let left_child = self.pager.get_page(left_child_page_num);

        // Left child has data copied from old root
        *left_child = new_left_child;
        left_child.set_node_root(false);

        // Root node is a new internal node with one key and two children
        let left_child_max_key = left_child.get_node_max_key();
        let root = self.pager.get_page(self.root_page_num);
        root.set_node_root(true);
        *root.internal_node_num_keys() = 1;
        *root.internal_node_child(0) = left_child_page_num;
        *root.internal_node_key(0) = left_child_max_key;
        *root.internal_node_right_child() = right_child_page_num;

        let left_child = self.pager.get_page(left_child_page_num);
        *left_child.parent() = self.root_page_num;

        let right_child = self.pager.get_page(right_child_page_num);
        *right_child.parent() = self.root_page_num;
    }

    // Add a new child/key pair to parent that corresponds to child
    pub fn internal_node_insert(&mut self, parent_page_num: u32, child_page_num: u32) {
        let child = self.pager.get_page(child_page_num);
        let child_max_key = child.get_node_max_key();

        let parent = self.pager.get_page(parent_page_num);
        let index = parent.internal_node_find_child(child_max_key);
        let original_num_keys = *parent.internal_node_num_keys();
        *parent.internal_node_num_keys() = original_num_keys + 1;

        if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
            panic!("Need to implement splitting internal node");
        }

        let right_child_page_num = *parent.internal_node_right_child();
        let right_child = self.pager.get_page(right_child_page_num);
        let right_child_node_max_key = right_child.get_node_max_key();

        if child_max_key > right_child_node_max_key {
            // Replace right child
            let parent = self.pager.get_page(parent_page_num);
            *parent.internal_node_child(original_num_keys) = right_child_page_num;
            *parent.internal_node_key(original_num_keys) = right_child_node_max_key;
            *parent.internal_node_right_child() = child_page_num;
        } else {
            // Make room for the new cell
            let parent = self.pager.get_page(parent_page_num);
            let mut i = original_num_keys;
            while i > index {
                let source =
                    std::mem::replace(parent.internal_node_cell(i - 1), InternalNodeCell::new());
                let destination = parent.internal_node_cell(i);
                *destination = source;
                i -= 1;
            }
            *parent.internal_node_child(index) = child_page_num;
            *parent.internal_node_key(index) = child_max_key;
        }
    }
}
