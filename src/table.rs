use crate::{node::Node, pager::Pager};

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
        let new_left_child = root.clone();

        let _right_child = self.pager.get_page(right_child_page_num);
        let left_child_page_num = self.pager.get_unused_page_num();
        let left_child = self.pager.get_page(left_child_page_num);

        // Left child has data copied from old root
        *left_child = new_left_child;
        left_child.set_node_root(false);

        // Root node is a new internal node with one key and two children
        let left_child_max_key = left_child.get_node_max_key();

        let root = self.pager.get_page(self.root_page_num);
        *root = Node::initialize_internal_node();
        root.set_node_root(true);

        let root_num_keys: u32 = 1;
        root.internal_node_num_keys()
            .copy_from_slice(&root_num_keys.to_le_bytes());

        root.internal_node_child(0)
            .copy_from_slice(&left_child_page_num.to_le_bytes());

        root.internal_node_key(0)
            .copy_from_slice(&left_child_max_key.to_le_bytes());

        root.internal_node_right_child()
            .copy_from_slice(&right_child_page_num.to_le_bytes());
    }
}
