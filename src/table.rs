use crate::{
    node::{InternalNodeCell, Node, INTERNAL_NODE_MAX_CELLS},
    pager::{Pager, INVALID_PAGE_NUM},
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

        let is_root_internal = match root {
            Node::Leaf { .. } => false,
            Node::Internal { .. } => true,
        };

        let new_left_child = std::mem::replace(root, Node::initialize_internal_node());

        let right_child = self.pager.get_page(right_child_page_num);

        if is_root_internal {
            *right_child = Node::initialize_internal_node();
        }

        let left_child_page_num = self.pager.get_unused_page_num();
        let left_child = self.pager.get_page(left_child_page_num);

        // Left child has data copied from old root
        *left_child = new_left_child;
        left_child.set_node_root(false);

        if let Node::Internal {
            num_keys,
            right_child_pointer,
            ..
        } = *left_child
        {
            let mut internal_node_page_num = Vec::new();
            for i in 0..num_keys {
                let internal_node_child = *left_child.internal_node_child(i);
                internal_node_page_num.push(internal_node_child);
            }

            for i in internal_node_page_num {
                let child = self.pager.get_page(i);
                *child.parent() = left_child_page_num;
            }

            let child = self.pager.get_page(right_child_pointer);
            *child.parent() = left_child_page_num;
        }

        // Root node is a new internal node with one key and two children
        let left_child_max_key = self.pager.get_node_max_key(left_child_page_num);
        let root = self.pager.get_page(self.root_page_num);
        *root = Node::initialize_internal_node();
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
        let child_max_key = self.pager.get_node_max_key(child_page_num);

        let parent = self.pager.get_page(parent_page_num);
        let index = parent.internal_node_find_child(child_max_key);
        let original_num_keys = *parent.internal_node_num_keys();

        if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
            self.internal_node_split_and_insert(parent_page_num, child_page_num);
            return;
        }

        let right_child_page_num = *parent.internal_node_right_child();

        // An internal node with a right child of INVALID_PAGE_NUM is empty
        if right_child_page_num == INVALID_PAGE_NUM {
            *parent.internal_node_right_child() = child_page_num;
            return;
        }

        // If we are already at the max number of cells for a node, we cannot increment
        // before splitting. Incrementing without inserting a new key/child pair
        // and immediately calling internal_node_split_and_insert has the effect
        // of creating a new key at (max_cells + 1) with an uninitialized value
        *parent.internal_node_num_keys() = original_num_keys + 1;

        let right_child_node_max_key = self.pager.get_node_max_key(right_child_page_num);

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

    pub fn internal_node_split_and_insert(&mut self, parent_page_num: u32, child_page_num: u32) {
        let mut old_page_num = parent_page_num;
        let old_max = self.pager.get_node_max_key(parent_page_num);

        let child_max = self.pager.get_node_max_key(child_page_num);
        let new_page_num = self.pager.get_unused_page_num();

        // Declaring a flag before updating pointers which
        // records whether this operation involves splitting the root -
        // if it does, we will insert our newly created node during
        // the step where the table's new root is created. If it does
        // not, we have to insert the newly created node into its parent
        // after the old node's keys have been transferred over. We are not
        // able to do this if the newly created node's parent is not a newly
        // initialized root node, because in that case its parent may have existing
        // keys aside from our old node which we are splitting. If that is true, we
        // need to find a place for our newly created node in its parent, and we
        // cannot insert it at the correct index if it does not yet have any keys
        let splitting_root = self.pager.get_page(old_page_num).is_node_root();

        let (parent_page_num, new_node_page_num) = if splitting_root {
            self.create_new_root(new_page_num);
            let parent = self.pager.get_page(self.root_page_num);
            let parent_page_num = self.root_page_num;
            old_page_num = *parent.internal_node_child(0);
            let _old_node = self.pager.get_page(old_page_num);
            (parent_page_num, 0)
        } else {
            let old_node = self.pager.get_page(old_page_num);
            let parent_page_num = *old_node.parent();
            let _parent = self.pager.get_page(parent_page_num);
            let new_node_page_num = new_page_num;
            let new_node = self.pager.get_page(new_node_page_num);
            *new_node = Node::initialize_internal_node();
            (parent_page_num, new_node_page_num)
        };

        let mut cur_page_num = *self
            .pager
            .get_page(old_page_num)
            .internal_node_right_child();

        // First put right child into new node and set right child of old node to invalid page number
        self.internal_node_insert(new_page_num, cur_page_num);
        *self.pager.get_page(cur_page_num).parent() = new_page_num;
        *self
            .pager
            .get_page(old_page_num)
            .internal_node_right_child() = INVALID_PAGE_NUM;

        // For each key until you get to the middle key, move the key and the child to the new node
        let mut i = INTERNAL_NODE_MAX_CELLS - 1;
        while i > INTERNAL_NODE_MAX_CELLS / 2 {
            cur_page_num = *self
                .pager
                .get_page(old_page_num)
                .internal_node_child(i as u32);
            self.internal_node_insert(new_page_num, cur_page_num);
            *self.pager.get_page(cur_page_num).parent() = new_page_num;
            *self
                .pager
                .get_page(old_page_num)
                .internal_node_right_child() -= 1;
            i -= 1;
        }

        {
            // Set child before middle key, which is now the highest key, to be node's right child,
            // and decrement number of keys
            let old_node = self.pager.get_page(old_page_num);
            let old_num_keys = *old_node.internal_node_num_keys();
            *old_node.internal_node_right_child() = *old_node.internal_node_child(old_num_keys - 1);
            *old_node.internal_node_num_keys() -= 1;
        }

        // Determine which of the two nodes after the split should contain the child to be inserted,
        // and insert the child
        let max_after_split = self.pager.get_node_max_key(old_page_num);
        let destination_page_num = if child_max < max_after_split {
            old_page_num
        } else {
            new_page_num
        };

        self.internal_node_insert(destination_page_num, child_page_num);
        *self.pager.get_page(child_page_num).parent() = destination_page_num;

        let old_node_max_key = self.pager.get_node_max_key(old_page_num);
        self.pager
            .get_page(parent_page_num)
            .update_internal_node_key(old_max, old_node_max_key);

        if !splitting_root {
            let old_node_parent = *self.pager.get_page(old_page_num).parent();
            self.internal_node_insert(old_node_parent, new_page_num);
            *self.pager.get_page(new_node_page_num).parent() = old_node_parent;
        }
    }
}
