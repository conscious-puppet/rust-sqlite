use crate::{
    pager::PAGE_SIZE,
    row::{Row, ROW_SIZE},
};

// Common Node Header Layout
pub const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
pub const NODE_TYPE_OFFSET: usize = 0;
pub const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
pub const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
pub const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
pub const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
pub const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_NEXT_LEAF_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
pub const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

// Leaf Node Body Layout
pub const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_KEY_OFFSET: usize = 0;
pub const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

pub const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
pub const LEAF_NODE_LEFT_SPLIT_COUNT: usize =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// Internal Node Header Layout
pub const INTERNAL_NODE_NUM_KEYS_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
pub const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Internal Node Header Layout
pub const INTERNAL_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_CHILD_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
pub const INTERNAL_NODE_MAX_CELLS: usize = 3; // Kept small for testing

// Leaf Node Format
// |-------------+----------------+----------------+-----------+--------------------|
// | byte 0      | byte 1         | bytes 2-5      | bytes 6-9 | bytes 10-13        |
// | node_type   | is_root        | parent_pointer | num_cells | next_leaf_pointer  |
// |-------------+----------------+----------------+-----------+--------------------|
// | bytes 14-17                  | bytes 18-308                                    |
// | key 0                        | value 0                                         |
// |------------------------------+-------------------------------------------------|
// | bytes 309-312                | bytes 313-603                                   |
// | key 1                        | value 1                                         |
// |------------------------------+-------------------------------------------------|
// |             ...              |          ...                                    |
// |------------------------------+-------------------------------------------------|
// | bytes 3554-3557              | bytes 3558-3848                                 |
// | key 12                       | value 12                                        |
// |------------------------------+-------------------------------------------------|
// |                                 bytes 3849-4095                                |
// |                                  wasted space                                  |
// |--------------------------------------------------------------------------------|
//
//
// Internal Node Format
// |-----------+---------+----------------+-----------+---------------------|
// | byte 0    | byte 1  | bytes 2-5      | bytes 6-9 | bytes 10-13         |
// | node_type | is_root | parent_pointer | num_keys  | right_child_pointer |
// |-----------+---------+----------------+-----------+---------------------|
// | bytes 14-17                         | bytes 18-21                      |
// | child pointer 0                     | key 0                            |
// |-------------------------------------+----------------------------------|
// | bytes 22-25                         | bytes 26-29                      |
// | child pointer 1                     | key 1                            |
// |-------------------------------------+----------------------------------|
// |                 ...                 |             ...                  |
// |-------------------------------------+----------------------------------|
// | bytes 4086-4089                     | bytes 4090-4093                  |
// | child pointer 509                   | key 509                          |
// |-------------------------------------+----------------------------------|
// |                              bytes 4094-4095                           |
// |                                wasted space                            |
// |------------------------------------------------------------------------|
//
// |------------------------+-----------------------+------------------------|
// | # internal node layers | max # leaf nodes      | Size of all leaf nodes |
// |------------------------+-----------------------+------------------------|
// | 0                      | 511 ^ 0 = 1           | 4 KB                   |
// | 1                      | 511 ^ 1 = 511         | ~2 MB                  |
// | 2                      | 511 ^ 2 = 261,121     | ~1 GB                  |
// | 3                      | 511 ^ 3 = 133,432,831 | ~550 GB                |
// |------------------------+-----------------------+------------------------|

pub enum Node {
    Leaf {
        is_root: bool,
        parent_pointer: u32,
        num_cells: u32,
        next_leaf_pointer: u32,
        cells: Vec<LeafNodeCell>,
    },
    Internal {
        is_root: bool,
        parent_pointer: u32,
        num_keys: u32,
        right_child_pointer: u32,
        cells: Vec<InternalNodeCell>,
    },
}

pub struct LeafNodeCell {
    key: u32,
    value: Row,
}

impl LeafNodeCell {
    pub fn new() -> Self {
        let row = [0; ROW_SIZE];
        Self {
            key: 0,
            value: Row::deserialize(&row),
        }
    }
}

pub struct InternalNodeCell {
    child_pointer: u32,
    key: u32,
}

impl InternalNodeCell {
    pub fn new() -> Self {
        Self {
            child_pointer: 0,
            key: 0,
        }
    }
}

impl Node {
    pub fn initialize_leaf_node() -> Self {
        let mut cells = Vec::new();
        for _ in 0..LEAF_NODE_MAX_CELLS {
            cells.push(LeafNodeCell::new())
        }
        Node::Leaf {
            is_root: false,
            parent_pointer: 0,
            num_cells: 0,
            next_leaf_pointer: 0,
            cells,
        }
    }

    pub fn initialize_internal_node() -> Self {
        let mut cells = Vec::new();
        for _ in 0..INTERNAL_NODE_MAX_CELLS {
            cells.push(InternalNodeCell::new())
        }
        Node::Internal {
            is_root: false,
            parent_pointer: 0,
            num_keys: 0,
            right_child_pointer: 0,
            cells,
        }
    }

    pub fn leaf_node_num_cells(&mut self) -> &mut u32 {
        match *self {
            Node::Leaf {
                ref mut num_cells, ..
            } => num_cells,
            Node::Internal { .. } => panic!("leaf_node_num_cells: Not a leaf node"),
        }
    }

    pub fn leaf_node_cell(&mut self, cell_num: u32) -> &mut LeafNodeCell {
        match *self {
            Node::Leaf { ref mut cells, .. } => &mut cells[cell_num as usize],
            Node::Internal { .. } => panic!("leaf_node_cell: Not a leaf node"),
        }
    }

    pub fn leaf_node_key(&mut self, cell_num: u32) -> &mut u32 {
        let leaf_node_cell = self.leaf_node_cell(cell_num);
        &mut leaf_node_cell.key
    }

    pub fn leaf_node_value(&mut self, cell_num: u32) -> &mut Row {
        let leaf_node_cell = self.leaf_node_cell(cell_num);
        &mut leaf_node_cell.value
    }

    pub fn is_node_root(&self) -> bool {
        match *self {
            Node::Leaf { is_root, .. } => is_root,
            Node::Internal { is_root, .. } => is_root,
        }
    }

    pub fn set_node_root(&mut self, is_root: bool) {
        let is_root_curr = match *self {
            Node::Leaf {
                ref mut is_root, ..
            } => is_root,
            Node::Internal {
                ref mut is_root, ..
            } => is_root,
        };

        *is_root_curr = is_root;
    }

    pub fn get_node_max_key(&mut self) -> u32 {
        match *self {
            Node::Leaf { num_cells, .. } => *self.leaf_node_key(num_cells - 1),
            Node::Internal { num_keys, .. } => *self.internal_node_key(num_keys - 1),
        }
    }

    pub fn internal_node_num_keys(&mut self) -> &mut u32 {
        match *self {
            Node::Leaf { .. } => {
                panic!("internal_node_num_keys: Not an internal node")
            }
            Node::Internal {
                ref mut num_keys, ..
            } => num_keys,
        }
    }

    pub fn internal_node_right_child(&mut self) -> &mut u32 {
        match *self {
            Node::Leaf { .. } => {
                panic!("internal_node_right_child: Not an internal node")
            }
            Node::Internal {
                ref mut right_child_pointer,
                ..
            } => right_child_pointer,
        }
    }

    pub fn internal_node_cell(&mut self, key_num: u32) -> &mut InternalNodeCell {
        match *self {
            Node::Leaf { .. } => {
                panic!("internal_node_right_child: Not an internal node")
            }
            Node::Internal { ref mut cells, .. } => &mut cells[key_num as usize],
        }
    }

    pub fn internal_node_child(&mut self, child_num: u32) -> &mut u32 {
        let num_keys = self.internal_node_num_keys();

        if child_num > *num_keys {
            panic!(
                "Tried to access child_num {} > num_keys {}",
                child_num, num_keys
            );
        } else if child_num == *num_keys {
            self.internal_node_right_child()
        } else {
            &mut self.internal_node_cell(child_num).child_pointer
        }
    }

    pub fn internal_node_key(&mut self, key_num: u32) -> &mut u32 {
        let internal_node_cell = self.internal_node_cell(key_num);
        &mut internal_node_cell.key
    }

    pub fn leaf_node_next_leaf(&mut self) -> &mut u32 {
        match *self {
            Node::Leaf {
                ref mut next_leaf_pointer,
                ..
            } => next_leaf_pointer,
            Node::Internal { .. } => panic!("leaf_node_next_leaf: Not a leaf node"),
        }
    }

    pub fn num_cell_or_keys(&mut self) -> &mut u32 {
        match *self {
            Node::Leaf {
                ref mut num_cells, ..
            } => num_cells,
            Node::Internal {
                ref mut num_keys, ..
            } => num_keys,
        }
    }

    pub fn node_key(&mut self, cell_num: u32) -> &mut u32 {
        match *self {
            Node::Leaf { .. } => self.leaf_node_key(cell_num),
            Node::Internal { .. } => self.internal_node_key(cell_num),
        }
    }

    pub fn parent(&mut self) -> &mut u32 {
        match *self {
            Node::Leaf {
                ref mut parent_pointer,
                ..
            } => parent_pointer,
            Node::Internal {
                ref mut parent_pointer,
                ..
            } => parent_pointer,
        }
    }

    pub fn update_internal_node_key(&mut self, old_key: u32, new_key: u32) {
        let old_child_index = self.internal_node_find_child(old_key);
        *self.internal_node_key(old_child_index) = new_key;
    }

    // Return the index of the child which should contain
    // the given key.
    pub fn internal_node_find_child(&mut self, key: u32) -> u32 {
        let num_keys = self.internal_node_num_keys();

        // Binary search
        let mut min_index = 0;
        let mut max_index = *num_keys; // there is one more child than key

        while min_index != max_index {
            let index = (min_index + max_index) / 2;
            let key_to_right = self.internal_node_key(index);

            if *key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        min_index
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let node_type = bytes[0]; // 0 -> Leaf Node, 1 -> Internal Node
        let is_root = bytes[1] == 1;

        let start = PARENT_POINTER_OFFSET;
        let end = start + PARENT_POINTER_SIZE;
        let mut parent_pointer_bytes = [0; PARENT_POINTER_SIZE];
        parent_pointer_bytes.copy_from_slice(&bytes[start..end]);
        let parent_pointer = u32::from_le_bytes(parent_pointer_bytes);

        if node_type == 0 {
            let start = LEAF_NODE_NUM_CELLS_OFFSET;
            let end = start + LEAF_NODE_NUM_CELLS_SIZE;
            let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
            num_cells_bytes.copy_from_slice(&bytes[start..end]);
            let num_cells = u32::from_le_bytes(num_cells_bytes);

            let start = LEAF_NODE_NEXT_LEAF_OFFSET;
            let end = start + LEAF_NODE_NEXT_LEAF_SIZE;
            let mut next_leaf_pointer_bytes = [0; LEAF_NODE_NEXT_LEAF_SIZE];
            next_leaf_pointer_bytes.copy_from_slice(&bytes[start..end]);
            let next_leaf_pointer = u32::from_le_bytes(next_leaf_pointer_bytes);

            let mut cells = Vec::new();

            let mut start = LEAF_NODE_HEADER_SIZE;
            let end = PAGE_SIZE;

            while start < end {
                let end = start + LEAF_NODE_KEY_SIZE;
                if end >= PAGE_SIZE {
                    break;
                }

                let mut key_bytes = [0; LEAF_NODE_KEY_SIZE];
                key_bytes.copy_from_slice(&bytes[start..end]);
                let key = u32::from_le_bytes(key_bytes);

                start = end;
                let end = start + ROW_SIZE;
                if end >= PAGE_SIZE {
                    break;
                }

                let value = Row::deserialize(&bytes[start..end]);
                let leaf_node_cell = LeafNodeCell { key, value };

                cells.push(leaf_node_cell);
                start = end;
            }

            Node::Leaf {
                is_root,
                parent_pointer,
                num_cells,
                next_leaf_pointer,
                cells,
            }
        } else {
            let start = INTERNAL_NODE_NUM_KEYS_OFFSET;
            let end = start + INTERNAL_NODE_NUM_KEYS_SIZE;
            let mut num_keys_bytes = [0; INTERNAL_NODE_NUM_KEYS_SIZE];
            num_keys_bytes.copy_from_slice(&bytes[start..end]);
            let num_keys = u32::from_le_bytes(num_keys_bytes);

            let start = INTERNAL_NODE_RIGHT_CHILD_OFFSET;
            let end = start + INTERNAL_NODE_RIGHT_CHILD_SIZE;
            let mut right_child_pointer_bytes = [0; INTERNAL_NODE_RIGHT_CHILD_SIZE];
            right_child_pointer_bytes.copy_from_slice(&bytes[start..end]);
            let right_child_pointer = u32::from_le_bytes(right_child_pointer_bytes);

            let mut cells = Vec::new();

            let mut start = INTERNAL_NODE_HEADER_SIZE;
            let end = PAGE_SIZE;

            while start < end {
                let end = start + INTERNAL_NODE_CHILD_SIZE;
                if end >= PAGE_SIZE {
                    break;
                }

                let mut child_pointer_bytes = [0; INTERNAL_NODE_CHILD_SIZE];
                child_pointer_bytes.copy_from_slice(&bytes[start..end]);
                let child_pointer = u32::from_le_bytes(child_pointer_bytes);

                start = end;
                let end = start + INTERNAL_NODE_KEY_SIZE;
                if end >= PAGE_SIZE {
                    break;
                }
                let mut key_bytes = [0; INTERNAL_NODE_KEY_SIZE];
                key_bytes.copy_from_slice(&bytes[start..end]);
                let key = u32::from_le_bytes(key_bytes);

                let internal_node_cell = InternalNodeCell { child_pointer, key };

                cells.push(internal_node_cell);
                start = end;
            }

            Node::Internal {
                is_root,
                parent_pointer,
                num_keys,
                right_child_pointer,
                cells,
            }
        }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut node = [0; PAGE_SIZE];

        match self {
            Node::Leaf {
                is_root,
                parent_pointer,
                num_cells,
                next_leaf_pointer,
                cells,
            } => {
                node[0] = 0;
                node[1] = if *is_root { 1 } else { 0 };

                let start = PARENT_POINTER_OFFSET;
                let end = start + PARENT_POINTER_SIZE;
                node[start..end].copy_from_slice(&parent_pointer.to_le_bytes());

                let start = LEAF_NODE_NUM_CELLS_OFFSET;
                let end = start + LEAF_NODE_NUM_CELLS_SIZE;
                node[start..end].copy_from_slice(&num_cells.to_le_bytes());

                let start = LEAF_NODE_NEXT_LEAF_OFFSET;
                let end = start + LEAF_NODE_NEXT_LEAF_SIZE;
                node[start..end].copy_from_slice(&next_leaf_pointer.to_le_bytes());

                let mut start = LEAF_NODE_HEADER_SIZE;
                for cell in cells {
                    let end = start + LEAF_NODE_KEY_SIZE;
                    if end >= PAGE_SIZE {
                        break;
                    }
                    node[start..end].copy_from_slice(&cell.key.to_le_bytes());

                    start = end;
                    let end = start + ROW_SIZE;
                    if end >= PAGE_SIZE {
                        break;
                    }

                    let mut value = [0; ROW_SIZE];
                    cell.value.serialize(&mut value);
                    node[start..end].copy_from_slice(&value);

                    start = end;
                }
            }
            Node::Internal {
                parent_pointer,
                is_root,
                num_keys,
                right_child_pointer,
                cells,
            } => {
                node[0] = 1;
                node[1] = if *is_root { 1 } else { 0 };

                let start = PARENT_POINTER_OFFSET;
                let end = start + PARENT_POINTER_SIZE;
                node[start..end].copy_from_slice(&parent_pointer.to_le_bytes());

                let start = INTERNAL_NODE_NUM_KEYS_OFFSET;
                let end = start + INTERNAL_NODE_NUM_KEYS_SIZE;
                node[start..end].copy_from_slice(&num_keys.to_le_bytes());

                let start = INTERNAL_NODE_RIGHT_CHILD_OFFSET;
                let end = start + INTERNAL_NODE_RIGHT_CHILD_SIZE;
                node[start..end].copy_from_slice(&right_child_pointer.to_le_bytes());

                let mut start = INTERNAL_NODE_HEADER_SIZE;
                for cell in cells {
                    let end = start + INTERNAL_NODE_CHILD_SIZE;
                    if end >= PAGE_SIZE {
                        break;
                    }
                    node[start..end].copy_from_slice(&cell.child_pointer.to_le_bytes());

                    start = end;
                    let end = start + INTERNAL_NODE_KEY_SIZE;
                    if end >= PAGE_SIZE {
                        break;
                    }
                    node[start..end].copy_from_slice(&cell.key.to_le_bytes());

                    start = end;
                }
            }
        }
        node
    }
}
