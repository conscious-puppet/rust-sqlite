use crate::{pager::PAGE_SIZE, row::ROW_SIZE};

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

#[derive(PartialEq, Eq)]
pub enum NodeType {
    Leaf,
    Internal,
}

impl NodeType {
    pub fn to_bytes(&self) -> [u8; 1] {
        match *self {
            NodeType::Leaf => [0],
            NodeType::Internal => [1],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        match *bytes {
            [0] => NodeType::Leaf,
            [1] => NodeType::Internal,
            _ => panic!("Undefined node type."),
        }
    }
}

/// Leaf Node Format
/// |-------------+----------------+----------------+-----------+--------------------|
/// | byte 0      | byte 1         | bytes 2-5      | bytes 6-9 | bytes 10-13        |
/// | node_type   | is_root        | parent_pointer | num_cells | left_leaf_pointer  |
/// |-------------+----------------+----------------+-----------+--------------------|
/// | bytes 14-17                  | bytes 18-308                                    |
/// | key 0                        | value 0                                         |
/// |------------------------------+-------------------------------------------------|
/// | bytes 309-312                | bytes 313-603                                   |
/// | key 1                        | value 1                                         |
/// |------------------------------+-------------------------------------------------|
/// |             ...              |          ...                                    |
/// |------------------------------+-------------------------------------------------|
/// | bytes 3554-3557              | bytes 3558-3848                                 |
/// | key 12                       | value 12                                        |
/// |------------------------------+-------------------------------------------------|
/// |                                 bytes 3849-4095                                |
/// |                                  wasted space                                  |
/// |--------------------------------------------------------------------------------|
///
///
/// Internal Node Format
/// |-----------+---------+----------------+-----------+---------------------|
/// | byte 0    | byte 1  | bytes 2-5      | bytes 6-9 | bytes 10-13         |
/// | node_type | is_root | parent_pointer | num_keys  | right_child_pointer |
/// |-----------+---------+----------------+-----------+---------------------|
/// | bytes 14-17                         | bytes 18-21                      |
/// | child pointer 0                     | key 0                            |
/// |-------------------------------------+----------------------------------|
/// | bytes 22-25                         | bytes 26-29                      |
/// | child pointer 1                     | key 1                            |
/// |-------------------------------------+----------------------------------|
/// |                 ...                 |             ...                  |
/// |-------------------------------------+----------------------------------|
/// | bytes 4086-4089                     | bytes 4090-4093                  |
/// | child pointer 509                   | key 509                          |
/// |-------------------------------------+----------------------------------|
/// |                              bytes 4094-4095                           |
/// |                                wasted space                            |
/// |------------------------------------------------------------------------|
///
/// |------------------------+-----------------------+------------------------|
/// | # internal node layers | max # leaf nodes      | Size of all leaf nodes |
/// |------------------------+-----------------------+------------------------|
/// | 0                      | 511 ^ 0 = 1           | 4 KB                   |
/// | 1                      | 511 ^ 1 = 511         | ~2 MB                  |
/// | 2                      | 511 ^ 2 = 261,121     | ~1 GB                  |
/// | 3                      | 511 ^ 3 = 133,432,831 | ~550 GB                |
/// |------------------------+-----------------------+------------------------|

#[derive(Clone)]
pub struct Node(pub [u8; PAGE_SIZE]);

impl Node {
    pub fn initialize_leaf_node() -> Self {
        let mut node = Self([0; PAGE_SIZE]);
        Node::set_node_type(&mut node, NodeType::Leaf);
        Node::set_node_root(&mut node, false);
        node
    }

    pub fn initialize_internal_node() -> Self {
        let mut node = Self([0; PAGE_SIZE]);
        Node::set_node_type(&mut node, NodeType::Internal);
        Node::set_node_root(&mut node, false);
        node
    }

    pub fn leaf_node_num_cells(&mut self) -> &mut [u8] {
        let start = LEAF_NODE_NUM_CELLS_OFFSET;
        let end = start + LEAF_NODE_NUM_CELLS_SIZE;
        &mut self.0[start..end]
    }

    pub fn leaf_node_cell(&mut self, cell_num: u32) -> &mut [u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE;
        let end = start + LEAF_NODE_CELL_SIZE;
        &mut self.0[start..end]
    }

    pub fn leaf_node_key(&mut self, cell_num: u32) -> &mut [u8] {
        let leaf_node_cell = self.leaf_node_cell(cell_num);
        let start = LEAF_NODE_KEY_OFFSET;
        let end = start + LEAF_NODE_KEY_SIZE;
        &mut leaf_node_cell[start..end]
    }

    pub fn leaf_node_value(&mut self, cell_num: u32) -> &mut [u8] {
        let leaf_node_cell = self.leaf_node_cell(cell_num);
        let start = LEAF_NODE_VALUE_OFFSET;
        let end = start + LEAF_NODE_VALUE_SIZE;
        &mut leaf_node_cell[start..end]
    }

    pub fn get_node_type(&self) -> NodeType {
        let start = NODE_TYPE_OFFSET;
        let end = start + NODE_TYPE_SIZE;
        NodeType::from_bytes(&self.0[start..end])
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        let start = NODE_TYPE_OFFSET;
        let end = start + NODE_TYPE_SIZE;
        let node_type = node_type.to_bytes();
        self.0[start..end].copy_from_slice(&node_type);
    }

    pub fn is_node_root(&self) -> bool {
        let start = IS_ROOT_OFFSET;
        let end = start + IS_ROOT_SIZE;
        let mut is_node_root_bytes = [0; IS_ROOT_SIZE];
        is_node_root_bytes.copy_from_slice(&self.0[start..end]);
        u8::from_le_bytes(is_node_root_bytes) == 1
    }

    pub fn set_node_root(&mut self, is_root: bool) {
        let value = is_root as u8;
        let start = IS_ROOT_OFFSET;
        let end = start + IS_ROOT_SIZE;
        self.0[start..end].copy_from_slice(&value.to_le_bytes());
    }

    pub fn get_node_max_key(&mut self) -> u32 {
        let mut max_key_bytes = [0; std::mem::size_of::<u32>()];
        max_key_bytes.copy_from_slice(match self.get_node_type() {
            NodeType::Internal => {
                let mut key_num_bytes = [0; INTERNAL_NODE_NUM_KEYS_SIZE];
                key_num_bytes.copy_from_slice(self.internal_node_num_keys());
                let key_num = u32::from_le_bytes(key_num_bytes) - 1;
                self.internal_node_key(key_num)
            }
            NodeType::Leaf => {
                let mut cell_num_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
                cell_num_bytes.copy_from_slice(self.leaf_node_num_cells());
                let cell_num = u32::from_le_bytes(cell_num_bytes) - 1;
                self.leaf_node_key(cell_num)
            }
        });
        u32::from_le_bytes(max_key_bytes)
    }

    pub fn internal_node_num_keys(&mut self) -> &mut [u8] {
        let start = INTERNAL_NODE_NUM_KEYS_OFFSET;
        let end = start + INTERNAL_NODE_NUM_KEYS_SIZE;
        &mut self.0[start..end]
    }

    pub fn internal_node_right_child(&mut self) -> &mut [u8] {
        let start = INTERNAL_NODE_RIGHT_CHILD_OFFSET;
        let end = start + INTERNAL_NODE_RIGHT_CHILD_SIZE;
        &mut self.0[start..end]
    }

    pub fn internal_node_cell(&mut self, cell_num: u32) -> &mut [u8] {
        let start = INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE;
        let end = start + INTERNAL_NODE_CELL_SIZE;
        &mut self.0[start..end]
    }

    pub fn internal_node_child(&mut self, child_num: u32) -> &mut [u8] {
        let mut num_keys_bytes = [0; INTERNAL_NODE_NUM_KEYS_SIZE];
        num_keys_bytes.copy_from_slice(self.internal_node_num_keys());
        let num_keys = u32::from_le_bytes(num_keys_bytes);

        if child_num > num_keys {
            panic!(
                "Tried to access child_num {} > num_keys {}",
                child_num, num_keys
            );
        } else if child_num == num_keys {
            self.internal_node_right_child()
        } else {
            if INTERNAL_NODE_CHILD_SIZE != INTERNAL_NODE_RIGHT_CHILD_SIZE {
                panic!("INTERNAL_NODE_CHILD_SIZE: {INTERNAL_NODE_CHILD_SIZE} != INTERNAL_NODE_RIGHT_CHILD_SIZE: {INTERNAL_NODE_RIGHT_CHILD_SIZE}")
            }
            &mut self.internal_node_cell(child_num)[..INTERNAL_NODE_RIGHT_CHILD_SIZE]
        }
    }

    pub fn internal_node_key(&mut self, key_num: u32) -> &mut [u8] {
        let start = INTERNAL_NODE_CHILD_SIZE;
        let end = start + INTERNAL_NODE_KEY_SIZE;
        &mut self.internal_node_cell(key_num)[start..end]
    }

    pub fn leaf_node_next_leaf(&mut self) -> &mut [u8] {
        let start = LEAF_NODE_NEXT_LEAF_OFFSET;
        let end = start + LEAF_NODE_NEXT_LEAF_SIZE;
        &mut self.0[start..end]
    }
}
