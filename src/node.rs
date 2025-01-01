use std::{cell::RefCell, fmt};

use crate::{pager::PAGE_SIZE, row::ROW_SIZE};

// Common Node Header Layout
pub const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
pub const NODE_TYPE_OFFSET: usize = 0;
pub const IS_ROOT_SIZE: usize = std::mem::size_of::<bool>();
pub const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
pub const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
pub const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
pub const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf Node Body Layout
pub const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_KEY_OFFSET: usize = 0;
pub const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
pub const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

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
/// |-------------+----------------+----------------+-----------|
/// | byte 0      | byte 1         | bytes 2-5      | bytes 6-9 |
/// | node_type   | is_root        | parent_pointer | num_cells |
/// |-------------+----------------+----------------+-----------|
/// | bytes 10-13                  | bytes 14-304               |
/// | key 0                        | value 0                    |
/// |------------------------------+----------------------------|
/// | bytes 305-308                | bytes 309-601              |
/// | key 1                        | value 1                    |
/// |------------------------------+----------------------------|
/// |             ...              |          ...               |
/// |------------------------------+----------------------------|
/// | bytes 3550-3553              | bytes 3554-3844            |
/// | key 12                       | value 12                   |
/// |------------------------------+----------------------------|
/// |                       bytes 3845-4095                     |
/// |                         wasted space                      |
/// |-----------------------------------------------------------|
pub struct Node(pub [u8; PAGE_SIZE]);

impl Node {
    pub fn initialize_leaf_node() -> Self {
        let mut node = Self([0; PAGE_SIZE]);
        Node::set_node_type(&mut node, NodeType::Leaf);
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
}

pub struct NodeProxy<'a>(RefCell<&'a mut Node>);

impl<'a> NodeProxy<'a> {
    pub fn new(node: &'a mut Node) -> Self {
        Self(RefCell::new(node))
    }
}

impl<'a> fmt::Display for NodeProxy<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut node = self.0.borrow_mut();

        let mut num_cells_bytes = [0; LEAF_NODE_NUM_CELLS_SIZE];
        num_cells_bytes.copy_from_slice(node.leaf_node_num_cells());
        let num_cells = u32::from_le_bytes(num_cells_bytes);
        writeln!(f, "leaf (size {num_cells})")?;

        for i in 0..num_cells {
            let mut key_bytes = [0; LEAF_NODE_KEY_SIZE];
            key_bytes.copy_from_slice(node.leaf_node_key(i));
            let key = u32::from_le_bytes(key_bytes);
            writeln!(f, "  - {i} : {key}")?;
        }

        Ok(())
    }
}
