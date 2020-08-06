use std::io::{Write};
use crate::{Row, ROW_SIZE, PAGE_SIZE};
use byteorder::{ReadBytesExt, LittleEndian, WriteBytesExt};
use std::convert::TryFrom;
use std::borrow::{Borrow, BorrowMut};
use log::trace;

#[derive(Clone, Debug)]
pub enum NodeType {
    Leaf = 0,
    Internal = 1,
}

impl TryFrom<u8> for NodeType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value == NodeType::Leaf as u8 {
            Ok(NodeType::Leaf)
        } else if value == NodeType::Internal as u8 {
            Ok(NodeType::Internal)
        } else {
            Err(format!("unknown node type: {}", value))
        }
    }
}

#[derive(Clone)]
pub enum BTreeNode {
    Leaf(BTreeLeafNode)
}

#[derive(Clone)]
pub struct BTreeLeafNode {
    pub node_type: NodeType,
    pub is_root: u8,
    pub parent: Option<Box<BTreeNode>>,
    pub num_cells: u32,
    pub key_values: Vec<KV>,
}

impl BTreeLeafNode {
    pub(crate) fn get_row(&mut self, cell_num: usize) -> &Row {
        let diff = (cell_num + 1) - (self.num_cells as usize);
        for _ in 0..diff {
            let new_row = Row::default();
            let kv = KV { key: 0, value: new_row };
            self.key_values.push(kv);
        }
        self.num_cells = self.key_values.len() as u32;
        self.key_values[cell_num].value.borrow()
    }

    pub(crate) fn get_row_mut(&mut self, cell_num: usize) -> &mut Row {
        trace!("BTreeLeafNode.get_row_mut");
        trace!("BTreeLeafNode.get_row_mut: cell_num: {}", cell_num);
        let diff = (cell_num + 1) - (self.num_cells as usize);
        trace!("BTreeLeafNode.get_row_mut: diff: {}", diff);
        for i in 0..diff {
            trace!("BTreeLeafNode.get_row_mut: insert kv {}", i);
            let new_row = Row::default();
            let kv = KV { key: 0, value: new_row };
            self.key_values.push(kv);
        }
        trace!("BTreeLeafNode.get_row_mut: key_values len: {}", self.key_values.len());
        self.num_cells = self.key_values.len() as u32;
        self.key_values[cell_num].value.borrow_mut()
    }

    pub(crate) fn insert(&mut self, key: u32, value: Row) {
        if self.num_cells >= Self::max_cells() {
            panic!("max cells!");
        }
        let kv = KV { key, value };
        self.key_values.push(kv);
        self.num_cells += 1;
    }

    pub const NODE_TYPE_SIZE: usize = 1;
    pub const IS_ROOT_SIZE: usize = 1;
    pub const NUM_CELLS_SIZE: usize = 4;
    pub const NODE_HEADER_SIZE: usize = Self::NODE_TYPE_SIZE + Self::IS_ROOT_SIZE + Self::NUM_CELLS_SIZE;
    pub const NODE_KEY_SIZE: usize = 4;
    pub const NODE_CELL_SIZE: usize = Self::NODE_KEY_SIZE + ROW_SIZE;
    pub const NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - Self::NODE_HEADER_SIZE;
    pub const NODE_MAX_CELLS: usize = Self::NODE_SPACE_FOR_CELLS / Self::NODE_CELL_SIZE;
    fn max_cells() -> u32 {
        Self::NODE_MAX_CELLS as u32
    }
}

impl BTreeNode {
    pub(crate) fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            BTreeNode::Leaf(leaf) => {
                let _ = buf.write(&[NodeType::Leaf as u8]);
                let _ = buf.write(&[leaf.is_root]);
                let _ = buf.write_u32::<LittleEndian>(leaf.num_cells);
                for key_value in &leaf.key_values {
                    let _ = buf.write_u32::<LittleEndian>(key_value.key);
                    let mut value = vec![];
                    key_value.value.serialize(&mut value);
                    let _ = buf.write(&value);
                }
            }
        };
    }
}

#[test]
fn test_serialize() {
    let row = Row {
        id: 1,
        username: *b"foo\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
        email: *b"bar\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
    };
    let key_value = KV { key: 1, value: row };
    let node = BTreeNode::Leaf(BTreeLeafNode {
        node_type: NodeType::Leaf,
        is_root: 0,
        parent: None,
        num_cells: 1,
        key_values: vec![key_value],
    });

    let mut buf = vec![];
    node.serialize(&mut buf);
    eprintln!("buf: {:?}", buf);
}

#[derive(Debug, Clone)]
pub struct KV {
    pub(crate) key: u32,
    pub(crate) value: crate::Row,
}

impl From<&[u8]> for BTreeNode {
    fn from(buf: &[u8]) -> Self {
        trace!("BTreeNode::from::<u8>");
        // 空のバッファが渡されたらLeafとして初期化する
        let buf = if buf.len() < 6 {
            trace!("BTreeNode::from::<u8>: given buffer is empty");
            &[1, 1, 0, 0, 0, 0]
        } else {
            buf
        };
        trace!("BTreeNode::from::<u8>: buf:\n{:?}", buf);
        let node_type = match NodeType::try_from(buf[0]) {
            Ok(v) => { v }
            Err(e) => panic!(e),
        };
        trace!("BTreeNode::from::<u8>: node_type: {:?}", node_type);

        let is_root = buf[1];
        trace!("BTreeNode::from::<u8>: is_root: {}", is_root);
        let parent = None; // FIXME どうすればいいのかよくわからん
        let num_cells: u32 = (&buf[2..6]).read_u32::<LittleEndian>().expect("num_cells must be u32");
        trace!("BTreeNode::from::<u8>: num_cells: {}", num_cells);

        match node_type {
            NodeType::Internal => {
                unimplemented!()
            }
            NodeType::Leaf => {
                let mut index = 6;
                let mut key_values = vec![];
                for _ in 0..num_cells {
                    let key = (&buf[index..index + 4]).read_u32::<LittleEndian>().expect("key must be u32");
                    index += 4;
                    let value = Row::deserialize(&buf[index..index + ROW_SIZE]);
                    index += ROW_SIZE;
                    let kv = KV { key, value };
                    key_values.push(kv);
                }
                let leaf: BTreeLeafNode = BTreeLeafNode {
                    node_type,
                    is_root,
                    parent,
                    num_cells,
                    key_values,
                };
                BTreeNode::Leaf(leaf)
            }
        }
    }
}