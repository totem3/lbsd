use std::io::{Read, Write};
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
    Leaf(BTreeLeafNode),
    Internal(BTreeInternalNode),
}

#[derive(Clone)]
pub struct BTreeLeafNode {
    pub node_type: NodeType,
    pub is_root: u8,
    pub parent: u32,
    pub num_cells: u32,
    pub key_values: Vec<KV>,
}

impl BTreeLeafNode {
    pub(crate) fn get_row(&self, cell_num: usize) -> &Row {
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

    pub(crate) fn insert_at(&mut self, index: usize, key: u32, value: Row) {
        if self.num_cells >= Self::max_cells() {
            log::trace!("max cells!");
        }
        let kv = KV { key, value };
        log::trace!("BTreeLeafNode::insert_at: insert at {}. key_values length is {}", index, self.key_values.len());
        self.key_values.insert(index, kv);
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

    pub(crate) fn is_max(&self) -> bool {
        self.num_cells >= Self::max_cells()
    }

    pub(crate) fn max_key(&self) -> u32 {
        match self.key_values.last() {
            Some(kv) => { kv.key }
            None => 0
        }
    }
}

#[derive(Clone)]
pub struct BTreeInternalNode {
    pub node_type: NodeType,
    pub is_root: u8,
    pub parent: u32,
    pub num_keys: u32,
    pub right_child: u32,
    pub key_children: Vec<KC>,
}

impl BTreeInternalNode {
    pub fn new(is_root: u8, parent: u32) -> Self {
        BTreeInternalNode {
            node_type: NodeType::Internal,
            is_root,
            parent,
            num_keys: 0,
            right_child: 0,
            key_children: vec![],
        }
    }

    pub(crate) fn max_key(&self) -> u32 {
        match self.key_children.get(self.num_keys as usize) {
            Some(kc) => { kc.key }
            None => 0
        }
    }

    pub const INTERNAL_SPACE_FOR_CELLS: usize = PAGE_SIZE - 1 - 1 - 4 - 4 - 4;
    pub const INTERNAL_CELL_SIZE: usize = 8;
    pub const INTERNAL_MAX_CELLS: usize = Self::INTERNAL_SPACE_FOR_CELLS / Self::INTERNAL_CELL_SIZE;
    pub(crate) fn insert(&mut self, key: u32, child: u32) {
        if self.num_keys as usize >= Self::INTERNAL_MAX_CELLS {
            unimplemented!("need to implement split!");
        }
        let kc = KC { child, key };
        let index = self.find_insert_position(key);
        self.key_children.insert(index, kc);
        self.num_keys += 1;
    }

    /// 次のページを返す。Leafまで再帰的に辿るのはPagerにやらせる
    pub(crate) fn find_key(&self, key: u32) -> u32 {
        let index = self.find_insert_position(key);

        if self.key_children.len() < index {
            self.key_children[index].key
        } else {
            self.right_child
        }
    }

    fn find_insert_position(&self, key: u32) -> usize {
        let mut left = 0;
        let mut right = self.key_children.len();

        while left != right {
            let index = (left + right) / 2;
            let key_to_right = self.key_children[index].key;
            if key_to_right >= key {
                right = index;
            } else {
                left = index + 1;
            }
        }
        left
    }
}

impl Default for BTreeInternalNode {
    fn default() -> Self {
        BTreeInternalNode::new(0, 0)
    }
}

#[derive(Debug, Clone)]
pub struct KC {
    pub child: u32,
    pub key: u32,
}

impl BTreeNode {
    pub(crate) fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            BTreeNode::Leaf(page) => {
                let _ = buf.write(&[NodeType::Leaf as u8]);
                let _ = buf.write(&[page.is_root]);
                let _ = buf.write_u32::<LittleEndian>(page.parent);
                let _ = buf.write_u32::<LittleEndian>(page.num_cells);
                for key_value in &page.key_values {
                    let _ = buf.write_u32::<LittleEndian>(key_value.key);
                    let mut value = vec![];
                    key_value.value.serialize(&mut value);
                    let _ = buf.write(&value);
                }
            }
            BTreeNode::Internal(page) => {
                let _ = buf.write(&[NodeType::Internal as u8]);
                let _ = buf.write(&[page.is_root]);
                let _ = buf.write_u32::<LittleEndian>(page.parent);
                let _ = buf.write_u32::<LittleEndian>(page.num_keys);
                let _ = buf.write_u32::<LittleEndian>(page.right_child);
                for key_child in &page.key_children {
                    let _ = buf.write_u32::<LittleEndian>(key_child.child);
                    let _ = buf.write_u32::<LittleEndian>(key_child.key);
                }
            }
        };
        if PAGE_SIZE > buf.len() {
            let padding = vec![0; PAGE_SIZE - buf.len()];
            let _ = buf.write(&padding);
        }
    }

    pub(crate) fn is_root(&self) -> u8 {
        match self {
            BTreeNode::Leaf(node) => node.is_root,
            BTreeNode::Internal(node) => node.is_root,
        }
    }

    pub(crate) fn get_parent(&self) -> u32 {
        match self {
            BTreeNode::Leaf(node) => node.parent,
            BTreeNode::Internal(node) => node.parent,
        }
    }

    pub(crate) fn max_key(&self) -> u32 {
        match self {
            BTreeNode::Leaf(node) => node.max_key(),
            BTreeNode::Internal(node) => node.max_key(),
        }
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
        parent: 0,
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
        let mut buf = if buf.len() < 6 {
            trace!("BTreeNode::from::<u8>: given buffer is empty");
            &[1, 1, 0, 0, 0, 0, 0, 0, 0, 0]
        } else {
            buf
        };
        // trace!("BTreeNode::from::<u8>: buf:\n{:?}", buf);
        let node_type = match NodeType::try_from(buf.read_u8().expect("node_type must be u8")) {
            Ok(v) => { v }
            Err(e) => panic!(e),
        };
        trace!("BTreeNode::from::<u8>: node_type: {:?}", node_type);

        let is_root = buf.read_u8().expect("is_root must be u8");
        trace!("BTreeNode::from::<u8>: is_root: {}", is_root);
        let parent: u32 = buf.read_u32::<LittleEndian>().expect("parent must be u32");

        match node_type {
            NodeType::Internal => {
                let num_keys: u32 = buf.read_u32::<LittleEndian>().expect("num_keys must be u32");
                trace!("BTreeNode::from::<u8>: num_cells: {}", num_keys);
                let right_child: u32 = buf.read_u32::<LittleEndian>().expect("right_keys must be u32");
                let mut key_children = vec![];
                for _ in 0..num_keys {
                    let child = buf.read_u32::<LittleEndian>().expect("child must be u32");
                    let key = buf.read_u32::<LittleEndian>().expect("key must be u32");
                    let kc = KC { key, child };
                    key_children.push(kc);
                }
                let node = BTreeInternalNode {
                    node_type,
                    is_root,
                    parent,
                    num_keys,
                    right_child,
                    key_children,
                };
                BTreeNode::Internal(node)
            }
            NodeType::Leaf => {
                let num_cells: u32 = buf.read_u32::<LittleEndian>().expect("num_cells must be u32");
                trace!("BTreeNode::from::<u8>: num_cells: {}", num_cells);
                let mut key_values = vec![];
                for _ in 0..num_cells {
                    let key = buf.read_u32::<LittleEndian>().expect("key must be u32");
                    let mut row_buffer = vec![0; ROW_SIZE];
                    let n = buf.read(&mut row_buffer).expect("read row failed");
                    trace!("BTreeNode::from:::<u8>: read row bytes: {}", n);
                    let value = Row::deserialize(&row_buffer);
                    let kv = KV { key, value };
                    key_values.push(kv);
                }
                let node: BTreeLeafNode = BTreeLeafNode {
                    node_type,
                    is_root,
                    parent,
                    num_cells,
                    key_values,
                };
                BTreeNode::Leaf(node)
            }
        }
    }
}