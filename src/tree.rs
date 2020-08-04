pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

pub enum BTreeNode<'a> {
    Leaf(BTreeLeafNode<'a>)
}

pub struct BTreeLeafNode<'a> {
    node_type: NodeType,
    is_root: u8,
    parent: Option<Box<BTreeNode<'a>>>,
    num_cells: u32,
    key_values: Vec<KV<'a>>
}

pub struct KV<'a> {
    key: &'a str,
    value: &'a str,
}
