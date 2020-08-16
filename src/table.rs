use std::path::Path;
use log::{trace};
use crate::{ROWS_PER_PAGE, ROW_SIZE, TABLE_MAX_PAGES, PAGE_SIZE, Row};
use crate::tree::{BTreeNode, BTreeLeafNode, BTreeInternalNode};
use std::fs::{File, OpenOptions};
use std::fs;
use std::convert::TryInto;
use std::io::{Seek, Write, SeekFrom, Read};

pub(crate) struct Table {
    pub(crate) pager: Pager,
    pub(crate) root_page_num: usize,
}

impl Table {
    pub(crate) fn new<P>(filename: P) -> Result<Self, String>
        where
            P: AsRef<Path>,
    {
        let mut pager = Pager::new(&filename)?;
        trace!("Table::new: initialize Table for {:?}", &filename.as_ref().display());
        let mut root_page_num = 0;
        if pager.num_pages == 1 {
            trace!("Table::new: new_table, initialize it");
            if let Some(BTreeNode::Leaf(node)) = pager.get_page_mut(0) {
                node.is_root = 1
            }
        } else {
            for num_page in 0..pager.num_pages {
                if let Some(page) = pager.get_page(num_page) {
                    if page.is_root() > 0 {
                        root_page_num = num_page;
                    }
                }
            }
        }
        trace!("Table::new: root_page_num: {}", root_page_num);
        Ok(Table { pager, root_page_num })
    }

    pub(crate) fn page_num(&self, row_num: usize) -> usize {
        row_num / ROWS_PER_PAGE
    }

    pub(crate) fn bytes_offset(&self, row_num: usize) -> usize {
        let rows = row_num % ROWS_PER_PAGE;
        rows * ROW_SIZE
    }

    pub(crate) fn close(&mut self) -> Result<(), String> {
        self.pager.flush()
    }
}

type Page = BTreeNode;

pub(crate) struct Pager {
    file: File,
    file_length: usize,
    pages: Vec<Option<Page>>,
    num_pages: usize,
}

impl Pager {
    pub(crate) fn new(filename: impl AsRef<Path>) -> Result<Self, String> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&filename)
        {
            Ok(v) => v,
            Err(e) => return Err(format!("{}", e)),
        };
        let metadata = match fs::metadata(&filename) {
            Ok(v) => v,
            Err(e) => return Err(format!("{}", e)),
        };
        let file_length = metadata.len().try_into().unwrap();
        let pages = vec![None; TABLE_MAX_PAGES];
        trace!("file_length: {}", file_length);
        trace!("PAGE_SIZE: {}", PAGE_SIZE);
        let num_pages = ::std::cmp::max(((file_length as f32) / (PAGE_SIZE as f32)).ceil() as usize, 1);
        trace!("num_pages: {}", num_pages);
        Ok(Pager {
            file,
            file_length,
            pages,
            num_pages,
        })
    }

    pub(crate) fn new_internal_page(&mut self, new_page_num: usize) -> Option<&Page> {
        log::trace!("new_page");
        trace!("new_page: page_num: {}", new_page_num);
        let page = BTreeNode::Internal(BTreeInternalNode::default());
        self.pages[new_page_num] = Some(page);
        self.pages[new_page_num].as_ref()
    }

    pub(crate) fn new_internal_page_mut(&mut self, new_page_num: usize) -> Option<&mut Page> {
        log::trace!("new_page");
        trace!("new_page: page_num: {}", new_page_num);
        let page = BTreeNode::Internal(BTreeInternalNode::default());
        self.pages[new_page_num] = Some(page);
        self.pages[new_page_num].as_mut()
    }

    pub(crate) fn get_page(&mut self, page_num: usize) -> Option<&Page> {
        log::trace!("get_page");
        if self.pages[page_num].is_some() {
            log::trace!("get_page: page is already on memory. return");
            return self.pages[page_num].as_ref();
        };
        log::trace!("get_page: page is not on memory. try to read from file");
        let mut num_pages = self.file_length / PAGE_SIZE;
        trace!("get_page: num_pages: {}", num_pages);
        if self.file_length % PAGE_SIZE != 0 {
            num_pages += 1;
        }
        trace!("get_page: page_num: {}", page_num);
        if page_num <= num_pages {
            trace!("page_num is equal to or smaller than num_pages");
            match self
                .file
                .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            {
                Ok(_) => {
                    trace!("get_page: seek to {}", page_num * PAGE_SIZE);
                }
                Err(e) => {
                    log::error!("seek failed! {}", e);
                    panic!("seek failed! {}", e);
                }
            };
        }
        let mut buf = vec![0u8; PAGE_SIZE];
        // fileのサイズを超えていたら何も読み込まない（けど試行するだけむだなので FIXME )
        match self.file.read(&mut buf) {
            Ok(n) => {
                trace!("read from file succeeded. read {} bytes", n);
            }
            Err(e) => {
                log::error!("read failed! {}", e);
                panic!("read failed! {}", e);
            }
        };
        let page = BTreeNode::from(buf.as_ref());
        self.pages[page_num] = Some(page);
        self.pages[page_num].as_ref()
    }

    pub(crate) fn get_page_mut(&mut self, page_num: usize) -> Option<&mut Page> {
        let _ = self.get_page(page_num);
        self.pages[page_num].as_mut()
    }

    fn flush_page(&mut self, page_num: usize) -> Result<usize, String> {
        if let Some(page) = &self.pages[page_num] {
            let mut buf = vec![];
            page.serialize(&mut buf);
            self.file.write(&buf).map_err(|e| e.to_string())
        } else {
            Err("Page not exists".to_string())
        }
    }

    fn flush(&mut self) -> Result<(), String> {
        trace!("Pager::flush");
        let _ = self.file.seek(SeekFrom::Start(0));
        trace!("Pager::flush: num_pages: {}", self.num_pages);
        for i in 0..self.num_pages {
            match self.flush_page(i) {
                Ok(n) =>
                    { log::trace!("write {} bytes to file", n) }
                Err(e) => {
                    log::error!("failed to write file: {}", e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn get_num_pages(&self) -> usize {
        self.file_length / ROW_SIZE
    }

    const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (BTreeLeafNode::NODE_MAX_CELLS + 1) / 2;
    const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (BTreeLeafNode::NODE_MAX_CELLS + 1) - Self::LEAF_NODE_RIGHT_SPLIT_COUNT;

    pub(crate) fn split_and_insert(&mut self, page_num: usize, cell_num: usize, key: u32, value: Row) -> Option<usize> {
        trace!("Pager::split_and_insert!");
        let old_node = self.get_page_mut(page_num).expect("split_and_insert: current page not found!");
        let mut parent_page_num = if old_node.is_root() > 0 {
            // rootの場合は後で新しい親を作る
            0
        } else {
            // 親がいる場合はまずその親を使う
            old_node.get_parent() as usize
        };
        let right_values;
        trace!("target page_num: {}", page_num);
        trace!("target cell_num: {}", cell_num);

        // ここでコピーしないとmutな借用をし続けてしまうのでコピーしておく
        let original_is_root = old_node.is_root();
        let original_parent = old_node.get_parent();
        let left_max_key;

        if let BTreeNode::Leaf(node) = old_node {
            trace!("Pager::split_and_insert: just insert to old node");
            node.insert_at(cell_num, key, value);
            trace!("Pager::split_and_insert: split current key_values");
            let cur_kv = node.key_values.clone();
            let (left, right) = cur_kv.split_at(Self::LEAF_NODE_LEFT_SPLIT_COUNT);
            node.key_values = left.to_vec();
            node.num_cells = Self::LEAF_NODE_LEFT_SPLIT_COUNT as u32;
            node.is_root = 0;
            trace!("Pager::split_and_insert: left num_cells: {}", node.num_cells);
            trace!("Pager::split_and_insert: left parent: {}", node.parent);
            left_max_key = node.max_key();
            right_values = right.to_vec();
        } else {
            unimplemented!("need to implement split internal node!");
        }

        let right_page_num = self.new_page_num();
        if original_is_root > 0 {
            parent_page_num = self.new_page_num();
            let new_parent = self.new_internal_page_mut(parent_page_num).expect("split_and_insert: failed to allocate new parent!");
            if let BTreeNode::Internal(node) = new_parent {
                node.is_root = original_is_root;
                node.parent = original_parent;
                node.right_child = right_page_num as u32;
                node.insert(left_max_key, page_num as u32)
            }
            let old_node = self.get_page_mut(page_num).expect("split_and_insert: current page not found!");
            if let BTreeNode::Leaf(node) = old_node {
                node.parent = parent_page_num as u32;
            } else {
                unimplemented!("need to implement split internal node!");
            }
        } else {
            match self.get_page_mut(original_parent as usize) {
                Some(BTreeNode::Internal(node)) => {
                    node.insert(left_max_key, page_num as u32);
                    if node.right_child == page_num as u32 {
                        node.right_child = right_page_num as u32;
                    }
                }
                Some(_) => {
                    unreachable!("Pager::split_and_insert: original parent is leaf node");
                }
                None => {
                    unreachable!("Pager::split_and_insert: original parent does not exist");
                }
            }
        }
        let new_node = self.get_page_mut(right_page_num).expect("split_and_insert: failed to allocate new page!");
        if let BTreeNode::Leaf(node) = new_node {
            node.key_values = right_values;
            node.num_cells = Self::LEAF_NODE_RIGHT_SPLIT_COUNT as u32;
            node.parent = parent_page_num as u32;
        } else {
            unreachable!("new node must be leaf");
        }

        trace!("Pager::split_and_insert: done");
        if original_is_root > 0 {
            trace!("Pager::split_and_insert: original was root");
            Some(parent_page_num)
        } else {
            trace!("Pager::split_and_insert: original was not root");
            None
        }
    }

    // とりあえず今は末尾を返す
    fn new_page_num(&mut self) -> usize {
        let val = self.num_pages;
        self.num_pages += 1;
        val
    }

    // pub(crate) fn find_key(&mut self, start_page_num: u32, key: u32) -> Option<u32> {
    //     match self.get_page() {
    //         Some(page) => page.find_key
    //     }
    // }
}

pub(crate) struct Cursor<'a> {
    pub(crate) table: &'a mut Table,
    pub(crate) page_num: usize,
    pub(crate) cell_num: usize,
    pub(crate) end_of_table: bool,
}

struct CursorOpts {
    page_num: usize,
    cell_num: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub(crate) fn table_start(table: &'a mut Table) -> Self {
        trace!("table_start");
        let cell_num = 0;
        trace!("table_start: cell_num: {}", cell_num);
        let page_num = table.root_page_num;
        trace!("table_start: page_num: {}", page_num);
        let end_of_table = table.pager.get_page(table.root_page_num).map_or(false, |page| {
            match page {
                BTreeNode::Leaf(page) => {
                    page.num_cells == 0
                }
                BTreeNode::Internal(_) => {
                    false
                }
            }
        });
        trace!("table_start: end_of_table: {}", end_of_table);
        Cursor {
            table,
            page_num,
            cell_num,
            end_of_table,
        }
    }

    pub(crate) fn find_insert_position(table: &'a mut Table, page_num: usize, key: u32) -> Self {
        trace!("find_insert_position");
        match table.pager.get_page(page_num) {
            Some(BTreeNode::Leaf(page)) => {
                let mut left = 0;
                let mut right = page.num_cells as usize;
                let mut cursor_opts = CursorOpts {
                    page_num,
                    cell_num: 0,
                    end_of_table: false,
                };
                while left != right {
                    trace!("find_insert_position: left: {}", left);
                    trace!("find_insert_position: right: {}", right);
                    let index = (left + right) / 2;
                    let current_key = page.key_values[index].key;
                    if key == current_key {
                        cursor_opts.cell_num = index;
                        trace!("find_insert_position: key == current_key: {}", key);
                        break;
                    }

                    if key < current_key {
                        right = index;
                    } else {
                        left = index + 1;
                    }
                    cursor_opts.cell_num = left;
                }
                trace!("find_insert_position: cursor position: {}", cursor_opts.cell_num);
                cursor_opts.end_of_table = page.num_cells == left as u32;

                Self {
                    table,
                    page_num: cursor_opts.page_num,
                    cell_num: cursor_opts.cell_num,
                    end_of_table: cursor_opts.end_of_table,
                }
            }
            Some(BTreeNode::Internal(page)) => {
                let next_page_num = page.find_key(key);
                Self::find_insert_position(table, next_page_num as usize, key)
            }
            None => panic!("page not found"),
        }
    }

    pub(crate) fn advance(&mut self) {
        trace!("advance");
        let page_num = self.page_num;
        let node = self.table.pager.get_page(page_num).expect("page not found!!");
        trace!("advance: before cell_num: {}", self.cell_num);
        self.cell_num += 1;
        trace!("advance: after cell_num: {}", self.cell_num);
        match node {
            BTreeNode::Leaf(leaf) => {
                if self.cell_num >= leaf.num_cells as usize {
                    if node.is_root() > 0 {
                        self.end_of_table = true
                    } else {
                        self.page_num = leaf.parent as usize;
                        match self.get_page() {
                            Some(BTreeNode::Internal(parent)) => {
                                trace!("advance: go up to parent");
                                let mut is_next = false;
                                let mut next_child = None;
                                for kc in &parent.key_children {
                                    if is_next {
                                        next_child = Some(kc.child);
                                    }
                                    if kc.child == page_num as u32 {
                                        is_next = true;
                                    }
                                }
                                self.page_num = match next_child {
                                    Some(v) => {
                                        trace!("advance: choose next child. page_num is {}", v);
                                        v
                                    }
                                    None => {
                                        trace!("advance: choose right_child. page_num is {}", parent.right_child);
                                        parent.right_child
                                    }
                                } as usize;
                                if self.page_num == page_num {
                                    self.end_of_table = true;
                                } else {
                                    self.cell_num = 0;
                                }
                            }
                            Some(_) => {
                                unreachable!("Cursor::advance: parent is not internal node")
                            }
                            None => {
                                unreachable!("Cursor::advance: non root but parent not found")
                            }
                        }
                    }
                }
            }
            BTreeNode::Internal(_) => { unimplemented!() }
        }
    }

    fn get_row_mut(&mut self) -> Option<&mut Row> {
        trace!("TCursor::get_row_mut");
        let page_num = self.page_num;
        trace!("TCursor::get_row_mut: page_num: {}", page_num);
        let cell_num = self.cell_num;
        match self.table.pager.get_page_mut(page_num) {
            Some(BTreeNode::Leaf(page)) => {
                Some(page.get_row_mut(cell_num))
            }
            _ => None,
        }
    }

    pub(crate) fn get_page_mut(&mut self) -> Option<&mut Page> {
        trace!("TCursor::get_mut");
        let page_num = self.page_num;
        trace!("TCursor::get_mut: page_num: {}", page_num);
        self.table.pager.get_page_mut(page_num)
    }

    pub(crate) fn get_row(&mut self) -> Option<&Row> {
        trace!("TCursor::get_row");
        let page_num = self.page_num;
        trace!("TCursor::get_row page_num: {}", page_num);
        let cell_num = self.cell_num;
        self.table.pager.get_page_mut(page_num).map(|page| {
            match page {
                BTreeNode::Leaf(page) => {
                    page.get_row(cell_num)
                }
                BTreeNode::Internal(_) => { unimplemented!() }
            }
        })
    }

    pub(crate) fn get_page(&mut self) -> Option<&Page> {
        trace!("TCursor::get");
        let page_num = self.page_num;
        trace!("TCursor::get page_num: {}", page_num);
        self.table.pager.get_page(page_num)
    }

    pub(crate) fn split_and_insert(&mut self, key: u32, value: Row) -> Option<usize> {
        trace!("TCursor::split_and_insert");
        self.table.pager.split_and_insert(self.page_num, self.cell_num, key, value)
    }
}

