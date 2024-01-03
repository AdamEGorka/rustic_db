use crate::buffer_pool::PAGE_SIZE;
use crate::database;
use crate::heap_page::{HeapPage, HeapPageId, Permission};
use crate::transaction::TransactionId;
use crate::tuple::{Tuple, TupleDesc};

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, RwLock};
use uuid::Uuid;

// Representation of a table stored in a file on disk
pub struct HeapFile {
    file: Mutex<File>,
    td: TupleDesc,
    id: usize,
}

impl HeapFile {
    pub fn new(file: File, td: TupleDesc) -> Self {
        HeapFile {
            file: Mutex::new(file),
            td,
            id: Uuid::new_v4().as_u128() as usize,
        }
    }

    // Retrieves the unique id of this table
    pub fn get_id(&self) -> usize {
        self.id
    }

    // Retrieves the tuple descriptor for this table
    pub fn get_tuple_desc(&self) -> &TupleDesc {
        &self.td
    }

    // Retrieves the page with the specified pid from disk
    pub fn read_page(&self, pid: &HeapPageId) -> HeapPage {
        let mut data = vec![0; PAGE_SIZE];
        let mut file = self.file.lock().unwrap();
        let mut num_pages =
            (file.metadata().unwrap().len() as f64 / PAGE_SIZE as f64).ceil() as usize;
        let page_no = pid.get_page_number();
        while num_pages <= page_no {
            file.seek(SeekFrom::Start((num_pages * PAGE_SIZE) as u64))
                .unwrap();
            file.write_all(&data).unwrap();
            num_pages += 1;
        }

        file.seek(SeekFrom::Start((page_no * PAGE_SIZE) as u64))
            .unwrap();
        file.read_exact(&mut data).unwrap();
        HeapPage::new(*pid, data, self.td.clone())
    }

    // Writes the specified page to disk
    pub fn write_page(&self, page: &HeapPage) {
        let pid = page.get_id();
        let data = page.get_page_data();
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start((pid.get_page_number() * PAGE_SIZE) as u64))
            .unwrap();
        file.write_all(&data).unwrap();
    }

    // Calculates the number of pages in this HeapFile
    pub fn num_pages(&self) -> usize {
        let file = self.file.lock().unwrap();
        (file.metadata().unwrap().len() as f64 / PAGE_SIZE as f64).ceil() as usize
    }

    // Adds the specified tuple to the file
    pub fn add_tuple(&self, tid: TransactionId, tuple: Tuple) {
        let table_id = self.get_id();
        let db = database::get_global_db();
        let bp = db.get_buffer_pool();
        let mut page_no = 0;

        // find the first page with an empty slot
        loop {
            let pid = HeapPageId::new(table_id, page_no);
            let page = bp.get_page(tid, pid, Permission::Read).unwrap();
            let page_read = page.read().unwrap();
            if page_read.get_num_empty_slots() > 0 {
                drop(page_read);
                let page = bp.get_page(tid, pid, Permission::Write).unwrap();
                let mut page_writer = page.write().unwrap();
                page_writer.add_tuple(tuple).unwrap();
                page_writer.mark_dirty(true, tid);
                return;
            }
            page_no += 1;
        }
    }

    // TODO: Deletes the specified tuple from the file
    pub fn delete_tuple(&self, tid: TransactionId, tuple: Tuple) {
        let db = database::get_global_db();
        let bp = db.get_buffer_pool();
        let rid = tuple.get_record_id();
        let pid = rid.get_page_id();
        let page = bp.get_page(tid, pid, Permission::Write).unwrap();
        let mut page_writer = page.write().unwrap();
        page_writer.delete_tuple(tuple).unwrap();
        page_writer.mark_dirty(true, tid);
    }

    // Retrieves an iterator over the pages in this file
    pub fn iter(&self, tid: TransactionId) -> HeapFileIterator {
        HeapFileIterator {
            heap_file: self,
            current_page_index: 0,
            tid,
        }
    }

    // Retrieves an iterator over the pages in this file
    pub fn iter_mut(&self, tid: TransactionId) -> HeapFileIteratorMut {
        HeapFileIteratorMut {
            heap_file: self,
            current_page_index: 0,
            tid,
        }
    }
}

pub struct HeapFileIterator<'a> {
    heap_file: &'a HeapFile,
    current_page_index: usize,
    tid: TransactionId,
}

impl<'a> Iterator for HeapFileIterator<'a> {
    type Item = Arc<RwLock<HeapPage>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_index < self.heap_file.num_pages() {
            let pid = HeapPageId::new(self.heap_file.get_id(), self.current_page_index);
            let db = database::get_global_db();
            let bp = db.get_buffer_pool();
            let page = bp.get_page(self.tid, pid, Permission::Read).unwrap();
            self.current_page_index += 1;
            Some(page)
        } else {
            None
        }
    }
}

pub struct HeapFileIteratorMut<'a> {
    heap_file: &'a HeapFile,
    current_page_index: usize,
    tid: TransactionId,
}

impl<'a> Iterator for HeapFileIteratorMut<'a> {
    type Item = Arc<RwLock<HeapPage>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_index < self.heap_file.num_pages() {
            let pid = HeapPageId::new(self.heap_file.get_id(), self.current_page_index);
            let db = database::get_global_db();
            let bp = db.get_buffer_pool();
            let page = bp.get_page(self.tid, pid, Permission::Write).unwrap();
            self.current_page_index += 1;
            Some(page)
        } else {
            None
        }
    }
}
