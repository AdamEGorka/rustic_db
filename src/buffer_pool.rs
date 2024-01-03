use crate::database;
use crate::heap_page::{HeapPage, HeapPageId, Permission};
use crate::lock_manager::LockManager;
use crate::transaction::TransactionId;
use crate::tuple::Tuple;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};

pub const PAGE_SIZE: usize = 4096;
pub const DEFAULT_PAGES: usize = 50;

// Cache of pages kept in memory
pub struct BufferPool {
    id_to_page: RwLock<HashMap<HeapPageId, Arc<RwLock<HeapPage>>>>,
    lock_manager: LockManager,
    num_pages: usize,
}

impl BufferPool {
    pub fn new() -> Self {
        BufferPool {
            id_to_page: RwLock::new(HashMap::new()),
            num_pages: DEFAULT_PAGES,
            lock_manager: LockManager::new(),
        }
    }

    // Retrieves the specified page from cache or disk
    pub fn get_page(
        &self,
        tid: TransactionId,
        pid: HeapPageId,
        perm: Permission,
    ) -> Option<Arc<RwLock<HeapPage>>> {
        let exclusive = perm == Permission::Write;
        self.lock_manager.acquire_lock(tid, pid, exclusive);

        {
            let id_to_page = self.id_to_page.read().unwrap();
            if id_to_page.contains_key(&pid) {
                return Some(Arc::clone(id_to_page.get(&pid).unwrap()));
            }
        }
        // read the page from disk and saves it to the buffer pool
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(pid.get_table_id()).unwrap();
        let page = table.read_page(&pid);
        let mut id_to_page = self.id_to_page.write().unwrap();
        id_to_page.insert(pid, Arc::new(RwLock::new(page)));
        Some(Arc::clone(id_to_page.get(&pid).unwrap()))
    }

    // Commits the specified transaction, writes all dirty pages to disk, and releases all locks
    pub fn commit_transaction(&self, tid: TransactionId) {
        let locked_pages = self.lock_manager.get_locked_pages(tid);
        for pid in locked_pages {
            if self.id_to_page.read().unwrap().contains_key(&pid) {
                let id_to_page = self.id_to_page.read().unwrap();
                let page = id_to_page.get(&pid).unwrap();
                let mut page = page.write().unwrap();
                if page.is_dirty() {
                    let db = database::get_global_db();
                    let catalog = db.get_catalog();
                    let table = catalog.get_table_from_id(pid.get_table_id()).unwrap();
                    table.write_page(&page);
                    page.mark_dirty(false, tid);
                    page.set_before_image();
                }
            }
        }
        self.lock_manager.release_locks(tid);
    }

    // Aborts the specified transaction, reverting any changes made, and releases all locks
    pub fn abort_transaction(&self, tid: TransactionId) {
        let locked_pages = self.lock_manager.get_locked_pages(tid);
        for pid in locked_pages {
            if self.id_to_page.read().unwrap().contains_key(&pid) {
                let id_to_page = self.id_to_page.read().unwrap();
                let page = id_to_page.get(&pid).unwrap();
                let mut page = page.write().unwrap();
                if page.is_dirty() {
                    // revert the page to its original state
                    *page = page.get_before_image();
                    page.mark_dirty(false, tid)
                }
            }
        }
        self.lock_manager.release_locks(tid);
    }

    // Adds the tuple to the specified table
    pub fn insert_tuple(&self, tid: TransactionId, table_id: usize, tuple: Tuple) {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.add_tuple(tid, tuple);
    }

    // TODO: Deletes the tuple from the specified table
    pub fn delete_tuple(&mut self, tid: TransactionId, table_id: usize, tuple: Tuple) {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        // TODO: get table by record id
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.delete_tuple(tid, tuple);
    }

    // Gets the number of pages in the buffer pool
    pub fn get_num_pages(&self) -> usize {
        self.num_pages
    }
}
