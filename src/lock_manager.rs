use crate::database;
use crate::heap_page::HeapPageId;
use crate::heap_page::Permission;
use crate::transaction::TransactionId;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use std::thread;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
struct Lock {
    tid: TransactionId,
    pid: HeapPageId,
    exclusive: bool,
}

pub struct LockManager {
    page_to_locks: RwLock<HashMap<HeapPageId, HashSet<Lock>>>,
    transaction_to_locks: RwLock<HashMap<TransactionId, HashSet<Lock>>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            page_to_locks: RwLock::new(HashMap::new()),
            transaction_to_locks: RwLock::new(HashMap::new()),
        }
    }

    // Acquires a lock on the specified page for the specified transaction
    pub fn acquire_lock(&self, tid: TransactionId, pid: HeapPageId, exclusive: bool) {
        // early return if the transaction already has the appropriate lock
        {
            let transaction_locks = self.transaction_to_locks.read().unwrap();
            if transaction_locks.contains_key(&tid) {
                let locks = transaction_locks.get(&tid).unwrap();
                for lock in locks {
                    if lock.pid == pid && (lock.exclusive == exclusive || !exclusive) {
                        return;
                    }
                }
            }
        }
        // check if there is a conflicting lock on the page
        loop {
            let mut page_to_locks = self.page_to_locks.write().unwrap();
            let mut transaction_to_locks = self.transaction_to_locks.write().unwrap();

            if let Some(locks) = page_to_locks.get(&pid) {
                // upgrade the lock if the transaction already has a lock on the page
                if locks.len() == 1 && locks.iter().next().unwrap().tid == tid {
                    if exclusive {
                        self.upgrade_lock(
                            tid,
                            pid,
                            page_to_locks.borrow_mut(),
                            transaction_to_locks.borrow_mut(),
                        );
                    }
                    return;
                }
                // conflict if there are others locks when we want an exclusive lock
                let mut conflict = exclusive && !locks.is_empty();
                // or if there is an exclusive lock and we want any lock
                conflict = conflict || locks.iter().any(|lock| lock.exclusive);

                if conflict {
                    let abort = locks.iter().any(|lock| lock.tid < tid);
                    drop(page_to_locks);
                    drop(transaction_to_locks);
                    if abort {
                        // abort the transaction
                        let db = database::get_global_db();
                        let bp = db.get_buffer_pool();
                        bp.abort_transaction(tid);
                        panic!("Transaction {:?} aborted", tid);
                    }
                    // wait for the lock to be released
                    thread::sleep(std::time::Duration::from_millis(500));
                    continue;
                }
            }
            // add the lock to the page and transaction
            let page_locks = page_to_locks.entry(pid).or_insert(HashSet::new());
            let transaction_locks = transaction_to_locks.entry(tid).or_insert(HashSet::new());
            page_locks.insert(Lock {
                tid,
                exclusive,
                pid,
            });
            transaction_locks.insert(Lock {
                tid,
                exclusive,
                pid,
            });
            return;
        }
    }

    // Upgrades a lock from read to write
    fn upgrade_lock(
        &self,
        tid: TransactionId,
        pid: HeapPageId,
        page_to_locks: &mut RwLockWriteGuard<HashMap<HeapPageId, HashSet<Lock>>>,
        transaction_to_locks: &mut RwLockWriteGuard<HashMap<TransactionId, HashSet<Lock>>>,
    ) {
        let page_locks = page_to_locks.get_mut(&pid).unwrap();
        let transaction_locks = transaction_to_locks.get_mut(&tid).unwrap();
        let old_lock = Lock {
            tid,
            pid,
            exclusive: false,
        };
        let new_lock = Lock {
            tid,
            pid,
            exclusive: true,
        };
        page_locks.remove(&old_lock);
        page_locks.insert(new_lock);
        transaction_locks.remove(&old_lock);
        transaction_locks.insert(new_lock);
    }

    // Releases all locks associated with the specified transaction
    pub fn release_locks(&self, tid: TransactionId) {
        let mut page_to_locks = self.page_to_locks.write().unwrap();
        let mut transaction_locks = self.transaction_to_locks.write().unwrap();
        let held_locks = transaction_locks.entry(tid).or_insert(HashSet::new());
        for lock in held_locks.iter() {
            let page_locks = page_to_locks.get_mut(&lock.pid).unwrap();
            page_locks.remove(lock);
            if page_locks.is_empty() {
                page_to_locks.remove(&lock.pid);
            }
        }
        held_locks.clear();
        transaction_locks.remove(&tid);
    }

    // Checks if the specified transaction has a lock on the specified page
    pub fn holds_lock(&self, tid: TransactionId, pid: HeapPageId) -> Option<Permission> {
        let transaction_locks = self.transaction_to_locks.read().unwrap();
        match transaction_locks.get(&tid) {
            Some(locks) => {
                for lock in locks {
                    if lock.pid == pid {
                        return Some(if lock.exclusive {
                            Permission::Write
                        } else {
                            Permission::Read
                        });
                    }
                }
                None
            }
            None => None,
        }
    }

    // gets the set of pages locked by the specified transaction
    pub fn get_locked_pages(&self, tid: TransactionId) -> HashSet<HeapPageId> {
        let transaction_locks = self.transaction_to_locks.read().unwrap();
        match transaction_locks.get(&tid) {
            Some(locks) => locks.iter().map(|lock| lock.pid).collect(),
            None => HashSet::new(),
        }
    }
}
