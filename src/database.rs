use crate::buffer_pool::BufferPool;
use crate::catalog::Catalog;
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    // Global database instance
    static ref GLOBAL_DB: Arc<Database> = Arc::new(Database::new());
}

// Retrieves a reference to the global database instance
pub fn get_global_db() -> Arc<Database> {
    Arc::clone(&GLOBAL_DB)
}

pub struct Database {
    buffer_pool: BufferPool,
    catalog: Catalog,
}

impl Database {
    pub fn new() -> Self {
        Database {
            buffer_pool: BufferPool::new(),
            catalog: Catalog::new(),
        }
    }

    pub fn get_buffer_pool(&self) -> &BufferPool {
        &self.buffer_pool
    }

    pub fn get_catalog(&self) -> &Catalog {
        &self.catalog
    }
}
