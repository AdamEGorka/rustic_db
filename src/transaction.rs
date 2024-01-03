use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId {
    tid: u64,
}

impl TransactionId {
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let tid = COUNTER.fetch_add(1, Ordering::SeqCst);
        TransactionId { tid }
    }

    pub fn get_tid(&self) -> u64 {
        self.tid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_transaction_id_increments() {
        let tid1 = TransactionId::new();
        let tid2 = TransactionId::new();
        assert_ne!(tid1, tid2);
    }

    #[test]
    fn test_transaction_id_get_tid() {
        let tid1 = TransactionId::new();
        let tid2 = TransactionId::new();
        assert_eq!(tid1.get_tid(), 0);
        assert_eq!(tid2.get_tid(), 1);
    }
}
