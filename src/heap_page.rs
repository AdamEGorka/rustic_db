use crate::buffer_pool::PAGE_SIZE;
use crate::transaction::TransactionId;
use crate::tuple::{Tuple, TupleDesc};

#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub enum Permission {
    Read,
    Write,
}

/// Representation of page id which just includes table id and page number
#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
pub struct HeapPageId {
    table_id: usize,
    page_number: usize,
}

impl HeapPageId {
    pub fn new(table_id: usize, page_number: usize) -> Self {
        HeapPageId {
            table_id,
            page_number,
        }
    }

    pub fn get_table_id(&self) -> usize {
        self.table_id
    }

    pub fn get_page_number(&self) -> usize {
        self.page_number
    }

    pub fn serialize(&self) -> Vec<usize> {
        vec![self.table_id, self.page_number]
    }
}

/**
 * Representation for a set of bytes of data read from disk.
 * Format is header bytes + tuple bytes. Header bytes indicate
 * whether or not a tuple is present in that slot on the page.
 * The number of bytes for header is equal to ceiling(# tuple slots / 8)
 */
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HeapPage {
    pid: HeapPageId,
    td: TupleDesc,
    header_size: usize,
    header: Vec<u8>,
    tuples: Vec<Tuple>,
    num_slots: usize,
    old_data: Vec<u8>,
    dirtied_by: Option<TransactionId>,
}

impl HeapPage {
    pub fn new(pid: HeapPageId, data: Vec<u8>, td: TupleDesc) -> Self {
        let num_slots = (PAGE_SIZE * 8) / (td.get_size() * 8 + 1);
        let old_data = vec![0; PAGE_SIZE];

        let header_size = (num_slots as f64 / 8.0).ceil() as usize;
        let header = data[..header_size].to_vec();

        let mut tuples = vec![];

        for i in 0..num_slots {
            if Self::get_slot(&header, i) {
                let start = header_size + i * td.get_size();
                let end = start + td.get_size();
                let tuple_data = data[start..end].to_vec();
                tuples.push(Tuple::deserialize(&tuple_data, &td));
            } else {
                tuples.push(Tuple::new(vec![], &td));
            }
        }

        HeapPage {
            pid,
            td,
            header_size,
            header,
            tuples,
            num_slots,
            old_data,
            dirtied_by: None,
        }
    }

    pub fn get_id(&self) -> HeapPageId {
        self.pid
    }

    pub fn get_before_image(&self) -> HeapPage {
        HeapPage::new(self.pid, self.old_data.clone(), self.td.clone())
    }

    pub fn set_before_image(&mut self) {
        self.old_data = self.get_page_data();
    }

    pub fn get_page_data(&self) -> Vec<u8> {
        let mut data = self.header.clone();
        for i in 0..self.num_slots {
            if Self::get_slot(&self.header, i) {
                data.extend(self.tuples[i].serialize());
            } else {
                data.extend(vec![0; self.td.get_size()]);
            }
        }
        // pad the rest of the page with 0s
        data.extend(vec![0; PAGE_SIZE - data.len()]);
        data
    }

    fn get_slot(header: &[u8], i: usize) -> bool {
        let idx = i / 8;
        let bit = i % 8;
        if idx >= header.len() {
            return false;
        }
        let byte = header[idx];
        let mask = 1 << bit;
        byte & mask != 0
    }

    fn set_slot(header: &mut [u8], i: usize, value: bool) {
        let idx = i / 8;
        let bit = i % 8;
        let byte = header[idx];
        let mask = 1 << bit;
        if value {
            header[idx] = byte | mask;
        } else {
            header[idx] = byte & !mask;
        }
    }

    fn create_empty_page_data(&self) -> Vec<u8> {
        vec![0; PAGE_SIZE]
    }

    pub fn add_tuple(&mut self, t: Tuple) -> Result<(), String> {
        let mut i = 0;
        while i < self.num_slots {
            if !Self::get_slot(&self.header, i) {
                self.tuples[i] = t;
                Self::set_slot(&mut self.header, i, true);
                return Ok(());
            }
            i += 1;
        }
        Err("No empty slots".to_string())
    }

    pub fn delete_tuple(&mut self, t: Tuple) -> Result<(), String> {
        let rid = t.get_record_id();
        let tuple_no = rid.get_tuple_no();
        if rid.get_page_id() != self.pid {
            return Err("Tuple not on this page".to_string());
        }
        if !Self::get_slot(&self.header, tuple_no) {
            return Err("Tuple not on this page".to_string());
        }

        self.tuples[tuple_no] = Tuple::new(vec![], &self.td);
        Self::set_slot(&mut self.header, tuple_no, false);
        Ok(())
    }

    pub fn get_num_empty_slots(&self) -> usize {
        let mut count = 0;
        for i in 0..self.num_slots {
            if !Self::get_slot(&self.header, i) {
                count += 1;
            }
        }
        count
    }

    pub fn mark_dirty(&mut self, dirty: bool, tid: TransactionId) {
        if dirty {
            self.dirtied_by = Some(tid);
        } else {
            self.dirtied_by = None;
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirtied_by.is_some()
    }

    pub fn iter(&self) -> HeapPageIterator {
        HeapPageIterator {
            page: self,
            index: 0,
        }
    }

    // by adam but idk if this is fine
    pub fn get_tuple(&self, i: usize) -> &Tuple {
        &self.tuples[i]
    }

    pub fn num_tuples(&self) -> usize {
        self.num_slots
    }
}

pub struct HeapPageIterator<'a> {
    page: &'a HeapPage,
    index: usize,
}

impl<'a> Iterator for HeapPageIterator<'a> {
    type Item = &'a Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.page.num_slots {
            return None;
        }
        while self.index < self.page.num_slots {
            if HeapPage::get_slot(&self.page.header, self.index) {
                let tuple = &self.page.tuples[self.index];
                self.index += 1;
                return Some(tuple);
            }
            self.index += 1;
        }
        None
    }
}
