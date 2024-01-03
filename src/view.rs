use std::sync::Arc;

use crate::database;
use crate::database::Database;
use crate::heap_file::HeapFile;
use crate::transaction;
use crate::tuple::Tuple;
use crate::tuple::TupleDesc; // Import the `database` module

// have an iterator over the tuples in the table
// operators: insert, delete? ,
// if i want to support multiple insertions, that a user can call given a db instance
//  a: Table = … create(name, schema)
// then can just call a.insert(tuple)
// or a.insert(vec![tuple1, tuple2, tuple3])

// insertOne((1, “hello”, ...))
// use traits

// scan(optional int)
// scan(5)
// a.scan(10)
// project(select)

// { id: int,  name: String }
// a.scan().project( { “name” })
// a.project( { “name” })

// scan should produce an iterator, project should take an iterator and apply a map to it where i am
// { id: int,  name: String } => { name: String }

// project(input vec/stream/iterator, output vec/stream/iterator, conditions)
// Conditions = projection fields

// filter
// filter(clause)
// clause -> field, predicate
// Numbers -> >, <, <=, … =
// dont do length for strings - maybe do contains instead , which takes in a field also

// a.project( “name” }).filter(name, equals(“Adam”))

// for joins - take both tables into memory and then do the join and combine it nxn

// make a Table struct, inside it it stores HeapFile and then call the functions on it
pub struct View {
    name: String,
    td: TupleDesc,
    // data: Vec<Tuple>,
    table: Arc<HeapFile>,
    // db: Arc<Database>,
}

impl View {
    // pub fn new(name: String, td: TupleDesc, table: Arc<HeapFile>) -> Self {
    //     let mut data = vec![];
    //     let mut tuple_count = 0;
    //     let mut page_count = 0;
    //     let tid = transaction::TransactionId::new();
    //     for page in table.iter(tid) {
    //         let page = page.read().unwrap();
    //         page_count += 1;
    //         for tuple in page.iter() {
    //             // print!("tuple: {:?}\n", tuple);
    //             data.push(tuple.clone());
    //             tuple_count += 1;
    //         }
    //     }
    //     View {
    //         name,
    //         td,
    //         data,
    //     }
    // }
    pub fn new(name: String, td: TupleDesc, table: Arc<HeapFile>) -> Self {
        View { name, td, table }
    }

    pub fn get_tuple_desc(&self) -> &TupleDesc {
        &self.td
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_table(&self) -> &Arc<HeapFile> {
        &self.table
    }

    pub fn print(&self) {
        let db = database::get_global_db();
        let mut tuple_count = 0;
        let mut page_count = 0;
        let tid = transaction::TransactionId::new();
        for page in self.table.iter(tid) {
            let page = page.read().unwrap();
            page_count += 1;
            for tuple in page.iter() {
                print!("tuple: {:?}\n", tuple);
                tuple_count += 1;
            }
        }
        let bp = db.get_buffer_pool();
        bp.commit_transaction(tid);

        print!("page count: {}\n", page_count);
        print!("tuple count: {}\n", tuple_count);
    }

    // pub fn filter(&self, predicate: &dyn Fn(&Tuple) -> bool) -> Self {
    //     // make a new table that matches the predicate

    // }
}
