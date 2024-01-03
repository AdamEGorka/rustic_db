use crate::database; // Import the `database` module or crate
use crate::fields::FieldVal;
use crate::heap_file::HeapFile;
use crate::transaction::TransactionId; // Import the `transaction` module or crate
use crate::tuple; // Import the `tuple` module or crate
use crate::tuple::Tuple;
use crate::tuple::TupleDesc;
use std::sync::Arc;

pub struct Table {
    name: String,
    heap_file: Arc<HeapFile>,
    table_id: usize,
    tuple_desc: TupleDesc,
}

impl Table {
    pub fn new(name: String, schema: String) -> Self {
        let db = database::get_global_db();
        let catalog = db.get_catalog();

        // use the path given in schema to load the schema - should maybe do it differently
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push(schema);

        let heap_file = catalog.get_table_from_name(&name).unwrap();
        let table_id = heap_file.get_id();

        Table {
            name,
            tuple_desc: heap_file.get_tuple_desc().clone(),
            heap_file,
            table_id,
        }
    }

    pub fn insert_tuple(&self, tuple: Tuple, tid: TransactionId) {
        self.heap_file.add_tuple(tid, tuple);
    }

    pub fn insert_many_tuples(&self, tuples: Vec<Tuple>, tid: TransactionId) {
        for tuple in tuples {
            self.heap_file.add_tuple(tid, tuple);
        }
    }

    pub fn get_tuple_desc(&self) -> &TupleDesc {
        &self.tuple_desc
    }

    pub fn get_id(&self) -> usize {
        self.table_id
    }

    pub fn print(&self) {
        let db = database::get_global_db();
        let tid = TransactionId::new();
        for page in self.heap_file.iter(tid) {
            let page = page.read().unwrap();
            for (i, tuple) in page.iter().enumerate() {
                println!("{}: {}", i, tuple);
            }
        }
        let bp = db.get_buffer_pool();
        bp.commit_transaction(tid);
    }

    pub fn scan(&self, count: usize, tid: TransactionId) -> TableIterator {
        TableIterator::new(self, tid, count)
    }
}

// iterator iterates on a view generated from the heapfile -> quick fix to get the view working
// and avoid iterating on the heapfile directly, which I cant get working properly
pub struct TableIterator<'a> {
    table: &'a Table,
    current_page_index: usize,
    tid: TransactionId,
    data: Vec<tuple::Tuple>, // like a view
    filters: Vec<(String, Predicate)>,
}

impl<'a> TableIterator<'a> {
    // make a new table iterator and fill its vector with count tuples -
    fn new(table: &'a Table, tid: TransactionId, count: usize) -> Self {
        let mut data = Vec::new();
        let mut count = count;
        for page in table.heap_file.iter(tid) {
            let page = page.read().unwrap();
            for tuple in page.iter() {
                if count == 0 {
                    break;
                }
                count -= 1;
                data.push(tuple.clone());
            }
        }
        TableIterator {
            table,
            current_page_index: 0,
            tid,
            data,
            filters: Vec::new(),
        }
    }

    pub fn project(&self, fields: Vec<String>) -> TableIterator {
        let mut data = Vec::new();

        // take the Tuple and make a new TupleDesc for it as well as a new Fields for it
        for tuple in self.data.iter() {
            let mut new_field_types = Vec::new();
            let mut new_field_vals = Vec::new();

            // go through each of the fields for this tuple
            for i in 0..tuple.get_tuple_desc().get_num_fields() {
                let field_name = tuple.get_tuple_desc().get_field_name(i).unwrap().clone();

                // Check if the field is in the list of fields to keep and has the desired type
                if fields.contains(&field_name) {
                    // we want to keep this field - so adding it to the new field types

                    let field_type = tuple.get_tuple_desc().get_field_type(i).unwrap().clone();
                    new_field_types.push(field_type);

                    let field = tuple.get_field(i).unwrap().clone();
                    new_field_vals.push(field);
                }
            }

            // Create a new tuple descriptor with only the selected fields
            let new_tuple_desc = TupleDesc::new(new_field_types, fields.clone());

            // Create a new tuple with the selected fields
            let new_tuple = Tuple::new(new_field_vals, &new_tuple_desc);

            data.push(new_tuple);
        }
        // make a new iterator with the new data
        TableIterator {
            table: self.table,
            current_page_index: 0,
            tid: self.tid,
            data,
            filters: Vec::new(),
        }
    }

    pub fn table_filter(&mut self, field_name: &str, predicate: Predicate) {
        self.filters.push((field_name.to_string(), predicate));
    }

    pub fn join(
        &self,
        other: &TableIterator,
        field_name_left: &str,
        field_name_right: &str,
    ) -> TableIterator {
        // making a new 'view'/ TableIterator using nxn from both tables
        // field_name is the field/col that we are joining on
        // similar to JOIN t1 ON t1.id = t2.id where id is field_name
        let mut data = Vec::new();

        for tuple in self.data.iter() {
            println!("{}", tuple);
            let target_col_left = tuple.get_tuple_desc().name_to_id(field_name_left).unwrap();
            for other_tuple in other.data.iter() {
                let target_col_right = other_tuple
                    .get_tuple_desc()
                    .name_to_id(field_name_right)
                    .unwrap();
                // check if the tuples match
                // if they do, add them to the new view
                if tuple.get_field(target_col_left).unwrap()
                    == other_tuple.get_field(target_col_right).unwrap()
                {
                    // add the tuple to the new view

                    // need to combine the two tuples

                    // making a new TupleDesc
                    let ctd: TupleDesc =
                        TupleDesc::combine(tuple.get_tuple_desc(), other_tuple.get_tuple_desc());
                    let combined_fields = tuple
                        .get_fields()
                        .iter()
                        .chain(other_tuple.get_fields().iter())
                        .cloned()
                        .collect::<Vec<_>>();
                    let new_tuple = Tuple::new(combined_fields, &ctd);
                    data.push(new_tuple);
                }
            }
        }
        TableIterator {
            table: self.table,
            current_page_index: 0,
            tid: self.tid,
            data,
            filters: Vec::new(),
        }
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = tuple::Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_index < self.data.len() {
            let tuple = self.data[self.current_page_index].clone();
            self.current_page_index += 1;

            // also apply any filters here - dumb but i think it would work
            for filter in self.filters.iter() {
                if !tuple.filter(&filter.0, &filter.1) {
                    return self.next();
                }
            }

            Some(tuple)
        } else {
            None
        }
    }
}

pub enum Predicate {
    Equals(String),
    EqualsInt(i32),
    GreaterThan(i32),
    LessThan(i32),
}

// trait to do filtering for filter()
pub trait Filterable {
    fn filter(&self, field_name: &str, predicate: &Predicate) -> bool;
}

// quick implementation of filter
impl Filterable for Tuple {
    fn filter(&self, field_name: &str, predicate: &Predicate) -> bool {
        for i in 0..self.get_tuple_desc().get_num_fields() {
            // iterating through all the fields in the tuple
            let field = self.get_field(i).unwrap();
            let t_field_name = self.get_tuple_desc().get_field_name(i).unwrap();
            if field_name == t_field_name {
                // found the field i want to filter
                match predicate {
                    Predicate::Equals(value) => {
                        if let FieldVal::StringField(string_field) = &field {
                            return string_field.get_value().as_str() == value;
                        } else {
                            return false;
                        }
                    }
                    Predicate::GreaterThan(value) => {
                        print!(
                            "field: {:?}\n",
                            field.clone().into_int().unwrap().get_value()
                        );
                        print!("value: {:?}\n", value);
                        if let FieldVal::IntField(int_field) = &field {
                            return int_field.get_value() > *value;
                        } else {
                            return false;
                        }
                    }
                    Predicate::LessThan(value) => {
                        if let FieldVal::IntField(int_field) = &field {
                            return int_field.get_value() < *value;
                        } else {
                            return false;
                        }
                    }
                    Predicate::EqualsInt(value) => {
                        if let FieldVal::IntField(int_field) = &field {
                            return int_field.get_value() == *value;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        false
    }
}
