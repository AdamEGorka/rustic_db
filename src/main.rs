mod buffer_pool;
mod catalog;
mod database;
mod fields;
mod heap_file;
mod heap_page;
mod lock_manager;
mod table;
mod transaction;
mod tuple;
mod types;
mod view;

use std::thread;
fn main() {
    let db = database::get_global_db();

    // 1. Load the schemas and tables from the schemas.txt file
    let mut schema_file_path = std::env::current_dir().unwrap();
    schema_file_path.push("schemas.txt");
    db.get_catalog()
        .load_schema(schema_file_path.to_str().unwrap());

    // 2. Retrieve the list of catalogs
    let catalog = db.get_catalog();

    // 3. Retrieve the table id for the employee table
    let table = catalog.get_table_from_name("employees").unwrap();
    let table_id = table.get_id();

    // 4. Retrieve the tuple descriptor for the employee table
    let td = table.get_tuple_desc().clone();

    // 5. Insert 3 tuples into the employee table in 3 separate threads
    // threads panic if aborted by WAIT-DIE protocol
    println!("table id: {}", table_id);
    println!("table name: {:?}", td.get_field_name(0));
    let handles: Vec<_> = (0..3)
        .map(|_| {
            let db = database::get_global_db();
            let table = db.get_catalog().get_table_from_id(table_id).unwrap();
            let td = table.get_tuple_desc().clone();
            thread::spawn(move || loop {
                let res = std::panic::catch_unwind(|| {
                    let tid = transaction::TransactionId::new();
                    let bp = db.get_buffer_pool();
                    let name = format!("Alice_{}", tid.get_tid());
                    for i in 0..3 {
                        bp.insert_tuple(
                            tid,
                            table_id,
                            tuple::Tuple::new(
                                vec![
                                    fields::FieldVal::IntField(fields::IntField::new(i)),
                                    fields::FieldVal::StringField(fields::StringField::new(
                                        name.clone(),
                                        7,
                                    )),
                                ],
                                &td,
                            ),
                        );
                    }
                    bp.commit_transaction(tid);
                });
                if res.is_err() {
                    println!("thread {:?} aborted", thread::current().id());
                    thread::sleep(std::time::Duration::from_millis(500));
                } else {
                    println!("thread {:?} committed", thread::current().id());
                    break;
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // 6. Print out the tuples in the employee table
    let mut tuple_count = 0;
    let mut page_count = 0;
    let tid = transaction::TransactionId::new();
    let table = catalog.get_table_from_id(table_id).unwrap();
    for page in table.iter(tid) {
        let page = page.read().unwrap();
        page_count += 1;
        for tuple in page.iter() {
            println!("tuple: {}", tuple);
            tuple_count += 1;
        }
    }
    let bp = db.get_buffer_pool();
    bp.commit_transaction(tid);

    print!("page count: {}\n", page_count);
    print!("tuple count: {}\n", tuple_count);

    // my stuff trying to create user friendly tables
    print!("my stuff\n\n\n");

    let my_table = table::Table::new("employess".to_string(), "schema.txt".to_string());

    my_table.insert_tuple(
        tuple::Tuple::new(
            vec![
                fields::FieldVal::IntField(fields::IntField::new(1)),
                fields::FieldVal::StringField(fields::StringField::new("Alice".to_string(), 7)),
            ],
            &td,
        ),
        transaction::TransactionId::new(),
    );

    my_table.print();
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_table() {
        let db = database::get_global_db();

        // 1. Load the schemas and tables from the schemas.txt file
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push("schemas.txt");
        db.get_catalog()
            .load_schema(schema_file_path.to_str().unwrap());

        let my_table = table::Table::new("products".to_string(), "schema.txt".to_string());

        // We can inserting tuples one at a time
        let tuple_to_insert = tuple::Tuple::new(
            vec![
                fields::FieldVal::IntField(fields::IntField::new(0)),
                fields::FieldVal::StringField(fields::StringField::new("Alice_0".to_string(), 7)),
            ],
            &my_table.get_tuple_desc().clone(),
        );
        let tid = transaction::TransactionId::new();
        my_table.insert_tuple(tuple_to_insert.clone(), tid);

        // Insert multiple tuples into the table
        let tuple_collection = (1..20)
            .map(|i| {
                let name = format!("Alice_{}", i);
                let length = name.len();
                tuple::Tuple::new(
                    vec![
                        fields::FieldVal::IntField(fields::IntField::new(i)),
                        fields::FieldVal::StringField(fields::StringField::new(
                            name,
                            length as u32,
                        )),
                    ],
                    &my_table.get_tuple_desc().clone(),
                )
            })
            .collect();
        my_table.insert_many_tuples(tuple_collection, tid);

        // We can then scan the table to see all of our results
        println!("-------------");
        println!("----SCAN-----");
        println!("-------------");
        let scan = my_table.scan(20, tid);
        for tuple in scan.into_iter() {
            println!("{}", tuple);
        }

        let mut scan2 = my_table.scan(5, tid);

        // simple filtering, using a predicate
        println!("---------------");
        println!("----FILTERS----");
        println!("---------------");
        let pred = table::Predicate::GreaterThan(1);
        scan2.table_filter("id", pred);
        for tuple in scan2.into_iter() {
            println!("{}", tuple);
        }
        // performing a filter on the scan, on the field "id" with the predicate "GreaterThan(1)"

        println!("-------------");
        println!("----JOINS----");
        println!("-------------");
        // load up second table
        let my_table2 = table::Table::new("test2".to_string(), "schema.txt".to_string());
        let tuple_collection2 = (5..10)
            .map(|i| {
                let name = format!("Alice_{}", i);
                let length = name.len();
                tuple::Tuple::new(
                    vec![
                        fields::FieldVal::IntField(fields::IntField::new(i)),
                        fields::FieldVal::StringField(fields::StringField::new(
                            name,
                            length as u32,
                        )),
                    ],
                    &my_table.get_tuple_desc().clone(),
                )
            })
            .collect();
        my_table2.insert_many_tuples(tuple_collection2, tid);

        // grab two scans, combine both scans into a join
        let scan3 = my_table2.scan(5, tid);
        let scan4 = my_table.scan(20, tid);
        let join = scan3.join(&scan4, "title", "id");

        for tuple in join {
            println!("{}", tuple);
        }

        println!("--------------");
        println!("--PROJECTION--");
        println!("--------------");
        let scan5 = my_table.scan(2, tid);
        let proj = scan5.project(vec!["id".to_string()]);
        for tuple in proj {
            println!("{}", tuple);
        }
    }

    #[test]
    fn test_asynchronous_scan() {
        let db = database::get_global_db();
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push("schemas.txt");
        db.get_catalog()
            .load_schema(schema_file_path.to_str().unwrap());

        let table = Arc::new(table::Table::new(
            "products".to_string(),
            "schema.txt".to_string(),
        ));
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let table = table.clone();
                thread::spawn(move || {
                    let tid = transaction::TransactionId::new();
                    let scan = table.scan(2, tid);
                    for tuple in scan.into_iter() {
                        println!("{} - Thread {}", tuple, i);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_younger_transaction_aborts() {
        let db = database::get_global_db();
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push("schemas.txt");
        db.get_catalog()
            .load_schema(schema_file_path.to_str().unwrap());

        let table = Arc::new(table::Table::new(
            "testwrites".to_string(),
            "schema.txt".to_string(),
        ));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let table = table.clone();
                let db = database::get_global_db();
                thread::spawn(move || {
                    // second transaction waits for 500 ms for first transaction to insert
                    // their first tuple
                    let tid = transaction::TransactionId::new();
                    if tid.get_tid() == 1 {
                        thread::sleep(std::time::Duration::from_millis(500));
                    }
                    // inserted i should be 0 from first transaction and 1 for second transaction
                    let i = tid.get_tid() as i32;
                    let mut tuple = tuple::Tuple::new(
                        vec![
                            fields::FieldVal::IntField(fields::IntField::new(i)),
                            fields::FieldVal::StringField(fields::StringField::new(
                                format!("Alice_{}", i),
                                7,
                            )),
                        ],
                        &table.get_tuple_desc().clone(),
                    );
                    table.insert_tuple(tuple.clone(), tid);
                    // first transaction sleeps and allows second thread to attempt insertion
                    // second transaction should abort since first transaction has write lock
                    thread::sleep(std::time::Duration::from_millis(2000 * (-i + 1) as u64));
                    tuple.set_field(
                        1,
                        fields::FieldVal::StringField(fields::StringField::new(
                            format!("Bob_{}", i),
                            7,
                        )),
                    );
                    table.insert_tuple(tuple, tid);
                    let bp = db.get_buffer_pool();
                    bp.commit_transaction(tid);
                })
            })
            .collect();
        for handle in handles {
            match handle.join() {
                Ok(_) => println!("Transaction committed"),
                Err(_) => println!("Transaction aborted"),
            }
        }

        // table should only have the tuples inserted by the first transaction
        for tuple in table.scan(10, transaction::TransactionId::new()) {
            println!("{}", tuple);
        }
    }

    #[test]
    fn test_older_transaction_waits() {
        let db = database::get_global_db();
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push("schemas.txt");
        db.get_catalog()
            .load_schema(schema_file_path.to_str().unwrap());

        let table = Arc::new(table::Table::new(
            "testwrites".to_string(),
            "schema.txt".to_string(),
        ));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let table = table.clone();
                let db = database::get_global_db();
                thread::spawn(move || {
                    // first transaction waits for 500 ms for second transaction to start insert
                    let tid = transaction::TransactionId::new();
                    let i = tid.get_tid() as i32;
                    if i == 0 {
                        thread::sleep(std::time::Duration::from_millis(500));
                    }
                    // second transaction should insert first and have write lock
                    let mut tuple = tuple::Tuple::new(
                        vec![
                            fields::FieldVal::IntField(fields::IntField::new(tid.get_tid() as i32)),
                            fields::FieldVal::StringField(fields::StringField::new(
                                format!("Alice_{}", i),
                                7,
                            )),
                        ],
                        &table.get_tuple_desc().clone(),
                    );
                    table.insert_tuple(tuple.clone(), tid);
                    // second transaction sleeps and first transaction will try to insert
                    // first transaction should wait since second transaction has write lock
                    if i == 1 {
                        thread::sleep(std::time::Duration::from_millis(1000));
                    }
                    tuple.set_field(
                        1,
                        fields::FieldVal::StringField(fields::StringField::new(
                            format!("Bob_{}", tid.get_tid()),
                            5,
                        )),
                    );
                    table.insert_tuple(tuple, tid);
                    let bp = db.get_buffer_pool();
                    bp.commit_transaction(tid);
                })
            })
            .collect();
        for handle in handles {
            match handle.join() {
                Ok(_) => println!("Transaction committed"),
                Err(_) => println!("Transaction aborted"),
            }
        }

        // we should see all 4 tuples inserted with transaction 1's tuples first
        for tuple in table.scan(10, transaction::TransactionId::new()) {
            println!("{}", tuple);
        }
    }

    #[test]
    fn test_inserting_different_tables() {
        let db = database::get_global_db();
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push("schemas.txt");
        db.get_catalog()
            .load_schema(schema_file_path.to_str().unwrap());

        let table1 = Arc::new(table::Table::new(
            "testwrites".to_string(),
            "schema.txt".to_string(),
        ));
        let table2 = Arc::new(table::Table::new(
            "testwrites2".to_string(),
            "schema.txt".to_string(),
        ));
        let handles: Vec<_> = (0..2)
            .map(|t| {
                let table = if t == 0 {
                    table1.clone()
                } else {
                    table2.clone()
                };
                let db = database::get_global_db();
                thread::spawn(move || {
                    let tid = transaction::TransactionId::new();
                    let i = tid.get_tid() as i32;
                    let tuple_collection = (0..10)
                        .map(|j| {
                            let name = format!("Alice_{} from transaction {}", j, i);
                            let length = name.len();
                            tuple::Tuple::new(
                                vec![
                                    fields::FieldVal::IntField(fields::IntField::new(
                                        tid.get_tid() as i32,
                                    )),
                                    fields::FieldVal::StringField(fields::StringField::new(
                                        name,
                                        length as u32,
                                    )),
                                ],
                                &table.get_tuple_desc().clone(),
                            )
                        })
                        .collect();

                    table.insert_many_tuples(tuple_collection, tid);
                    let bp = db.get_buffer_pool();
                    bp.commit_transaction(tid);
                })
            })
            .collect();
        for handle in handles {
            match handle.join() {
                Ok(_) => println!("Transaction committed"),
                Err(_) => println!("Transaction aborted"),
            }
        }

        // we should see all the tuples inserted
        for tuple in table1.scan(20, transaction::TransactionId::new()) {
            println!("{}", tuple);
        }
        for tuple in table2.scan(20, transaction::TransactionId::new()) {
            println!("{}", tuple);
        }
    }

    #[test]
    fn test_recovery_from_abort() {
        let db = database::get_global_db();
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push("schemas.txt");
        db.get_catalog()
            .load_schema(schema_file_path.to_str().unwrap());

        let table1 = Arc::new(table::Table::new(
            "testwrites".to_string(),
            "schema.txt".to_string(),
        ));
        let table2 = Arc::new(table::Table::new(
            "testwrites2".to_string(),
            "schema.txt".to_string(),
        ));
        let tables = vec![table1.clone(), table2.clone()];
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let db = database::get_global_db();
                let tables = tables.clone();
                thread::spawn(move || {
                    let tid = transaction::TransactionId::new();
                    let i = tid.get_tid() as usize;
                    let tuple = tuple::Tuple::new(
                        vec![
                            fields::FieldVal::IntField(fields::IntField::new(tid.get_tid() as i32)),
                            fields::FieldVal::StringField(fields::StringField::new(
                                format!("Alice from transaction {}", i),
                                24,
                            )),
                        ],
                        &tables[0].get_tuple_desc().clone(),
                    );
                    tables[i].insert_tuple(tuple, tid);
                    // second transaction waits to make sure first transaction has write
                    // lock on the first table
                    if i == 1 {
                        thread::sleep(std::time::Duration::from_millis(1000));
                    }
                    let tuple = tuple::Tuple::new(
                        vec![
                            fields::FieldVal::IntField(fields::IntField::new(tid.get_tid() as i32)),
                            fields::FieldVal::StringField(fields::StringField::new(
                                format!("Bob from transaction {}", i),
                                22,
                            )),
                        ],
                        &tables[1].get_tuple_desc().clone(),
                    );
                    // second transaction should abort since first transaction has write lock
                    tables[(i + 1) % 2].insert_tuple(tuple, tid);
                    let bp = db.get_buffer_pool();
                    bp.commit_transaction(tid);
                })
            })
            .collect();
        for handle in handles {
            match handle.join() {
                Ok(_) => println!("Transaction committed"),
                Err(_) => println!("Transaction aborted"),
            }
        }

        // we should only see the tuples inserted by the first transaction
        println!("table 1");
        for tuple in table1.scan(20, transaction::TransactionId::new()) {
            println!("{}", tuple);
        }
        println!("table 2");
        for tuple in table2.scan(20, transaction::TransactionId::new()) {
            println!("{}", tuple);
        }
    }
}
