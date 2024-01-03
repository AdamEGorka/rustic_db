# rustic_db
A simple database management system inspired by [MIT Opencourseware](https://ocw.mit.edu/courses/6-830-database-systems-fall-2010/) implemented in Rust.

## Project Description
- The aim of our project is to create a basic relational database system. The system will be able to store and retrieve data. The user will also be able to perform basic table operations such as table creation and data operations such as insertion, filtering, projection, and joining.
- The database also allows for concurrent reads and writes through a transaction manager that guarantees atomicity
- We have also implemented the WAIT-DIE protocol for deadlock avoidance (Younger transactions are not allowed to wait on older transactions)
- The underlying structure of the data is stored in heapfiles, each representing one of our tables. Each heapfile consists of heappages for the table. The heappage consists of tuple data and a header bit mask that indicates the valid tuple slots on the page.

## Project Structure:
![SimpleDB](https://github.com/Jeffroyang/rustic_db/assets/82118995/2213c564-6b7c-4b62-99fb-0c298aebdf16)
- The buffer pool module is responsible for managing accessing page on disk and caching pages in memory for quicker access. It is also in charge of managing transactions in our database.
- The lock manager module is responsible for ensuring atomic transactions in our database. It also implements the WAIT-DIE protocol for deadlock avoidnace
- The heapfile module represents the underlying data for a data, and it communicates with the buffer pool in order to retrieve relevant pages. This provides a simple abstraction that allows us to easily query for pages.
- The database and catalog modules provide global variables that we can access. The database consists of both buffer pool and catalog fields. Having access to the catalog is useful for communicating what tables are available. Having access to the buffer pool allows us to commit transactions and allow heap files to easily access the pages needed.


## Operations:
 - The Table struct represents a table with properties like name, heap_file, table_id, and tuple_desc. Operations include inserting, scanning, and printing tuples.
 - The TableIterator struct serves as an iterator for table views, supporting projection, filtering, and joining.
 - Predicates like Equals, EqualsInt, GreaterThan, and LessThan facilitate filtering, while the Filterable trait adds filtering functionality to tuples.
 - The code offers a means for a user to communicate with the actual database, demonstrating table creation, tuple insertion, scanning, and a join operation.

To use these functions, create a new table instance with Table::new(name, schema), specifying the table name and the path to its schema. Insert single or multiple tuples using insert_tuple and insert_many_tuples. Retrieve the table's tuple descriptor with get_tuple_desc and its ID with get_id. Printing the table's content is facilitated by the print function. Scanning the table can be done using the scan method, and further operations like projection, filtering, and joining are available through the TableIterator struct. Examples demonstrate the usage of these functionalities, such as inserting tuples, scanning, applying filters, and performing joins. The provided tests illustrate scenarios like asynchronous scans, transaction handling, and recovery from aborted transactions. Adapt and integrate this module into your project as needed.

