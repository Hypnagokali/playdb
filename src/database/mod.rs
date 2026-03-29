pub mod access;

use thiserror::Error;

use crate::{data::page::{self, PageDataLayout}, database::access::TableAccess, store::Store, table::{Column, ColumnType, TableSchema, table::{Cell, Row, Table}}};

pub struct Database<S: Store> {
    pub name: String,
    store: S,
    layout: PageDataLayout,
}

#[derive(Debug, Error)]
pub enum CreateTableError {
    #[error("Invalid schema definition: {0}")]
    InvalidSchemaDefinition(String),
    #[error("Table with the same name already exists")]
    TableAlreadyExists,
}

pub struct CreateColumnCommand {
    name: String,
    col_type: ColumnType,
    sequence_name: Option<String>,
    unique: bool,
}

impl CreateColumnCommand {
    pub fn with_sequence(name: &str, col_type: ColumnType, sequence_name: &str, unique: bool) -> Self {
        Self::new_internal(name, col_type, Some(sequence_name.to_owned()), unique)
    }

    pub fn new(name: &str, col_type: ColumnType, unique: bool) -> Self {
        Self::new_internal(name, col_type, None, unique)
    }

    fn new_internal(name: &str, col_type: ColumnType, sequence_name: Option<String>, unique: bool) -> Self {
        Self {
            name: name.to_owned(),
            sequence_name,
            col_type,
            unique,
        }
    }
}

impl<S: Store> Database<S> {
    pub fn new(name: &str, store: S, page_size: usize) -> Self {
        if page_size < 2048 {
            panic!("Page size must be at least 2048 bytes");
        }
        Self {
            name: name.to_string(),
            store,
            layout: PageDataLayout::new(page_size).unwrap(),
        }
    }

    // ToDo: serialize, deserialize
    pub fn load(&self) {
        // load page data layout
        // load tables, columns, indexes
    }

    pub fn create_new(&self) {
        // self.init();
    }

    fn init_table_table(&self) {
        let table_schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(512)),
        ]);

        let table_table = Table::new(1, "tables".to_owned(), table_schema);
        self.store.create(&self.layout, &table_table).unwrap();
        let access = TableAccess::new(&table_table, &self.store, &self.layout);

        let table_row_tables = Row::new(vec![
            Cell::Int(1),
            Cell::Varchar("tables".to_owned()),
        ]);
        let table_row_cols = Row::new(vec![
            Cell::Int(2),
            Cell::Varchar("columns".to_owned()),
        ]);
        let table_row_seq = Row::new(vec![
            Cell::Int(3),
            Cell::Varchar("sequences".to_owned()),
        ]);
        let table_row_indexes = Row::new(vec![
            Cell::Int(4),
            Cell::Varchar("indexes".to_owned()),
        ]);

        access.insert(&table_row_tables).unwrap();
        access.insert(&table_row_cols).unwrap();
        access.insert(&table_row_seq).unwrap();
        access.insert(&table_row_indexes).unwrap();
    }

    fn init_column_table(&self) {
        let col_schema = TableSchema::new(vec![
            Column::new(3, "id", ColumnType::Int),
            Column::new(4, "t_id", ColumnType::Int),
            Column::new(5, "name", ColumnType::Varchar(512)),
            Column::new(6, "type", ColumnType::Int),
            Column::new(7, "length", ColumnType::Int),
        ]);

        let col_table = Table::new(1, "columns".to_owned(), col_schema);

        // insert rows for columns table

        self.store.create(&self.layout, &col_table).unwrap();
    }


    fn init(&self) {
        // ToDo: use proper error handling instead of unwrap
        // ignore unique index for base table ids for now
        self.init_table_table();
        self.init_column_table();
    }

    // pub fn create_table(&self, name: &str, schema_command: Vec<CreateColumnCommand>)
    //  -> Result<Table, CreateTableError> {
    //     // check if unique index is only created on int
    //     // reate columns
    //     Ok(Table::new(id, name.to_owned(), schema))
    // }
}