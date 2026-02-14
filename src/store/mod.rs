pub mod file_store;

use thiserror::Error;

use crate::{data::page::{Page, PageDataLayout, PageFileMetadata, Record, RecordIterator}, table::{TableSchema, table::{Row, Table}}};

// Store is always owned by a Database instance
pub trait Store {
    // fn store_layout(&self, layout: &PageDataLayout, database: &Database);
    // fn load_layout(&self, database: &Database) -> PageDataLayout;
    fn read_metadata(&self, layout: &PageDataLayout, table: &Table) -> Result<PageFileMetadata, StoreError>;
    fn read_page<'db>(&self, layout: &'db PageDataLayout, page_id: i32, table: &Table) -> Result<Page<'db>, StoreError>;
    fn write_page(&self, layout: &PageDataLayout, page: &Page, table: &Table) -> Result<(), StoreError>;
    fn allocate_page<'db>(&self, layout: &'db PageDataLayout, table: &Table) -> Result<Page<'db>, StoreError>;
    fn page_iterator<'database>(&'database self, layout: &'database PageDataLayout, table: &'database crate::table::table::Table) -> Result<PageIterator<'database, Self>, StoreError> 
    where
        Self: Sized
    {
        Ok(PageIterator::new(table, self, layout))
    }
}

pub struct PageIterator<'db, S: Store> {
    layout: &'db PageDataLayout,
    store: &'db S,
    table: &'db Table,
    current_page_id: i32,
    total_pages: i32,
}

impl<'db, S: Store> PageIterator<'db, S> {
    pub fn new(table: &'db Table, store: &'db S, layout: &'db PageDataLayout) -> Self {
        // ToDo: better error handling
        let metadata = store.read_metadata(layout, table).expect("Couldn't read metadata");
        let total_pages = metadata.number_of_pages();
        Self {
            table,
            layout,
            store,
            current_page_id: 1,
            total_pages,
        }
    }
}

impl<'db, S: Store> Iterator for PageIterator<'db, S> {
    type Item = Page<'db>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_id > self.total_pages {
            return None;
        }
        let page = self.store.read_page(self.layout, self.current_page_id, self.table).unwrap();

        self.current_page_id += 1;
        Some(page)
    }
}

pub struct PageRowIterator<'a> {
    record_iterator: RecordIterator<'a>,
    schema: &'a TableSchema,
}

impl<'a> PageRowIterator<'a> {
    pub fn new(page: &'a Page, schema: &'a TableSchema) -> Self {
        Self { 
            record_iterator: page.record_iterator(),
            schema 
        }
    }
}

impl<'db> Iterator for PageRowIterator<'db> {
    type Item = (Record<'db>, Row);

    fn next(&mut self) -> Option<Self::Item> {
        self.record_iterator.next()
            .map(|r| {
                let row = Row::deserialize(r.data(), self.schema);

                (r, row)
            })
    }
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("StoreError - I/O Error: {0}")]
    IoError(String),
    #[error("StoreError - Deserialization Error: {0}")]
    DeserializationError(String),
}

impl From<std::io::Error> for StoreError {
    fn from(err: std::io::Error) -> Self {
        StoreError::IoError(err.to_string())
    }
}
