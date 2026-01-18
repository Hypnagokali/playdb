use thiserror::Error;

use crate::{data::page::{PageDataLayout, PageError}, store::Store, table::table::{Row, RowValidationError, Table}};

pub struct TableAccess<'db, S: ?Sized> {
    table: &'db Table,
    store: &'db S,
    layout: &'db PageDataLayout,
}

#[derive(Error, Debug)]
pub enum TableAccessError {
    #[error("TableAccessError - insert error: {0}")]
    InsertRowError(String),
    #[error("TableAccessError - load error: {0}")]
    LoadRowsError(String),
}

impl From<PageError> for TableAccessError {
    fn from(err: PageError) -> Self {
        match err {
            PageError::InsertRowError => TableAccessError::InsertRowError("Failed to insert row into page.".to_string()),
            PageError::ReadPageError => TableAccessError::LoadRowsError("Failed to read page from file.".to_string()),
        }
    }
}

impl From<RowValidationError> for TableAccessError {
    fn from(err: RowValidationError) -> Self {
        TableAccessError::InsertRowError(format!("Row validation error: {}", err))
    }
}

impl<'db, S: Store> TableAccess<'db, S> {
    pub fn new(table: &'db Table, store: &'db S, layout: &'db PageDataLayout) -> Self {
        Self { table, store, layout }
    }

    /// Drop the table by deleting its underlying file
    pub fn drop(&self) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::remove_file(&self.table.file_path())?;
        Ok(())
    }

    /// Load all rows from all pages in the table
    pub fn load_all(&self) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
        let mut rows = Vec::new();

        // Read metadata to know how many pages exist
        let metadata = self.store.read_metadata(self.layout, self.table)?;
        let total_pages = metadata.number_of_pages();

        // Iterate over all pages and collect rows
        for page_id in 0..total_pages {
            let page = self.store.read_page(self.layout, page_id, self.table)?;
            for row in page.rows(&self.table.schema) {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    // Currently maximally naive insert implementation
    // Should be refactored, so that FSM is used to find pages with free space
    pub fn insert(&self, row: &Row) -> Result<(), TableAccessError> {
        row.validate(&self.table.schema)?;

        let page_iterator = self.store.page_iterator(self.layout, self.table)
            .map_err(|_| TableAccessError::InsertRowError("Cannot retrieve page iterator".to_string()))?;

        let mut inserted = false;
        for mut page in page_iterator {
            let row_data = row.serialize();
            if page.space_remaining() >= row_data.len() {
                page.insert_row(row_data)?;
                self.store.write_page(self.layout, &page, self.table)
                    .map_err(|_| TableAccessError::InsertRowError("Cannot write page".to_string()))?;

                inserted = true;
                break;
            }
        }

        if !inserted {
            // No page with enough space found
            let mut new_page = self.store.allocate_page(self.layout, self.table)
                .map_err(|_| TableAccessError::InsertRowError("Cannot allocate page".to_string()))?;

            let row_data = row.serialize();
            new_page.insert_row(row_data)?;

            self.store.write_page(self.layout, &new_page, self.table)
                .map_err(|_| TableAccessError::InsertRowError("Cannot write new allocated page".to_string()))?;
        }

        Ok(())
    }
}