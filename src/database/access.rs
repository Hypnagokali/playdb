use thiserror::Error;

use crate::{data::page::{PageDataLayout, PageError}, store::{PageIterator, PageRowIterator, Store}, table::table::{Cell, Row, RowValidationError, Table}};

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
    pub fn drop(&self) -> Result<(), TableAccessError> {
        unimplemented!()
        // std::fs::remove_file(&self.table.file_path())?;
        // Ok(())
    }

    /// Load all rows from all pages in the table
    pub fn load_all(&self) -> Result<Vec<Row>, TableAccessError> {
        let mut rows = Vec::new();

        // Read metadata to know how many pages exist
        for page in PageIterator::new(self.table, self.store, self.layout) {
            let row_iterator = PageRowIterator::new(&page, self.table.schema());

            for record_row in row_iterator {
                rows.push(record_row.1);
            }
        }

        Ok(rows)
    }

    pub fn find(&self, col_name: &str, cell: Cell) -> Result<Vec<Row>, TableAccessError> {
        // Full table scan:
        let mut result = Vec::new();
        let mut col_index = 0;
        let mut col_found = false;
        for (index, col) in self.table.schema().columns.iter().enumerate() {
            if col.name == col_name.trim() {
                col_index = index;
                col_found = true;
                break;
            }
        }

        if !col_found {
            return Err(TableAccessError::LoadRowsError(format!("Column '{}' not found!", col_name)));
        }

        for page in PageIterator::new(self.table, self.store, self.layout) {
            let row_iterator = PageRowIterator::new(&page, self.table.schema());

            for record_row in row_iterator {
                let row = record_row.1;
                if row.cells()[col_index] == cell {
                    result.push(row);
                }
            }
        }

        Ok(result)
    }

    // Currently maximally naive insert implementation
    // Should be refactored, so that FSM is used to find pages with free space
    pub fn insert(&self, row: &Row) -> Result<(), TableAccessError> {
        row.validate(self.table.schema())?;

        let page_iterator = self.store.page_iterator(self.layout, self.table)
            .map_err(|_| TableAccessError::InsertRowError("Cannot retrieve page iterator".to_string()))?;

        let mut inserted = false;
        for mut page in page_iterator {
            let row_data = row.serialize();
            if page.can_insert(&row_data) {
                page.insert_record(row_data)?;
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
            new_page.insert_record(row_data)?;

            self.store.write_page(self.layout, &new_page, self.table)
                .map_err(|_| TableAccessError::InsertRowError("Cannot write new allocated page".to_string()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::{data::page::PageDataLayout, database::access::TableAccess, store::file_store::FileStore, table::{Column, ColumnType, TableSchema, table::{Cell, Row, Table}}};


    #[test]
    fn should_insert_two_rows() {
        let schema = TableSchema::new(vec![
            Column::new(1, "name", ColumnType::Varchar(10))
        ]);

        let table = Table::new(1, "test".to_owned(), schema);
        let base_dir = tempdir().unwrap();
        let store = FileStore::new(base_dir.path());
        let layout = PageDataLayout::new(64).unwrap();

        let access = TableAccess::new(&table, &store, &layout);

        let first_row = Row::new(vec![
            Cell::Varchar("Hans".to_owned())
        ]);
        let second_row = Row::new(vec![
            Cell::Varchar("Rabbit".to_owned())
        ]);

        access.insert(&first_row).unwrap();
        access.insert(&second_row).unwrap();

        let rows = access.load_all().unwrap();
        assert_eq!(rows.len(), 2);
        // TODO: Test Cells
    }

    #[test]
    fn should_find_a_row() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(10))
        ]);

        let table = Table::new(1, "test".to_owned(), schema);
        let base_dir = tempdir().unwrap();
        let store = FileStore::new(base_dir.path());
        let layout = PageDataLayout::new(64).unwrap();

        let access = TableAccess::new(&table, &store, &layout);

        let first_row = Row::new(vec![
            Cell::Int(1),
            Cell::Varchar("Hans".to_owned())
        ]);

        let second_row = Row::new(vec![
            Cell::Int(2),
            Cell::Varchar("Rabbit".to_owned())
        ]);

        access.insert(&first_row).unwrap();
        access.insert(&second_row).unwrap();

        let rows = access.find("name", Cell::Varchar("Hans".to_owned())).unwrap();
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        assert!(matches!(row.cells().as_slice(), [Cell::Int(id), Cell::Varchar(name)] if *id == 1 && name == "Hans"))
    }

}