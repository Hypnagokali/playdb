use std::{collections::HashMap, rc::Rc};

use thiserror::Error;

use crate::{data::page::{PageDataLayout, PageError}, store::{PageIterator, PageRowIterator, Store}, table::{Column, TableSchema, table::{Cell, Row, RowValidationError, Table}}};

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

pub struct QueryResult {
    schema: Rc<TableSchema>,
    rows: Vec<Row>,
}

impl QueryResult {
    fn find(self, col_name: &str, cell: Cell) -> Result<QueryResult, TableAccessError> {
        let col_index = find_column_for_query_by_cell(&self.schema, col_name, &cell)?;

        let result: Vec<Row> = self.rows.into_iter().filter(|r| {
            r.cells()[col_index] == cell
        }).collect();

        Ok(QueryResult { schema: self.schema, rows: result })    
    }
    
    fn rows(&self) -> &Vec<Row> {
        &self.rows
    }
}

fn find_column_for_query(schema: &TableSchema, col_name: &str) -> Result<usize, TableAccessError> {
    let mut col_index = 0;
    let mut col_found = false;
    for (index, col) in schema.columns.iter().enumerate() {
        if col.name == col_name.trim() {
            col_index = index;
            col_found = true;
            break;
        }
    }

    if !col_found {
        return Err(TableAccessError::LoadRowsError(format!("Column '{}' not found!", col_name)));
    }

    Ok(col_index)
}

fn find_column_for_query_by_cell(schema: &TableSchema, col_name: &str, cell: &Cell) -> Result<usize, TableAccessError> {
    let col_index = find_column_for_query(schema, col_name)?;
    
    let ref_column = &schema.columns[col_index];
    if !cell.is_of_type(&ref_column.col_type) {
        return Err(TableAccessError::LoadRowsError(format!("Column '{}' is of type {} not {}", col_name, ref_column.col_type, cell.column_type())));
    }

    Ok(col_index)
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
    pub fn find_all(&self) -> Result<QueryResult, TableAccessError> {
        let mut rows = Vec::new();

        // Read metadata to know how many pages exist
        for page in PageIterator::new(self.table, self.store, self.layout) {
            let row_iterator = PageRowIterator::new(&page, self.table.schema());

            for record_row in row_iterator {
                rows.push(record_row.1);
            }
        }

        Ok(QueryResult { schema: Rc::new(self.table.schema().clone()), rows })
    }

    pub fn find(&self, col_name: &str, cell: Cell) -> Result<QueryResult, TableAccessError> {
        // Full table scan:
        let mut result = Vec::new();
        let col_index = find_column_for_query_by_cell(self.table.schema(), col_name, &cell)?;


        for page in PageIterator::new(self.table, self.store, self.layout) {
            let row_iterator = PageRowIterator::new(&page, self.table.schema());

            for record_row in row_iterator {
                let row = record_row.1;
                if row.cells()[col_index] == cell {
                    result.push(row);
                }
            }
        }

        Ok(QueryResult { schema: Rc::new(self.table.schema().clone()), rows: result })
    }

    // Performs a inner join with Hash Join
    pub fn join(&self, join_table_access: &TableAccess<'_, S>, this_join_column: &str, that_join_column: &str) -> Result<QueryResult, TableAccessError> {
        let that_col_index = find_column_for_query(join_table_access.table.schema(), that_join_column)?;
        let this_col_index = find_column_for_query(self.table.schema(), this_join_column)?;

        // 1. check if type is equal
        let this_col_type = self.table.schema().columns[this_col_index].col_type.raw_type();
        let that_col_type = join_table_access.table.schema().columns[that_col_index].col_type.raw_type();

        if this_col_type != that_col_type {
            return Err(TableAccessError::LoadRowsError(format!("Join columns have different types: {} vs {}", this_col_type, that_col_type)));
        }

        // 2. create hashmap with this type as key        
        let mut inner_table_hashes = HashMap::new();
        let inner_result= join_table_access.find_all()?;
        for r in inner_result.rows().iter() {
            let join_key = &r.cells()[that_col_index];
            inner_table_hashes.entry(join_key)
                .or_insert_with(Vec::new)
                .push(r);
        }

        // 3. build join QueryResult
        let outer_result = self.find_all()?;
        let mut result = Vec::new();
        for r in outer_result.rows().iter() {
            if let Some(join_tuples) = inner_table_hashes.get(&r.cells()[this_col_index]) {
                for &inner_row in join_tuples {
                    let joined_cells: Vec<Cell> = r.cells().iter()
                    .chain(inner_row.cells().iter())
                    .map(|c| (*c).clone())
                    .collect();
                    result.push(Row::new(joined_cells));
                }
            }
        }
        let joined_cols: Vec<Column> = self.table.schema().columns
            .iter()
            .chain(join_table_access.table.schema().columns.iter())
            .map(|col| (*col).clone())
            .collect();

        let joined_schema = TableSchema::new(joined_cols);

        Ok(QueryResult { schema: Rc::new(joined_schema), rows: result })  
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

        let result = access.find_all().unwrap();
        let rows = result.rows();
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

        let result = access.find("name", Cell::Varchar("Hans".to_owned())).unwrap();
        let rows = result.rows();
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        assert!(matches!(row.cells().as_slice(), [Cell::Int(id), Cell::Varchar(name)] if *id == 1 && name == "Hans"));
    }

     #[test]
    fn should_find_multiple_rows() {
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
            Cell::Varchar("Hans".to_owned())
        ]);

        access.insert(&first_row).unwrap();
        access.insert(&second_row).unwrap();

        let result = access.find("name", Cell::Varchar("Hans".to_owned())).unwrap();
        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        let row = rows.get(0).unwrap();
        assert!(matches!(row.cells().as_slice(), [Cell::Int(id), Cell::Varchar(name)] if *id == 1 && name == "Hans"));

        let row = rows.get(1).unwrap();
        assert!(matches!(row.cells().as_slice(), [Cell::Int(id), Cell::Varchar(name)] if *id == 2 && name == "Hans"));
    }

    #[test]
    fn find_should_return_error_if_cell_has_wrong_type() {
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

        let rows = access.find("name", Cell::Int(1));
        assert!(rows.is_err());
        let err_message = rows.err().unwrap().to_string();
        assert!(err_message.contains("Column 'name' is of type Varchar not Int"));
    }

    #[test]
    fn should_join_two_tables() {
        let person_schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(10))
        ]);

        let person_table = Table::new(1, "persons".to_owned(), person_schema);
        let base_dir = tempdir().unwrap();
        let store = FileStore::new(base_dir.path());
        let layout: PageDataLayout = PageDataLayout::new(1024).unwrap();

        let person_access = TableAccess::new(&person_table, &store, &layout);

        let first_person = Row::new(vec![
            Cell::Int(1),
            Cell::Varchar("Hans".to_owned())
        ]);

        let second_person = Row::new(vec![
            Cell::Int(2),
            Cell::Varchar("Rabbit".to_owned())
        ]);

        person_access.insert(&first_person).unwrap();
        person_access.insert(&second_person).unwrap();

        let address_schema = TableSchema::new(vec![
            Column::new(1, "person_id", ColumnType::Int),
            Column::new(2, "address", ColumnType::Varchar(64))
        ]);

        let address_table = Table::new(2, "addresses".to_owned(), address_schema);
        let address_access = TableAccess::new(&address_table, &store, &layout);

        let first_address = Row::new(vec![
            Cell::Int(1),
            Cell::Varchar("Lilienstr 99".to_owned())
        ]);

        let second_address = Row::new(vec![
            Cell::Int(2),
            Cell::Varchar("Bergmansweg 10".to_owned())
        ]);

        address_access.insert(&first_address).unwrap();
        address_access.insert(&second_address).unwrap();

        let result = person_access.join(&address_access, "id", "person_id").unwrap();
        assert_eq!(result.schema.columns.len(), 4);
        assert_eq!(result.schema.columns[0], Column::new(1, "id", ColumnType::Int));
        assert_eq!(result.schema.columns[1], Column::new(2, "name", ColumnType::Varchar(10)));
        assert_eq!(result.schema.columns[2], Column::new(1, "person_id", ColumnType::Int));
        assert_eq!(result.schema.columns[3], Column::new(2, "address", ColumnType::Varchar(64)));

        assert_eq!(result.rows().len(), 2);
        let cells_hans = result.rows()[0].cells();
        assert_eq!(
            *cells_hans, 
            vec![Cell::Int(1), Cell::Varchar("Hans".to_owned()), Cell::Int(1), Cell::Varchar("Lilienstr 99".to_owned())]);

        let cells_rabbit = result.rows()[1].cells();
        assert_eq!(
            *cells_rabbit, 
            vec![Cell::Int(2), Cell::Varchar("Rabbit".to_owned()), Cell::Int(2), Cell::Varchar("Bergmansweg 10".to_owned())]);
    }

}