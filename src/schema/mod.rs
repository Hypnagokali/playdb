use std::path::Path;

use crate::tree::store::BTreeStore;

#[derive(Debug)]
pub enum ColumnType {
    Int,
    Varchar(u16),
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
    pub index_file: Option<String>,
}

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(name: &str, columns: Vec<Column>) -> Self {
        Self {
            name: name.to_string(),
            columns,
        }
    }
}

impl Column {
    pub fn new(name: &str, col_type: ColumnType) -> Self {
        Self {
            name: name.to_string(),
            col_type,
            index_file: None,
        }
    }

    pub fn new_with_index(name: &str, col_type: ColumnType, index_file: &str) -> Self {
        let p = Path::new(index_file);

        // create B+ tree index and store in Column
        let _ = BTreeStore::new(p, 6).unwrap();
        Self {
            name: name.to_string(),
            col_type,
            index_file: Some(index_file.to_owned()),
        }
    }
}