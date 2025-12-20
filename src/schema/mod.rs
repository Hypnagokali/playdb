#[derive(Debug)]
pub enum ColumnType {
    Int,
    Varchar(u16),
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub col_type: ColumnType,
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
        }
    }
}