pub mod table;
// Table: play_attribute

#[derive(Debug)]
pub enum ColumnType { // Byte type
    Int,            // 0x01
    Varchar(u16),   // 0x02 length is stored separately
    Byte,           // 0x03
}

#[derive(Debug)]
pub struct Column {
    pub id: i32,
    pub name: String,
    pub col_type: ColumnType,
}

#[derive(Debug)]
pub struct TableSchema {
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(columns: Vec<Column>) -> Self {
        if columns.is_empty() {
            panic!("Cannot crate empty schema");
        }
        Self {
            columns,
        }
    }
}

impl Column {
    pub fn new(id: i32, name: &str, col_type: ColumnType) -> Self {
        Self {
            id,
            name: name.to_string(),
            col_type,
        }
    }
}