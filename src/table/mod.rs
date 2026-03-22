use std::fmt::Display;

pub mod table;
// Table: play_attribute

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnType { // Byte type
    Int,            // 0x01
    Varchar(u16),   // 0x02 length is stored separately
    Byte,           // 0x03
}
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub id: i32,
    pub name: String,
    pub col_type: ColumnType,
}

// needs Clone for now, because it is shared across QueryResult and this is the quickest solution
#[derive(Debug, Clone, PartialEq)]
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

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::Int => f.write_str("Int"),
            ColumnType::Varchar(_) => f.write_str("Varchar"),
            ColumnType::Byte => f.write_str("Byte"),
        }
    }
}

impl ColumnType {
    pub fn is_var_size(&self) -> bool {
        match self {
            ColumnType::Int => false,
            ColumnType::Varchar(_) => true,
            ColumnType::Byte => false,
        }
    }

    pub fn raw_type(&self) -> ColumnType {
        match self {
            ColumnType::Int => ColumnType::Int,
            ColumnType::Varchar(_) => ColumnType::Varchar(0),
            ColumnType::Byte => ColumnType::Byte,
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