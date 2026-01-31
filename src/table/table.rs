use thiserror::Error;

use crate::{table::{self, ColumnType, TableSchema}};

#[derive(Debug)]
pub enum Cell {
    Int(i32),
    Varchar(String),
    Byte(u8),
}

#[derive(Debug)]
pub struct Row {
    // move deleted and index later into something like PageRow?
    deleted: bool,
    index: i32,
    cells: Vec<Cell>,
}

pub struct Table {
    pub id: i32,
    pub name: String,
    pub schema: TableSchema,
    num_pages: usize,
}

#[derive(PartialEq, Debug, Error)]
pub enum RowValidationError {
    #[error("Row length does not match schema")]
    LengthMismatch,
    #[error("Type mismatch for column '{0}'")]
    TypeMismatch(String),
    #[error("Varchar length exceeds maximum of {0} for column '{1}'")]
    VarcharTooLong(u16, String),
}

impl Row {
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // First 4 bytes: index
        bytes.extend(self.index.to_be_bytes());
        // 5th byte: deleted flag
        bytes.push(if self.deleted { 1 } else { 0 });
        // Remaining bytes: cells
        for cell in &self.cells {
            bytes.extend(cell.serialize());
        }
        bytes
    }

    // ToDo: return Result<Row, RowDeserializationError> instead of using unwrap
    pub fn deserialize(row_data: &[u8], schema: &TableSchema) -> (Self, usize) {
        let mut cells = Vec::new();
        let mut offset = 0;
        let index = i32::from_be_bytes(row_data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let deleted = if row_data.len() > 0 && row_data[4] != 0 { true } else { false };
        offset += 1;
        for col in schema.columns.iter() {
            let (cell, bytes_read) = Cell::deserialize(&row_data[offset..], &col).unwrap();
            offset += bytes_read;
            cells.push(cell);
        }

        (Row { deleted, index, cells }, offset)
    }

    pub fn validate(&self, schema: &TableSchema) -> Result<(), RowValidationError> {
        if self.cells.len() != schema.columns.len() {
            return Err(RowValidationError::LengthMismatch);
        }

        for (cell, column) in self.cells.iter().zip(schema.columns.iter()) {
            match (cell, &column.col_type) {
                (Cell::Int(_), ColumnType::Int) => {
                    // always valid
                }
                (Cell::Varchar(input), ColumnType::Varchar(max_len)) => {
                    if input.len() > *max_len as usize {
                        return Err(RowValidationError::VarcharTooLong(*max_len, column.name.clone()));
                    }
                }
                (Cell::Byte(_), ColumnType::Byte) => {
                    // always valid
                }
                _ => {
                    return Err(RowValidationError::TypeMismatch(column.name.clone()));
                }
            }
        }

        Ok(())
    }
}

impl Table {
    pub fn file_path(&self) -> String {
        format!("table_{}.dat", self.id)
    }

    pub fn validate_row(&self, row: &Row) -> Result<(), RowValidationError> {
        row.validate(&self.schema)
    }
}


#[derive(Debug, Error)]
pub enum CellDeserializationError {
    #[error("Cell deserialization error")]
    InvalidData,
}

impl Cell {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Cell::Int(i) => i.to_be_bytes().to_vec(),
            Cell::Varchar(s) => {
                // 2 Bytes for length + actual string bytes
                let mut bytes = (s.len() as u16).to_be_bytes().to_vec();
                bytes.extend_from_slice(s.as_bytes());
                bytes
            },
            Cell::Byte(b) => {
                vec![b.clone()]
            }
        }
    }

    // ToDo: better error handling
    // Gets always the next slice of the row_data
    // Returns: (Cell, number of bytes read)
    pub fn deserialize<'a>(row_data: &'a [u8], column: &table::Column) -> Result<(Self, usize), CellDeserializationError> {
        match &column.col_type {
            ColumnType::Int => {
                if row_data.len() < 4 {
                    return Err(CellDeserializationError::InvalidData);
                }
                let int_bytes = &row_data[0..4];
                let int_value = i32::from_be_bytes(
                    int_bytes.try_into()
                        .map_err(|_| CellDeserializationError::InvalidData)?
                );
                Ok((Cell::Int(int_value), 4))
            }
            ColumnType::Varchar(len) => {
                if row_data.len() < 2 {
                    // needs at least 2 bytes for length
                    return Err(CellDeserializationError::InvalidData);
                }
                let len_bytes = &row_data[0..2];
                let str_len = u16::from_be_bytes(
                    len_bytes.try_into()
                        .map_err(|_| CellDeserializationError::InvalidData)?
                ) as usize;
                if row_data.len() < (2 + str_len) {
                    return Err(CellDeserializationError::InvalidData);
                }

                if str_len > *len as usize {
                    return Err(CellDeserializationError::InvalidData);
                }

                let str_bytes = &row_data[2..2 + str_len];
                let str_value = String::from_utf8(str_bytes.to_vec())
                    .map_err(|_| CellDeserializationError::InvalidData)?;

                Ok((Cell::Varchar(str_value), 2 + str_len))
            },
            ColumnType::Byte => {
                if row_data.len() < 1 {
                    return Err(CellDeserializationError::InvalidData);
                }
                let byte_value = row_data[0];
                Ok((Cell::Byte(byte_value), 1))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Column;

    #[test]
    fn test_row_serialize_basic() {
        let cells = vec![
            Cell::Int(42),
            Cell::Varchar("hello".to_string()),
            Cell::Byte(1),
        ];

        let row = Row {
            deleted: false,
            index: 100,
            cells,
        };

        let serialized = row.serialize();

        // Index
        assert_eq!(&serialized[0..4], &100i32.to_be_bytes());
        // Deleted flag
        assert_eq!(serialized[4], 0);
        // Cell 1: Int
        assert_eq!(&serialized[5..9], &42i32.to_be_bytes());
        // Varchar("hello"): 2 bytes length + 5 bytes data
        assert_eq!(&serialized[9..11], &5u16.to_be_bytes());
        assert_eq!(&serialized[11..16], b"hello");
        // Byte(1): 1 byte
        assert_eq!(serialized[16], 1);
    }

    #[test]
    fn test_row_serialize_deleted_flag() {
        let cells = vec![Cell::Int(99)];

        let row = Row {
            deleted: true,
            index: 50,
            cells,
        };

        let serialized = row.serialize();

        // Index: 50
        assert_eq!(&serialized[0..4], &50i32.to_be_bytes());
        // Deleted: true
        assert_eq!(serialized[4], 1);
    }

    #[test]
    fn test_row_serialize_empty_varchar() {
        let cells = vec![Cell::Varchar(String::new())];

        let row = Row {
            deleted: false,
            index: 0,
            cells,
        };

        let serialized = row.serialize();

        // Index: 0
        assert_eq!(&serialized[0..4], &0i32.to_be_bytes());
        // Deleted: false
        assert_eq!(serialized[4], 0);
        // Varchar length: 0
        assert_eq!(&serialized[5..7], &0u16.to_be_bytes());
        assert_eq!(serialized.len(), 7);
    }

    #[test]
    fn test_row_deserialize_basic() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(50)),
            Column::new(3, "flag", ColumnType::Byte),
        ]);

        let mut data = Vec::new();
        // Index: 100
        data.extend(100i32.to_be_bytes());
        // Deleted: false
        data.push(0);
        // Int(42)
        data.extend(42i32.to_be_bytes());
        // Varchar("hello")
        data.extend(5u16.to_be_bytes());
        data.extend_from_slice(b"hello");
        // Byte(1)
        data.push(1);

        let (row, bytes_read) = Row::deserialize(&data, &schema);

        assert_eq!(row.index, 100);
        assert_eq!(row.deleted, false);
        assert_eq!(bytes_read, data.len());

        let cells = row.cells;

        assert!(matches!(&cells[0], Cell::Int(42)));
        assert!(matches!(&cells[1], Cell::Varchar(s) if s == "hello"));
        assert!(matches!(&cells[2], Cell::Byte(1)));
    }

    #[test]
    fn test_row_deserialize_deleted_flag() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
        ]);

        let mut data = Vec::new();
        // Index: 50
        data.extend(50i32.to_be_bytes());
        // Deleted: true
        data.push(1);
        // Int(99)
        data.extend(99i32.to_be_bytes());

        let (row, _) = Row::deserialize(&data, &schema);

        assert_eq!(row.index, 50);
        assert_eq!(row.deleted, true);
    }

    #[test]
    fn test_table_file_path() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
        ]);

        let table = Table {
            id: 42,
            name: "users".to_string(),
            schema,
            num_pages: 5,
        };

        assert_eq!(table.file_path(), "table_42.dat");
    }

    #[test]
    fn should_validate_valid_row() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(50)),
            Column::new(3, "active", ColumnType::Byte),
        ]);

        let table = Table {
            id: 1,
            name: "users".to_string(),
            schema,
            num_pages: 0,
        };

        let valid_row = Row {
            deleted: false,
            index: 1,
            cells: vec![
                Cell::Int(100),
                Cell::Varchar("John Doe".to_string()),
                Cell::Byte(128),
            ],
        };

        assert!(table.validate_row(&valid_row).is_ok());
    }

    #[test]
    fn should_not_validate_row_with_extra_cell() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(50)),
        ]);

        let table = Table {
            id: 1,
            name: "users".to_string(),
            schema,
            num_pages: 0,
        };

        let invalid_row = Row {
            deleted: false,
            index: 1,
            cells: vec![
                Cell::Int(100),
                Cell::Varchar("John Doe".to_string()),
                Cell::Byte(1), // Extra cell
            ],
        };

        let result = table.validate_row(&invalid_row);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RowValidationError::LengthMismatch));
    }

    #[test]
    fn should_not_validate_when_type_mismatch() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(50)),
        ]);

        let table = Table {
            id: 1,
            name: "users".to_string(),
            schema,
            num_pages: 0,
        };

        let invalid_row = Row {
            deleted: false,
            index: 1,
            cells: vec![
                Cell::Int(100),
                Cell::Byte(1),
            ],
        };

        let result = table.validate_row(&invalid_row);
        assert!(result.is_err());
        
        matches!(result.unwrap_err(), RowValidationError::TypeMismatch(name) if name == "name");
    }

    #[test]
    fn test_table_validate_row_varchar_too_long() {
        let schema = TableSchema::new(vec![
            Column::new(1, "name", ColumnType::Varchar(10)),
        ]);

        let table = Table {
            id: 1,
            name: "users".to_string(),
            schema,
            num_pages: 0,
        };

        // Row with varchar longer than max length
        let invalid_row = Row {
            deleted: false,
            index: 1,
            cells: vec![
                Cell::Varchar("This string is way too long for the column".to_string()),
            ],
        };

        let result = table.validate_row(&invalid_row);
        assert!(result.is_err());
        matches!(result.unwrap_err(), RowValidationError::VarcharTooLong(10, name) if name == "name");
    }
}