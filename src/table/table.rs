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
    cells: Vec<Cell>,
}

pub struct Table {
    id: i32,
    name: String,
    schema: TableSchema,
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
    pub fn new(cells: Vec<Cell>) -> Self {
        Self {
            cells,
        }
    }

    pub fn cells(&self) -> &Vec<Cell> {
        &self.cells
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Remaining bytes: cells
        for cell in &self.cells {
            bytes.extend(cell.serialize());
        }
        bytes
    }

    // ToDo: return Result<Row, RowDeserializationError> instead of using unwrap
    pub fn deserialize(row_data: &[u8], schema: &TableSchema) -> Self {
        let mut cells = Vec::new();
        let mut offset = 0;
        for col in schema.columns.iter() {
            let (cell, bytes_read) = Cell::deserialize(&row_data[offset..], &col).unwrap();
            offset += bytes_read;
            cells.push(cell);
        }

        Row { cells }
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
    pub fn new(id: i32, name: String, schema: TableSchema) -> Self {
        Self {
            id,
            name,
            schema,
            num_pages: 0,
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn num_pages(&self) -> usize {
        self.num_pages
    }

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
    fn should_serialize_and_deserialize_correctly() {
        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int),
            Column::new(2, "name", ColumnType::Varchar(50)),
            Column::new(3, "flag", ColumnType::Byte),
        ]);

        let cells = vec![
            Cell::Int(42),
            Cell::Varchar("John".to_string()),
            Cell::Byte(1),
        ];

        let row = Row {
            cells,
        };

        let serialized = row.serialize();

        let deserialized_row = Row::deserialize(&serialized, &schema);
        let cells = deserialized_row.cells;

        assert!(matches!(&cells[0], Cell::Int(42)));
        assert!(matches!(&cells[1], Cell::Varchar(s) if s == "John"));
        assert!(matches!(&cells[2], Cell::Byte(1)));
    }

    #[test]
    fn table_should_return_table_path_correctly() {
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
            cells: vec![
                Cell::Varchar("This string is way too long for the column".to_string()),
            ],
        };

        let result = table.validate_row(&invalid_row);
        assert!(result.is_err());
        matches!(result.unwrap_err(), RowValidationError::VarcharTooLong(10, name) if name == "name");
    }
}