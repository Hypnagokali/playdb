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
    deleted: bool,
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
        // First byte: deleted flag
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
        // First byte: deleted flag
        let deleted = if row_data.len() > 0 && row_data[0] != 0 { true } else { false };
        offset += 1;
        for col in schema.columns.iter() {
            let (cell, bytes_read) = Cell::deserialize(&row_data[offset..], &col).unwrap();
            offset += bytes_read;
            cells.push(cell);
        }

        (Row { deleted, cells }, offset)
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
