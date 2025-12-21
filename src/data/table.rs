use std::fs::OpenOptions;

use thiserror::Error;

use crate::{data::page::Page, schema::{self, ColumnType, TableSchema}};

#[derive(Debug)]
pub enum Cell {
    Int(i32),
    Varchar(String),
}

#[derive(Debug)]
pub struct Row {
    cells: Vec<Cell>,
}

pub struct Table {
    pub schema: TableSchema,
    pub file_path: String,
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
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for cell in &self.cells {
            bytes.extend(cell.serialize());
        }
        bytes
    }

    // ToDo: return Result<Row, RowDeserializationError> instead of using unwrap
    fn deserialize(row_data: &[u8], schema: &TableSchema) -> Self {
        let mut next = row_data;
        let mut cells = Vec::new();
        for col in schema.columns.iter() {
            let (cell, next_bytes) = Cell::deserialize(next, &col).unwrap();
            next = next_bytes;
            cells.push(cell);
        }

        Row {
            cells,
        }
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
    pub fn drop(&self) {
        std::fs::remove_file(&self.file_path).expect("Failed to delete table file");
    }

    pub fn insert(&self, row: &Row) {
        let row_validate_result = row.validate(&self.schema);
        if row_validate_result.is_err() {
            panic!("Row validation failed: {}", row_validate_result.unwrap_err());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&self.file_path)
            .expect("Failed to open table file");

        // Iterate over pages and find place to insert (just for simplicity)
        let pages = Page::pages(&file);
        let row_bytes = row.serialize();
        let mut row_written = false;

        for page_counter in 0..pages {
            println!("Is there space in page {}?", page_counter);
            let mut page = Page::from_file(&mut file, page_counter)
                    .expect("Failed to read page from file");

            let space = page.space_remaining();

            println!("Space left in page: {}", space);

            if space >= row_bytes.len() {
                println!("There is enough space in page. Write row to page {}", page_counter);

                match page.insert_row(&row_bytes, &mut file) {
                    Ok(_) => {
                        println!("Saved to page !!!");
                        row_written = true;
                        break;
                    }
                    Err(e) => {
                        panic!("Failed to insert row into page: {}", e);
                    }
                }
            }
        }

        if !row_written {
            println!("No existing page had enough space. Create new page.");
            let last_page = Page::last_page(&mut file);
            let mut new_page = match last_page {
                Some(ref p) => p.create_next(),
                None => Page::new(),
            };
            new_page.insert_row(&row_bytes, &mut file)
                .expect("Failed to insert row into new page");
        }

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
        }
    }

    pub fn deserialize<'a>(row_data: &'a [u8], column: &schema::Column) -> Result<(Self, &'a [u8]), CellDeserializationError> {
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
                Ok((Cell::Int(int_value), &row_data[4..]))
            }
            ColumnType::Varchar(_) => {
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

                let str_bytes = &row_data[2..2 + str_len];
                let str_value = String::from_utf8(str_bytes.to_vec())
                    .map_err(|_| CellDeserializationError::InvalidData)?;

                Ok((Cell::Varchar(str_value), &row_data[2 + str_len..]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::table::{Cell, Row, RowValidationError, Table};
    use crate::schema::{Column, ColumnType, TableSchema};

    #[test]
    fn should_insert_table() {
        let columns = vec![
            Column::new("id", ColumnType::Int),
            Column::new("name", ColumnType::Varchar(25)),
        ];
        let schema = TableSchema::new("TestTable", columns);
        let table = Table {
            schema,
            file_path: "test_table.dat".to_string(),
            num_pages: 0,
        };

        let row = Row {
            cells: vec![
                Cell::Int(1),
                Cell::Varchar("Alice".to_string()),
            ],
        };

        // Insert first row
        table.insert(&row);
        let file = std::fs::metadata("test_table.dat").unwrap();
        assert!(file.len() == 4096);

        // Insert second row
        table.insert(&row);
        let file = std::fs::metadata("test_table.dat").unwrap();
        assert!(file.len() == 4096);

        // Clean up
        table.drop();
        let dropped_file = std::fs::metadata("test_table.dat");
        assert!(dropped_file.is_err());
        assert!(dropped_file.unwrap_err().kind() == std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_row_validation() { 
        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    col_type: ColumnType::Int,
                },
                Column {
                    name: "name".to_string(),
                    col_type: ColumnType::Varchar(10),
                },
            ],
        };

        let valid_row = Row {
            cells: vec![
                Cell::Int(1),
                Cell::Varchar("Alice".to_string()),
            ],
        };

        assert!(valid_row.validate(&schema).is_ok());

        let invalid_row_length = Row {
            cells: vec![
                Cell::Int(1),
            ],
        };

        let invalid_row_length_res  = invalid_row_length.validate(&schema);
        assert!(invalid_row_length_res.is_err());
        assert_eq!(invalid_row_length_res.unwrap_err(), RowValidationError::LengthMismatch);

        let invalid_row_type = Row {
            cells: vec![
                Cell::Varchar("NotAnInt".to_string()),
                Cell::Varchar("Bob".to_string()),
            ],
        };

        let invalid_row_type_res = invalid_row_type.validate(&schema);
        assert!(invalid_row_type_res.is_err());
        assert_eq!(invalid_row_type_res.unwrap_err(), RowValidationError::TypeMismatch("id".to_string()));

        let invalid_row_varchar_length = Row {
            cells: vec![
                Cell::Int(2),
                Cell::Varchar("ThisNameIsWayTooLong".to_string()),
            ],
        };

        let invalid_row_varchar_length_res = invalid_row_varchar_length.validate(&schema);
        assert!(invalid_row_varchar_length_res.is_err());
        assert_eq!(invalid_row_varchar_length_res.unwrap_err(), RowValidationError::VarcharTooLong(10, "name".to_string()));
    }

}