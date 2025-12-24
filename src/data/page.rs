use std::{fs::File, io::{Read, Seek, SeekFrom}};
use thiserror::Error;

use crate::{data::table::Row, schema::TableSchema};

const PAGE_SIZE: usize = 4096;

// Todo: should not be a constant
const DATA_SIZE: usize = 4086;

pub struct Page {
    data: Vec<u8>,
    offset: usize,
    page_number: u32,
    num_rows: u16,
}

pub struct PageIterator {
    file: File,
    current_page: usize,
    total_pages: usize,
}

impl PageIterator {
    pub fn new(file: File) -> Self {
        let total_pages = Page::count_pages(&file);
        Self {
            file,
            current_page: 0,
            total_pages,
        }
    }
}

impl Iterator for PageIterator {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page >= self.total_pages {
            return None;
        }

        let page = Page::from_file(&mut self.file, self.current_page).ok()?;
        self.current_page += 1;
        Some(page)
    }
}

pub struct PageRowIterator<'a> {
    data: &'a [u8],
    offset: usize,
    end: usize,
    schema: &'a TableSchema,
}

impl<'a> PageRowIterator<'a> {
    pub fn new(page: &'a Page, schema: &'a TableSchema) -> Self {
        Self { 
            data: &page.data,
            offset: 0,
            end: page.offset,
            schema 
        }
    }
}

impl Iterator for PageRowIterator<'_> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.end {
            return None;
        }

        let (next_row, byte_offset) = Row::deserialize(&self.data[self.offset..self.end], self.schema);

        self.offset += byte_offset;
        Some(next_row)
    }
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Failed to insert row into page. Page is full.")]
    InsertRowError,
    #[error("Failed to read page from file.")]
    ReadPageError,
}

#[cfg(target_pointer_width = "64")]
impl Page {
    pub fn new() -> Self {
        Self {
            data: vec![0; DATA_SIZE],
            offset: 0,
            num_rows: 0,
            page_number: 0,
        }
    }

    pub fn row_data(&self) -> &[u8] {
        &self.data
    }

    pub fn create_next(&self) -> Self {
        let mut new = Self::new();
        new.page_number = self.page_number + 1;
        new
    }

    pub fn count_pages(file: &File) -> usize {
        let metadata = file.metadata().unwrap();
        (metadata.len() / PAGE_SIZE as u64) as usize
    }

    pub fn last_page(file: &mut File) -> Option<Self> {
        let metadata = file.metadata().unwrap();
        if metadata.len() == 0 {
            None
        } else {
            let last_page_number = (metadata.len() / PAGE_SIZE as u64 - 1);
            Self::from_file(file, last_page_number as usize).ok()
        }
    }

    pub fn from_file(file: &mut File, page_counter: usize) -> Result<Page, PageError> {
        let mut data = vec![0; PAGE_SIZE];
        file.seek(SeekFrom::Start((page_counter * PAGE_SIZE) as u64))
            .map_err(|_| PageError::ReadPageError)?;
    
        file.read_exact(&mut data)
            .map_err(|e| {
                println!("Error reading page: {}", e);
                PageError::ReadPageError
            })?;

        let p = Page::deserialize(&data);
        println!("Page loaded: number {}, rows {}, offset {}", p.page_number, p.num_rows, p.offset);
        Ok(p)
    }

    pub fn space_remaining(&self) -> usize {
        PAGE_SIZE - self.offset
    }

    pub fn number(&self) -> u32 {
        self.page_number
    }

    pub fn page_iter(file: File) -> PageIterator {
        PageIterator::new(file)
    }

    // What about moving self here?
    pub fn rows<'a>(&'a self, schema: &'a TableSchema) -> PageRowIterator<'a> {
        PageRowIterator::new(self, schema)
    }

    pub fn insert_row(&mut self, row_bytes: &[u8], file: &mut File) -> Result<(), PageError> {
        use std::io::Write;

        if self.offset + row_bytes.len() > DATA_SIZE {
            return Err(PageError::InsertRowError);
        }

        let end = self.offset + row_bytes.len();
        self.data[self.offset..end].copy_from_slice(row_bytes);
        self.offset += row_bytes.len();
        self.num_rows += 1;

        file.seek(SeekFrom::Start((self.page_number as usize * PAGE_SIZE) as u64))
            .map_err(|_| PageError::InsertRowError)?;

        file.write_all(&self.serialize())
            .map_err(|_| PageError::InsertRowError)?;
        
        Ok(())
    }

    fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        // Number of rows 2 Bytes
        buf[0..2].copy_from_slice(&self.num_rows.to_be_bytes());
        // Offset 4 Bytes
        let offset_bytes = (self.offset as u32).to_be_bytes();
        buf[2..6].copy_from_slice(&offset_bytes);
        // Page number 4 Bytes
        let page_number_bytes = self.page_number.to_be_bytes();
        buf[6..10].copy_from_slice(&page_number_bytes);

        buf[10..PAGE_SIZE].copy_from_slice(&self.data);
        buf
    }

    fn deserialize(buf: &[u8]) -> Self {
        let num_rows = u16::from_be_bytes([buf[0], buf[1]]);
        let offset = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let page_number = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);
        let data = buf[10..PAGE_SIZE].to_vec();
        Self {
            num_rows,
            offset: offset as usize,
            page_number,
            data,
        }
    }
}
