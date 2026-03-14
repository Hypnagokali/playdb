use std::{fs::File, io::{Read, Seek, SeekFrom}};
use thiserror::Error;

const PAGE_SIZE: usize = 4096;

const HEADER_SIZE: usize = std::mem::size_of::<usize>() +
        std::mem::size_of::<u32>() +
        std::mem::size_of::<u16>();
const DATA_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

#[derive(Default)]
pub struct PageHeader {
    num_rows: u16,
    page_number: u32,
    offset: usize,
}

pub struct Page {
    data: Vec<u8>,
    header: PageHeader,
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
            header: PageHeader::default(),
        }
    }

    pub fn create_next(&self) -> Self {
        let mut new = Self::new();
        new.header.page_number = self.header.page_number + 1;
        new
    }

    pub fn pages(file: &File) -> usize {
        let metadata = file.metadata().unwrap();
        (metadata.len() / PAGE_SIZE as u64) as usize
    }

    pub fn last_page(file: &mut File) -> Option<Self> {
        let metadata = file.metadata().unwrap();
        if metadata.len() == 0 {
            None
        } else {
            let last_page_number = metadata.len() / PAGE_SIZE as u64 - 1;
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
        println!("Page loaded: number {}, rows {}, offset {}", p.header.page_number, p.header.num_rows, p.header.offset);
        Ok(p)
    }

    pub fn space_remaining(&self) -> usize {
        PAGE_SIZE - self.header.offset
    }

    pub fn number(&self) -> u32 {
        self.header.page_number
    }

    pub fn insert_row(&mut self, row_bytes: &[u8], file: &mut File) -> Result<(), PageError> {
        use std::io::Write;

        if self.header.offset + row_bytes.len() > DATA_SIZE {
            return Err(PageError::InsertRowError);
        }

        let end = self.header.offset + row_bytes.len();
        self.data[self.header.offset..end].copy_from_slice(row_bytes);
        self.header.offset += row_bytes.len();
        self.header.num_rows += 1;

        file.seek(SeekFrom::Start((self.header.page_number as usize * PAGE_SIZE) as u64))
            .map_err(|_| PageError::InsertRowError)?;

        file.write_all(&self.serialize())
            .map_err(|_| PageError::InsertRowError)?;
        
        Ok(())
    }

    fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        // Header
        buf[0..2].copy_from_slice(&self.header.num_rows.to_be_bytes());
        let offset_bytes = (self.header.offset).to_be_bytes();
        buf[2..10].copy_from_slice(&offset_bytes);
        let page_number_bytes = self.header.page_number.to_be_bytes();
        buf[10..14].copy_from_slice(&page_number_bytes);

        // Data
        buf[14..PAGE_SIZE].copy_from_slice(&self.data);
        buf
    }

    fn deserialize(buf: &[u8]) -> Self {
        let num_rows = u16::from_be_bytes(buf[..2].try_into().unwrap());
        let offset = usize::from_be_bytes(buf[2..10].try_into().unwrap());
        let page_number = u32::from_be_bytes(buf[10..14].try_into().unwrap());

        let data = buf[14..PAGE_SIZE].to_vec();
        let header = PageHeader {
            offset,
            page_number,
            num_rows,
        };
        Self {
            data,
            header,
        }
    }
}
