use thiserror::Error;

pub struct PageDataLayout {
    page_size: usize,
}

#[derive(Error, Debug)]
pub enum PageDataLayoutError {
    #[error("Invalid page size specified. Page size must be at least 32 bytes.")]
    InvalidPageSize,
}

impl PageDataLayout {
    const INDEX_NUMBER_ROWS: usize = 0;
    const INDEX_ROW_OFFSET: usize = 2;
    const INDEX_PAGE_ID: usize = 6;
    const INDEX_FREE_SLOTS_OFFSET: usize = 10;

    // table meta data: 4 bytes next_id, 4 bytes number_of_pages
    pub const META_DATA_SIZE: usize = 8;
    // page header: 2 bytes num_rows, 4 bytes data_offset, 4 bytes page_id, 4 bytes slots_offset
    const PAGE_HEADER_SIZE: usize = 14;
    const MIN_PAGE_SIZE: usize = 32; // just arbitrarily chosen value > 10 and good to test :)


    // free data tuple constants
    const FREE_DATA_TUPLE_SIZE: usize = 6; // 4 bytes offset, 2 bytes length
    const FREE_DATA_TUPLE_OFFSET_INDEX: usize = 0;
    const FREE_DATA_TUPLE_LENGTH_INDEX: usize = 4;
    const MAX_ROW_LENGTH: u16 = u16::MAX;

    pub fn new(page_size: usize) -> Result<Self, PageDataLayoutError> {
        if page_size < Self::MIN_PAGE_SIZE {
            return Err(PageDataLayoutError::InvalidPageSize);
        }

        Ok(Self { page_size })
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn page_data_size(&self) -> usize {
        self.page_size - Self::PAGE_HEADER_SIZE
    }

    pub fn metadata_size(&self) -> usize {
        Self::META_DATA_SIZE
    }

}

pub struct PageFileMetadata {
    next_id: i32, // There is currently just a signed int for ids
    number_of_pages: i32, // because of next_id being i32
}

impl PageFileMetadata {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            number_of_pages: 0,
        }
    }
    pub fn deserialize(buf: &[u8]) -> Self {
        let next_id = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let number_of_pages = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        Self {
            next_id,
            number_of_pages,
        }
    }
    pub fn serialize(&self, layout: &PageDataLayout) -> Vec<u8> {
        let mut buf = vec![0u8; layout.metadata_size()];
        buf[0..4].copy_from_slice(&self.next_id.to_be_bytes());
        buf[4..8].copy_from_slice(&self.number_of_pages.to_be_bytes());
        buf
    }
    pub fn next_id(&self) -> i32 {
        self.next_id
    }

    pub fn number_of_pages(&self) -> i32 {
        self.number_of_pages
    }

    pub fn allocate_next_page_id(&mut self) -> i32 {
        let id = self.next_id;
        self.next_id += 1;
        self.number_of_pages += 1;
        id
    }
}

// Page Layout
// ------------
// Header
// ------------
// free_slots (go downwards)
// slot1
// slot2
// ...
// ...
// row2
// row1
// rows (go upwards)
pub struct Page<'database> {
    data: Vec<u8>,
    free_slots: Vec<(usize, u16)>,  // redundant (stored in data) (pointer to free space (on disk as i32), free bytes)
    layout: &'database PageDataLayout,
    // header
    num_rows: u16,
    // data_offset is actually the free space pointer
    // offset uses usize internally for easier handling (but it's actually an i32) 
    // starts from page_data_size and is heading towards 0
    data_offset: usize, 
    // Place where the next free slot can be inserted:
    // stored as i32 (starts from 0)
    free_slots_offset: usize,
    page_id: i32, // it's because of id being a i32
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Failed to insert row into page. Page is full.")]
    InsertRowError,
    #[error("Failed to read page from file.")]
    ReadPageError,
}

#[cfg(target_pointer_width = "64")] // so that I can use always 8 bytes for usize
impl<'database> Page<'database> {
    pub fn new(layout: &'database PageDataLayout) -> Self {
        Self {
            layout,
            data: vec![0; layout.page_data_size()],
            data_offset: layout.page_data_size(),
            num_rows: 0,
            page_id: 0,
            free_slots: Vec::new(),
            free_slots_offset: 0,
        }
    }

    pub fn row_data(&self) -> &[u8] {
        &self.data[self.data_offset..self.layout.page_data_size()]
    }
    pub fn data_offset(&self) -> usize {
        self.data_offset
    }

    pub fn page_number(&self) -> i32 {
        self.page_id
    }

    pub fn row_data_size(&self) -> usize {
        self.layout.page_data_size() - self.data_offset
    }

    pub fn num_rows(&self) -> u16 {
        self.num_rows
    }

    pub fn page_id(&self) -> i32 {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: i32) {
        self.page_id = page_id;
    }

    // There should be somewhere an allocation method for a new page
    pub fn create_next(&self) -> Self {
        let mut new = Self::new(self.layout);
        new.page_id = self.page_id + 1;
        new
    }

    pub fn space_remaining(&self) -> usize {
        // page_data_size - row_data_size - (free_slots + size of next free_slot entry)
        self.layout.page_data_size() - self.row_data_size() - PageDataLayout::FREE_DATA_TUPLE_SIZE * (self.free_slots.len() + 1)
    }

    pub fn insert_row(&mut self, row_bytes: Vec<u8>) -> Result<(), PageError> {
        if row_bytes.len() > self.space_remaining() {
            return Err(PageError::InsertRowError);
        }

        let start_of_data = self.data_offset - row_bytes.len();
        self.data[start_of_data..self.data_offset].copy_from_slice(&row_bytes);
        self.data_offset -= row_bytes.len();
        self.num_rows += 1;

        // reserve free slot for this row
        self.allocate_free_slot(start_of_data);
        
        Ok(())
    }

    fn allocate_free_slot(&mut self, offset: usize) {
        self.free_slots.push((offset, 0));

        let offset_index_len = self.free_slots_offset + PageDataLayout::FREE_DATA_TUPLE_LENGTH_INDEX;
        let length_index_len = offset_index_len + 2;

        println!("offset");
        self.data[self.free_slots_offset..offset_index_len]
            .copy_from_slice(&(offset as i32).to_be_bytes());

        self.data[offset_index_len..length_index_len]
            .copy_from_slice(&(0u16).to_be_bytes());

        self.free_slots_offset += PageDataLayout::FREE_DATA_TUPLE_SIZE;
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.layout.page_size()];
        // Number of rows 2 Bytes
        buf[PageDataLayout::INDEX_NUMBER_ROWS..PageDataLayout::INDEX_ROW_OFFSET].copy_from_slice(&self.num_rows.to_be_bytes());
        // Offset 4 Bytes
        let offset_bytes = (self.data_offset as u32).to_be_bytes();
        buf[PageDataLayout::INDEX_ROW_OFFSET..PageDataLayout::INDEX_PAGE_ID].copy_from_slice(&offset_bytes);
        // PageId 4 Bytes
        let page_id_bytes = self.page_id.to_be_bytes();
        buf[PageDataLayout::INDEX_PAGE_ID..PageDataLayout::INDEX_FREE_SLOTS_OFFSET].copy_from_slice(&page_id_bytes);
        // Free slots offset 4 Bytes
        let free_slots_offset_bytes = (self.free_slots_offset as i32).to_be_bytes();
        buf[PageDataLayout::INDEX_FREE_SLOTS_OFFSET..PageDataLayout::INDEX_FREE_SLOTS_OFFSET + 4].copy_from_slice(&free_slots_offset_bytes);
        
        // Free slots are redundant and live also in the data array, so it's only needed to serialize the whole data array
        buf[10..self.layout.page_size()].copy_from_slice(&self.data);
        buf
    }

    pub fn deserialize(buf: &[u8], layout: &'database PageDataLayout) -> Self {
        let num_rows = u16::from_be_bytes(
            buf[PageDataLayout::INDEX_NUMBER_ROWS..PageDataLayout::INDEX_ROW_OFFSET].try_into().unwrap()
        );

        let offset = i32::from_be_bytes(
            buf[PageDataLayout::INDEX_ROW_OFFSET..PageDataLayout::INDEX_PAGE_ID].try_into().unwrap()
        
        );
        let page_id = i32::from_be_bytes(
            buf[PageDataLayout::INDEX_PAGE_ID..PageDataLayout::INDEX_FREE_SLOTS_OFFSET].try_into().unwrap()
        );

        let free_slots_offset = i32::from_be_bytes(
            buf[PageDataLayout::INDEX_FREE_SLOTS_OFFSET..PageDataLayout::INDEX_FREE_SLOTS_OFFSET + 4].try_into().unwrap()
        ) as usize;

        let data = buf[PageDataLayout::INDEX_FREE_SLOTS_OFFSET + 4..layout.page_size()].to_vec();
        let free_slots: Vec<(usize, u16)> = data[0..free_slots_offset].to_vec()
            .windows(6)
            .map(|window| {
                let offset = i32::from_be_bytes(window[0..4].try_into().unwrap()) as usize;
                let length = u16::from_be_bytes(window[4..6].try_into().unwrap());
                (offset, length)
            }).collect();

        Self {
            layout,
            num_rows,
            data_offset: offset as usize,
            page_id,
            data,
            free_slots,
            free_slots_offset,
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::data::page::{Page, PageDataLayout, PageDataLayoutError};

    #[test]
    fn should_not_allow_page_layout_size_less_than_32() {
        let result = PageDataLayout::new(31);
        assert!(result.is_err());
        matches!(result.err().unwrap(), PageDataLayoutError::InvalidPageSize);
    }

    #[test]
    fn should_allow_page_layout_size_greater_than_31() {
        let result = PageDataLayout::new(32);
        assert!(result.is_ok());
    }

    #[test]
    fn should_calc_all_values_correctly_when_insert_row() {
        let layout = PageDataLayout::new(32).unwrap();
        let mut page = Page::new(&layout);

        // insert 7 bytes
        let row = vec![1, 2, 3, 4, 5, 6, 7];
        page.insert_row(row.clone()).unwrap();

        // 32 - 14(header) = 18
        assert_eq!(page.data.len(), 18);
        assert_eq!(page.free_slots.len(), 1);
        assert_eq!(page.free_slots, vec![(11, 0)]);
        
        let data = page.row_data();
        assert_eq!(data, row);
    }

}