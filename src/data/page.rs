use thiserror::Error;

pub struct PageDataLayout {
    page_size: usize,
}

impl PageDataLayout {
    const INDEX_NUMBER_ROWS: usize = 0;
    const INDEX_ROW_OFFSET: usize = 2;
    const INDEX_PAGE_ID: usize = 6;
    const INDEX_FREE_SLOTS_OFFSET: usize = 10;

    pub const META_DATA_SIZE: usize = 8;    // 4 bytes next_id, 4 bytes number_of_pages

    // 2 bytes num_rows, 4 bytes offset, 4 bytes page_id
    const PAGE_HEADER_SIZE: usize = 10;
    const FREE_DATA_TUPLE_SIZE: usize = 6; // 4 bytes offset, 2 bytes length
    const FREE_DATA_TUPLE_OFFSET_INDEX: usize = 0;
    const FREE_DATA_TUPLE_LENGTH_INDEX: usize = 4;
    const MAX_ROW_LENGTH: u16 = u16::MAX;

    pub fn new(page_size: usize) -> Self {
        Self { page_size }
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn max_data_size(&self) -> usize {
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
    data_offset: usize, // usize used internally for easier handling (but it's actually an i32) goes from page_size to 0
    free_slots_offset: usize, // stored as i32 (goes from 0 to page_size)
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
            data: vec![0; layout.max_data_size()],
            data_offset: layout.page_size() - 1,
            num_rows: 0,
            page_id: 0,
            free_slots: Vec::new(),
            free_slots_offset: 0,
        }
    }

    pub fn row_data(&self) -> &[u8] {
        &self.data[self.data_offset..self.layout.page_size()]
    }
    pub fn offset(&self) -> usize {
        self.data_offset
    }

    pub fn page_number(&self) -> i32 {
        self.page_id
    }

    pub fn row_data_length(&self) -> usize {
        self.layout.page_size() - self.data_offset
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
        // page_size - row_data - (free_slots + size of next free_slot entry)
        self.layout.page_size - (self.layout.page_size() - self.data_offset) - PageDataLayout::FREE_DATA_TUPLE_SIZE * (self.free_slots.len() + 1)
    }

    pub fn insert_row(&mut self, row_bytes: Vec<u8>) -> Result<(), PageError> {
        if self.data_offset + row_bytes.len() > self.layout.max_data_size() {
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

        let offset_index = self.free_slots_offset + PageDataLayout::FREE_DATA_TUPLE_OFFSET_INDEX;
        let length_index = offset_index + PageDataLayout::FREE_DATA_TUPLE_LENGTH_INDEX;

        self.data[self.free_slots_offset..offset_index]
            .copy_from_slice(&(offset as i32).to_be_bytes());

        self.data[offset_index..length_index]
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
        let free_slots_offset_bytes = (self.free_slots_offset as i32).to_be_bytes();
        buf[PageDataLayout::INDEX_FREE_SLOTS_OFFSET..PageDataLayout::INDEX_FREE_SLOTS_OFFSET + 4].copy_from_slice(&free_slots_offset_bytes);
        
        // Free slots are only deserialized and are directly inserted into the data array when a row is deleted
        // or removed /updated if a new row is inserted at that position
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

        let data = buf[PageDataLayout::INDEX_PAGE_ID + 4..layout.page_size()].to_vec();
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
