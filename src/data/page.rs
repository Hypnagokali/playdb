use thiserror::Error;

#[derive(Debug)]
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
    const INDEX_FREE_SLOTS_START: usize = 14;

    // table meta data: 4 bytes next_id, 4 bytes number_of_pages
    pub const META_DATA_SIZE: usize = 8;
    // page header: 2 bytes num_rows, 4 bytes data_offset, 4 bytes page_id, 4 bytes slots_offset
    const PAGE_HEADER_SIZE: usize = 14;
    const MIN_PAGE_SIZE: usize = 32; // just arbitrarily chosen value > 10 and good to test :)


    // free data tuple constants
    const SLOT_SIZE: usize = 7; // 4 bytes offset, 2 bytes length, 1 byte deleted flag
    const SLOT_DELETED_INDEX: usize = 0;
    const SLOT_PAGE_OFFSET_INDEX: usize = 1;
    const SLOT_RECORD_LENGTH_INDEX: usize = 5;
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

#[derive(Debug)]
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

#[derive(Debug)]
struct Slot {
    record_length: u16,
    page_offset: usize, // stored as u32
    deleted: bool,
}

#[derive(Debug)]
pub struct Record<'a> {
    record_index: usize,
    data: &'a [u8]
}

impl<'a> Record<'a> {
    pub fn data(&self) -> &'a [u8] {
        self.data
    }
}

pub struct RecordIterator<'a> {
    data: &'a [u8],
    slots: &'a Vec<Slot>,
    slot_index: usize,
}

impl<'a> RecordIterator<'a> {

    pub fn new(page: &'a Page) -> Self {
        Self {
            data: &page.data,
            slots: &page.slots,
            slot_index: 0,
        }
    }
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut found = None;

        while self.slot_index < self.slots.len() && found.is_none() {
            let slot = &self.slots[self.slot_index];

            if !slot.deleted {
                let record = Record {
                    record_index: self.slot_index,
                    data: &self.data[slot.page_offset..slot.page_offset + slot.record_length as usize],
                };

                found = Some(record);
            }

            self.slot_index += 1;
        }

        found
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
#[derive(Debug)]
pub struct Page<'database> {
    data: Vec<u8>,
    slots: Vec<Slot>,
    layout: &'database PageDataLayout,
    // header
    number_of_records: u16,
    // data_offset is actually the free space pointer
    // offset uses usize internally for easier handling (but it's actually an i32) 
    // starts from page_data_size and is heading towards 0
    data_offset: usize,
    // Place where the next free slot can be inserted:
    // stored as i32 (starts from 0)
    slots_offset: usize,
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
            number_of_records: 0,
            page_id: 0,
            slots: Vec::new(),
            slots_offset: 0,
        }
    }

    pub fn record_iterator(&self) -> RecordIterator<'_> {
        RecordIterator::new(self)
    }

    pub fn row_data(&self) -> &[u8] {
        &self.data[self.data_offset..self.layout.page_data_size()]
    }
    pub fn data_offset(&self) -> usize {
        self.data_offset
    }

    pub fn row_data_size(&self) -> usize {
        self.layout.page_data_size() - self.data_offset
    }

    pub fn slot_size(&self) -> usize {
        self.slots.len() * PageDataLayout::SLOT_SIZE
    }

    pub fn num_rows(&self) -> u16 {
        self.number_of_records
    }

    pub fn page_id(&self) -> i32 {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: i32) {
        self.page_id = page_id;
    }

    fn max_fragmented_free_space(&self) -> usize {
        self.slots.iter()
            .filter(|s| s.deleted)
            .map(|s| s.record_length)
            .max()
            .unwrap_or(0) as usize
    }

    fn space_remaining(&self) -> usize {
        std::cmp::max(
            // page_data_size - row_data_size - (free_slots + size of next free_slot entry)
            self.layout.page_data_size() - self.row_data_size() - self.slot_size(),
            self.max_fragmented_free_space())
    }

    pub fn can_insert(&self, row_bytes: &Vec<u8>) -> bool {
        row_bytes.len() <= self.space_remaining() && row_bytes.len() <= PageDataLayout::MAX_ROW_LENGTH as usize
    }

    pub fn insert_record(&mut self, row_bytes: Vec<u8>) -> Result<(), PageError> {
        if !self.can_insert(&row_bytes) {
            return Err(PageError::InsertRowError);
        }

        let new_record_len = row_bytes.len() as u16; // size already checked

        let slot = self.slots.iter_mut()
            .find(|s| s.record_length >= new_record_len && s.deleted);


        let (new_slot_len, slot_data_start_offset) = if let Some(slot) = slot {
            let end_of_data = slot.page_offset + row_bytes.len();
            self.data[slot.page_offset..end_of_data].copy_from_slice(&row_bytes);

            // Need a new slot if the length of the inserted record is not as long as the slot
            let new_slot_len = new_record_len - slot.record_length;

            if new_slot_len == 0 {
                slot.deleted = false;
            } else {
                slot.record_length = new_record_len;
            }

            (new_slot_len, end_of_data)
        } else {
            let start_of_data = self.data_offset - row_bytes.len();
            self.data[start_of_data..self.data_offset].copy_from_slice(&row_bytes);
            self.data_offset -= row_bytes.len();

            (row_bytes.len() as u16, start_of_data)
        };

        if new_slot_len > 0 {
            self.allocate_slot(slot_data_start_offset, new_record_len);
        }
    
        self.number_of_records += 1;
        
        Ok(())
    }

    fn allocate_slot(&mut self, page_offset: usize, record_length: u16) {
        self.slots.push(Slot { record_length, page_offset, deleted: false });

        // let offset_index = self.slots_offset + 1; // after the delete flag index
        // let length_index = self.slots_offset + PageDataLayout::SLOT_RECORD_LENGTH_INDEX;
        // let length_end = length_index + 2;

        // // deleted flag
        // self.data[self.slots_offset] = 0;
        // // offset
        // self.data[offset_index..length_index]
        //     .copy_from_slice(&(page_offset as u32).to_be_bytes());
        // // length
        // self.data[length_index..length_end]
        //     .copy_from_slice(&(0u16).to_be_bytes());

        // self.slots_offset += PageDataLayout::SLOT_SIZE;
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.layout.page_size()];
        // Number of rows 2 Bytes
        buf[PageDataLayout::INDEX_NUMBER_ROWS..PageDataLayout::INDEX_ROW_OFFSET]
            .copy_from_slice(&self.number_of_records.to_be_bytes());
        
        // Offset 4 Bytes
        let offset_bytes = (self.data_offset as u32).to_be_bytes();
        buf[PageDataLayout::INDEX_ROW_OFFSET..PageDataLayout::INDEX_PAGE_ID]
            .copy_from_slice(&offset_bytes);

        // PageId 4 Bytes
        let page_id_bytes = self.page_id.to_be_bytes();
        buf[PageDataLayout::INDEX_PAGE_ID..PageDataLayout::INDEX_FREE_SLOTS_OFFSET]
            .copy_from_slice(&page_id_bytes);

        // Free slots offset 4 Bytes
        let free_slots_offset_bytes = ((self.slots.len() * PageDataLayout::SLOT_SIZE) as i32).to_be_bytes();
        buf[PageDataLayout::INDEX_FREE_SLOTS_OFFSET..PageDataLayout::INDEX_FREE_SLOTS_OFFSET + 4]
            .copy_from_slice(&free_slots_offset_bytes);

        buf[PageDataLayout::INDEX_FREE_SLOTS_OFFSET + 4..self.layout.page_size()].copy_from_slice(&self.data);

        // serialize Slots:
        for (i, slot) in self.slots.iter().enumerate() {
            let next_free_slots_start = PageDataLayout::INDEX_FREE_SLOTS_START + (i * PageDataLayout::SLOT_SIZE);
            let offset_of_data_index = next_free_slots_start + 1;
            let length_index = next_free_slots_start + PageDataLayout::SLOT_RECORD_LENGTH_INDEX;
            let length_end = length_index + 2;
            
            // deleted flag
            buf[next_free_slots_start] = 0;
            // offset
            buf[offset_of_data_index..length_index]
                .copy_from_slice(&(slot.page_offset as u32).to_be_bytes());
            // length
            buf[length_index..length_end]
                .copy_from_slice(&(slot.record_length).to_be_bytes());
        }

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

        let free_slots: Vec<Slot> = data[0..free_slots_offset].to_vec()
            .chunks_exact(7)
            .map(|chunk| {
                let deleted = if chunk[0] == 1 { true } else { false };
                let offset = u32::from_be_bytes(chunk[1..5].try_into().unwrap()) as usize;
                let length = u16::from_be_bytes(chunk[5..7].try_into().unwrap());
                let slot = Slot {
                    page_offset: offset,
                    record_length: length,
                    deleted,
                };
                slot
            }).collect();

        Self {
            layout,
            number_of_records: num_rows,
            data_offset: offset as usize,
            page_id,
            data,
            slots: free_slots,
            slots_offset: free_slots_offset,
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
        page.insert_record(row.clone()).unwrap();

        // 32 - 14(header) = 18
        assert_eq!(page.data.len(), 18);
        assert_eq!(page.slots.len(), 1);
        //points to offset 11, but no free space
        
        let slot_option = page.slots.get(0);
        assert!(slot_option.is_some());
        let slot = slot_option.unwrap();
        assert_eq!(slot.deleted, false);
        assert_eq!(slot.record_length, 7);
        assert_eq!(slot.page_offset, 11);

        assert_eq!(page.row_data_size(), 7);
        let data = page.row_data();
        assert_eq!(data, row);
    }

    #[test]
    fn should_serialize_and_deserialize_correctly() {
        let layout = PageDataLayout::new(32).unwrap();
        let mut page = Page::new(&layout);
        page.set_page_id(1);

        // insert 7 bytes
        let row = vec![1, 2, 3, 4, 5, 6, 7];
        page.insert_record(row.clone()).unwrap();

        // act: serialize
        let bytes = page.serialize();

        let deserialized_page = Page::deserialize(&bytes, &layout);

        assert_eq!(deserialized_page.page_id, 1);
        // 32 - 14(header) = 18
        assert_eq!(deserialized_page.data.len(), 18);
        assert_eq!(deserialized_page.slots.len(), 1);
        // Slot points to offset 11 and it isn't deleted
        let slot_option = deserialized_page.slots.get(0);
        assert!(slot_option.is_some());
        let slot = slot_option.unwrap();
        assert_eq!(slot.deleted, false);
        assert_eq!(slot.record_length, 7);
        assert_eq!(slot.page_offset, 11);

        assert_eq!(deserialized_page.row_data_size(), 7);
        let data = deserialized_page.row_data();
        assert_eq!(data, row);
    }

    #[test]
    fn should_serialize_and_deserialize_correctly_multiple_inserts() {
        let layout = PageDataLayout::new(64).unwrap();
        let mut page = Page::new(&layout);
        page.set_page_id(1);

        page.insert_record(vec![1, 2, 3]).unwrap();
        page.insert_record(vec![4, 5, 6]).unwrap();
        page.insert_record(vec![7, 8, 9]).unwrap();

        // act: serialize + deserialize
        let bytes = page.serialize();
        let deserialized_page = Page::deserialize(&bytes, &layout);

        assert_eq!(deserialized_page.page_id, 1);
        // 64 - 14(header) = 50
        assert_eq!(deserialized_page.data.len(), 50);
        assert_eq!(deserialized_page.slots.len(), 3);

        let slot_option = page.slots.get(2);
        assert!(slot_option.is_some());
        let slot = slot_option.unwrap();
        assert_eq!(slot.deleted, false);
        assert_eq!(slot.record_length, 3);
        // 50 - 9 = 41
        assert_eq!(slot.page_offset, 41);

        assert_eq!(deserialized_page.row_data_size(), 9);
    }

    #[test]
    fn data_iterator_should_return_none_if_no_data_present() {
        let layout = PageDataLayout::new(32).unwrap();
        let page = Page::new(&layout);
        let record_data = page.record_iterator().next();
              
        assert!(record_data.is_none());
    }

    #[test]
    fn data_iterator_should_iterate_over_record_data() {
        let layout = PageDataLayout::new(32).unwrap();
        let mut page = Page::new(&layout);
        page.insert_record(vec![1, 2, 3]).unwrap();
        page.insert_record(vec![4, 5, 6]).unwrap();

        let mut rec_iterator = page.record_iterator();
        let mut record_data = rec_iterator.next();        
        assert!(matches!(record_data, Some(record) if record.data() == &[1, 2, 3]));

        record_data = rec_iterator.next();
        assert!(matches!(record_data, Some(record) if record.data() == &[4, 5, 6]));
        
        record_data = rec_iterator.next();
        assert!(record_data.is_none());
    }

    #[test]
    fn data_iterator_should_not_return_deleted_data() {
        let layout = PageDataLayout::new(32).unwrap();
        let mut page = Page::new(&layout);
        page.insert_record(vec![1, 2, 3]).unwrap();
        page.insert_record(vec![4, 5, 6]).unwrap();
        page.slots[0].deleted = true;

        let mut rec_iterator = page.record_iterator();
        let mut record_data = rec_iterator.next();              
        assert!(matches!(record_data, Some(record) if record.data() == &[4, 5, 6]));

        record_data = rec_iterator.next();              
        assert!(record_data.is_none());
    }

}