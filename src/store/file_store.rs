use std::{io::{Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}};

use crate::{data::page::{Page, PageDataLayout, PageFileMetadata}, store::{Store, StoreError}, table::table::Table};

pub struct FileStore<'a> {
    base_path: &'a Path,
}
impl<'a> FileStore<'a> {
    pub fn new(base_path: &'a Path) -> Self {
        if !base_path.is_dir() {
            // TODO: Use proper error handling
            panic!("FileStore needs a directory as a base_path");
        }
        Self { 
            base_path,
         }
    }

    fn file_path(&self, table: &Table) -> PathBuf {
        self.base_path.join(table.file_path())
    }

    fn init(&self, layout: &PageDataLayout, table: &Table) -> Result<(), StoreError> {
        let metadata = PageFileMetadata::new();
        self.write_metadata(layout, &metadata, table)
    }

    fn write_metadata(&self, layout: &PageDataLayout, metadata: &PageFileMetadata, table: &Table) -> Result<(), StoreError> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.base_path.join(table.file_path()))?;

        file.write_all(&metadata.serialize(layout))?;

        Ok(())
    }
}
impl<'a> Store for FileStore<'a> {
    fn read_metadata(&self, layout: &PageDataLayout, table: &Table) -> Result<PageFileMetadata, StoreError> {
        let path: PathBuf = self.file_path(table);
        if !path.exists() {
            self.init(layout, table)?;
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)?;

        let fmeta = file.metadata().unwrap();
        if fmeta.len() < layout.metadata_size() as u64 {
            return Err(StoreError::IoError("Metadata size is smaller than expected".to_string()));
        }

        let mut buf = vec![0u8; layout.metadata_size()];
        file.read_exact(&mut buf)?;

        Ok(PageFileMetadata::deserialize(&buf))
    }

    fn read_page<'database>(&self, layout: &'database PageDataLayout, page_id: i32, table: &Table) -> Result<Page<'database>, StoreError> {
        let mut page_data = vec![0; layout.page_size()];

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.base_path.join(table.file_path()))?;

        let page_pos = page_id - 1;
        file.seek(SeekFrom::Start((layout.metadata_size() + page_pos as usize * layout.page_size()) as u64))?;
    
        file.read_exact(&mut page_data)?;

        let p = Page::deserialize(&page_data, layout);
        Ok(p)
    }

    fn write_page(&self, layout: &PageDataLayout, page: &Page, table: &Table) -> Result<(), StoreError> {
        let data = page.serialize();

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.base_path.join(table.file_path()))?;
        let page_pos = page.page_id() - 1;
        file.seek(SeekFrom::Start((layout.metadata_size() + page_pos as usize * layout.page_size()) as u64))?;
        file.write_all(&data)?;
        Ok(())
    }
    
    fn allocate_page<'database>(&self, layout: &'database PageDataLayout, table: &Table) -> Result<Page<'database>, StoreError> {
        let mut metadata = self.read_metadata(layout, table)?;
        let mut new_page = Page::new(layout);
        new_page.set_page_id(metadata.allocate_next_page_id());

        // ToDo: here we can get into an inconsistent state if write_page fails after write_metadata succeeded
        self.write_metadata(layout, &metadata, table)?;
        self.write_page(layout, &new_page, table)?;
        Ok(new_page)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::{data::page::PageDataLayout, store::{PageIterator, Store, file_store::FileStore}, table::{Column, ColumnType, TableSchema, table::{Cell, Row, Table}}};

    struct Sequence {
            col_id: i32,
            current: i32,
    }

    impl Sequence {
        fn serialize(&self) -> Vec<u8> {
            let mut buf = [0; 8];
            buf[0..4].copy_from_slice(&self.col_id.to_be_bytes());
            buf[4..8].copy_from_slice(&self.current.to_be_bytes());
            buf.to_vec()
        }

        fn deserialize(data: &[u8]) -> Self {
            let col_id = i32::from_be_bytes(data[0..4].try_into().unwrap());
            let current = i32::from_be_bytes(data[4..8].try_into().unwrap());

            Self {
                col_id,
                current,
            }
        }
    }

    #[test]
    fn should_allocate_and_write_page() {
        let dir = tempdir().unwrap();
        let store = FileStore::new(dir.path());

        let layout = PageDataLayout::new(128).unwrap();

        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int)
        ]);

        let table = Table::new(1, "test".to_owned(), schema);

        let mut new_page = store.allocate_page(&layout, &table).unwrap();
        assert_eq!(new_page.page_id(), 1);

        let row = Row::new(vec![
            Cell::Int(42)
        ]);

        new_page.insert_record(row.serialize()).unwrap();

        store.write_page(&layout, &new_page, &table).unwrap();

        let loaded_page = store.read_page(&layout, 1, &table).unwrap();

        let row = Row::deserialize(loaded_page.row_data(), table.schema());

        assert_eq!(row.cells().len(), 1);
        matches!(row.cells().get(0).unwrap(), Cell::Int(42));
    }

    #[test]
    fn should_can_allocate_twice() {
        let dir = tempdir().unwrap();
        let store = FileStore::new(dir.path());

        let layout = PageDataLayout::new(128).unwrap();

        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int)
        ]);

        let table = Table::new(1, "test".to_owned(), schema);

        // Create first page (stays empty)
        let first_page = store.allocate_page(&layout, &table).unwrap();
        assert_eq!(first_page.page_id(), 1);

        store.write_page(&layout, &first_page, &table).unwrap();

        // Create second page with a row
        let mut second_page = store.allocate_page(&layout, &table).unwrap();
        assert_eq!(second_page.page_id(), 2);

        let row = Row::new(vec![
            Cell::Int(42)
        ]);

        second_page.insert_record(row.serialize()).unwrap();
        store.write_page(&layout, &second_page, &table).unwrap();
        let loaded_page = store.read_page(&layout, 2, &table).unwrap();

        let row = Row::deserialize(loaded_page.row_data(), table.schema());

        assert_eq!(row.cells().len(), 1);
        matches!(row.cells().get(0).unwrap(), Cell::Int(42));
    }

    #[test]
    fn should_be_able_to_store_arbitrary_data() {
        let dir = tempdir().unwrap();
        let store = FileStore::new(dir.path());

        let layout = PageDataLayout::new(128).unwrap();

        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int)
        ]);
        let table = Table::new(1, "test".to_owned(), schema);

        // Create page for sequences
        let mut seq_page = store.allocate_page(&layout, &table).unwrap();

        let seq = Sequence {
            col_id: 1,
            current: 3,
        };

        seq_page.insert_record(seq.serialize()).unwrap();
        store.write_page(&layout, &seq_page, &table).unwrap();

        let loaded_page = store.read_page(&layout, 1, &table).unwrap();

        let seq_loaded: Sequence = Sequence::deserialize(loaded_page.row_data());

        assert_eq!(seq_loaded.col_id, 1);
        assert_eq!(seq_loaded.current, 3);
    }

    #[test]
    fn should_iterate_over_pages() {
        let dir = tempdir().unwrap();
        let store = FileStore::new(dir.path());

        let layout = PageDataLayout::new(32).unwrap();

        let schema = TableSchema::new(vec![
            Column::new(1, "id", ColumnType::Int)
        ]);

        let table = Table::new(1, "test".to_owned(), schema);

        let mut new_page = store.allocate_page(&layout, &table).unwrap();
        assert_eq!(new_page.page_id(), 1);

        let row = Row::new(vec![
            Cell::Int(42)
        ]);

        new_page.insert_record(row.serialize()).unwrap();
        store.write_page(&layout, &new_page, &table).unwrap();

        let mut iter = PageIterator::new(&table, &store, &layout);

        let page = iter.next().unwrap();

        assert_eq!(page.page_id(), 1);
        matches!(page.data_offset(), 28);
    }
}

