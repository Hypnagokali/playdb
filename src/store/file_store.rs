use std::{io::{Read, Seek, SeekFrom, Write}, path::Path};

use crate::{data::page::{Page, PageDataLayout, PageFileMetadata}, store::{PageIterator, Store, StoreError}, table::table::Table};

pub struct FileStore<'a> {
    base_path: &'a Path,
}
impl<'a> FileStore<'a> {
    pub fn new(base_path: &'a Path) -> Self {
        Self { 
            base_path,
         }
    }
}
impl<'a> Store for FileStore<'a> {
    fn read_metadata(&self, layout: &PageDataLayout, table: &Table) -> Result<PageFileMetadata, StoreError> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.base_path.join(table.file_path()))?;

        let fmeta = file.metadata().unwrap();
        if fmeta.len() < layout.metadata_size() as u64 {
            return Err(StoreError::IoError("Metadata size is smaller than expected".to_string()));
        }

        let mut buf = vec![0u8; layout.metadata_size()];
        file.read_exact(&mut buf)?;

        Ok(PageFileMetadata::deserialize(&buf))
    }

    fn write_metadata(&self, layout: &PageDataLayout, metadata: &PageFileMetadata, table: &Table) -> Result<(), StoreError> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.base_path.join(table.file_path()))?;

        file.write_all(&metadata.serialize(layout))?;

        Ok(())
    }

    fn read_page<'database>(&self, layout: &'database PageDataLayout, page_id: i32, table: &Table) -> Result<Page<'database>, StoreError> {
        let mut data = vec![0; layout.max_data_size()];

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.base_path.join(table.file_path()))?;

        file.seek(SeekFrom::Start((layout.metadata_size() + page_id as usize * layout.page_size()) as u64))?;
    
        file.read_exact(&mut data)?;

        let p = Page::deserialize(&data, layout);
        println!("Page loaded: number {}, rows {}, offset {}", p.page_id(), p.num_rows(), p.offset());
        Ok(p)
    }

    fn write_page(&self, layout: &PageDataLayout, page: &Page, table: &Table) -> Result<(), StoreError> {
        let data = page.serialize();

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.base_path.join(table.file_path()))?;
        file.seek(SeekFrom::Start((layout.metadata_size() + page.page_id() as usize * layout.page_size()) as u64))?;
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
    
    fn page_iterator<'database>(&'database self, layout: &'database PageDataLayout, table: &'database crate::table::table::Table) -> Result<PageIterator<'database, Self>, StoreError> {
        Ok(PageIterator::new(table, self, layout))
    }
}
