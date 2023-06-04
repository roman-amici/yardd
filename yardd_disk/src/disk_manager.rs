use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::page::{PageId, PAGE_SIZE_BYTES};

struct DiskEntry {
    pub file_path: PathBuf, // Relative path to the base directory
    pub offset: u64,
    pub page_id: PageId,
}

pub struct DiskManager {
    page_map: HashMap<PageId, DiskEntry>,
    base_directory: PathBuf, // and maybe file handles...
    next_page_id: PageId,
}

impl DiskManager {
    pub fn new(base_directory: &str) -> Self {
        DiskManager {
            page_map: HashMap::new(),
            base_directory: PathBuf::from(base_directory),
            next_page_id: 0,
        }
    }

    fn next_page_id(&mut self) -> u64 {
        let next = self.next_page_id;
        self.next_page_id += 1;
        next
    }

    pub fn allocate_pages(
        &mut self,
        pages: usize,
        file_name: &str,
    ) -> Result<Vec<PageId>, Box<dyn Error>> {
        let path = self.base_directory.join(Path::new(file_name));
        let mut file = File::create(&path)?;

        // Create a file with the size to fill all the pages
        file.seek(SeekFrom::Start((pages * PAGE_SIZE_BYTES as usize) as u64))?;
        file.write(&[0])?;

        let mut page_ids = vec![];
        for i in 0..pages {
            let entry = DiskEntry {
                file_path: path.clone(),
                offset: (i as u16 * PAGE_SIZE_BYTES) as u64,
                page_id: self.next_page_id(),
            };

            page_ids.push(entry.page_id);

            self.page_map.insert(entry.page_id, entry);
        }

        Ok(page_ids)
    }

    pub fn load_page(&mut self, page_id: PageId) -> Result<Vec<u8>, Box<dyn Error>> {
        let page_entry = self
            .page_map
            .get(&page_id)
            .expect("Attempt to load page with unknown id");

        let mut file = File::open(&page_entry.file_path)?;

        file.seek(SeekFrom::Start(page_entry.offset))?;
        let mut buffer: Vec<u8> = vec![0; PAGE_SIZE_BYTES as usize];
        file.read_exact(&mut buffer);

        Ok(buffer)
    }

    pub fn save_page(&mut self, page_id: PageId, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let page_entry = self
            .page_map
            .get(&page_id)
            .expect("Attempt to save a page with unknown id");

        let mut file = File::options()
            .write(true)
            .open(&page_entry.file_path)
            .expect("Failed to open file.");

        file.seek(SeekFrom::Start(page_entry.offset))?;
        file.write_all(data)?;

        Ok(())
    }
}
