use std::{
    cmp::min,
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::{
    disk_manager::DiskManager,
    page::{Page, PageId, PAGE_SIZE_BYTES},
    usage_tracker::UsageTracker,
};

pub type PagePointer = Arc<RwLock<Page>>;

pub struct PageManager {
    disk_manager: DiskManager,
    usage_tracker: UsageTracker,
    pages: BTreeMap<PageId, PagePointer>,
    empty_pages: Vec<PageId>, // List of pages allocated by the disk manager which are empty. Not necessarily memory in the page manager
    max_num_pages: usize,
}

impl PageManager {
    pub fn new(max_num_pages: usize, base_directory: &str) -> PageManager {
        PageManager {
            disk_manager: DiskManager::new(base_directory),
            usage_tracker: UsageTracker::new(),
            pages: BTreeMap::new(),
            empty_pages: vec![],
            max_num_pages,
        }
    }

    pub fn add_empty_pages(&mut self, file: &str, n_pages: usize) {
        let empty_pages = self.disk_manager.allocate_pages(n_pages, file).unwrap();

        let buffer_spots = self.max_num_pages - self.pages.len();
        let len = min(buffer_spots, n_pages);

        for (i, id) in empty_pages.iter().enumerate() {
            if i < len {
                self.add_free_page(*id);
            } else {
                self.empty_pages.push(*id);
            }
        }
    }

    fn add_free_page(&mut self, page_id: PageId) {
        // Arena allocate eventually
        let page = Arc::new(RwLock::new(Page {
            page_id: page_id,
            data: vec![0; PAGE_SIZE_BYTES as usize],
            is_dirty: false,
        }));
        self.pages.insert(page_id, page);
        self.usage_tracker.insert(page_id);

        self.empty_pages.push(page_id);
    }

    pub fn next_free_page(&mut self) -> PagePointer {
        if self.empty_pages.len() == 0 {
            panic!("No empty pages left"); // out of memory
        }

        // Future optimization: try to find one that's in memory already
        let page_id = self.empty_pages.pop().unwrap();

        let page = self.find_page(page_id);

        page
    }

    fn evict_next_page(&mut self) -> Option<()> {
        let mut page_to_evict = None;

        for (page_id, _) in self.usage_tracker.last_used.iter() {
            let page = self.pages.get(page_id).unwrap();

            // If there's only one reference then it must not be in use by any clients.
            // Note this only work because we've already locked the page_manager
            // Consider making your own class that does this automatically.
            if Arc::strong_count(page) == 1 {
                page_to_evict = Some(*page_id);
                break;
            }
        }

        if let Some(page_id) = page_to_evict {
            let page = self.pages.remove(&page_id).unwrap();
            self.usage_tracker.last_used.remove(&page_id);
            let page_inner = page.write().unwrap();

            self.disk_manager
                .save_page(page_id, &page_inner.data)
                .unwrap();

            Some(())
        } else {
            None
        }
    }

    fn load_page(&mut self, page_id: PageId) -> PagePointer {
        if self.pages.len() == self.max_num_pages {
            self.evict_next_page().expect("All pages are in use");
        }

        let data = self.disk_manager.load_page(page_id).unwrap();

        let page = Arc::new(RwLock::new(Page {
            page_id,
            data,
            is_dirty: false,
        }));

        self.pages.insert(page_id, page.clone());
        self.usage_tracker.insert(page_id);

        page
    }

    pub fn find_page(&mut self, page_id: PageId) -> PagePointer {
        if let Some(page) = self.pages.get(&page_id) {
            self.usage_tracker.touch(page_id);
            page.clone()
        } else {
            self.load_page(page_id)
        }
    }
}

#[cfg(test)]
mod page_manager_tests {
    use std::{
        fs::{create_dir_all, remove_dir_all},
        path::Path,
    };

    use crate::page::PAGE_SIZE_BYTES;

    use super::PageManager;

    fn setup_test_dir(base_dir: &str) {
        let path = Path::new(base_dir);
        create_dir_all(path).expect("Failed to create test directory.");
    }

    fn cleanup(base_dir: &str) {
        let _ = remove_dir_all(base_dir);
    }

    #[test]
    pub fn allocate_empty_pages() {
        let base_dir = "./test1";
        setup_test_dir(base_dir);

        let mut manager = PageManager::new(50, base_dir);

        manager.add_empty_pages("empty.db", 100);

        assert_eq!(manager.pages.len(), 50);
        assert_eq!(manager.usage_tracker.last_used.len(), 50);

        cleanup(base_dir);
    }

    #[test]
    pub fn write_free_page_persisted() {
        let base_dir = "./test2";
        setup_test_dir(base_dir);

        let mut manager = PageManager::new(1, base_dir);
        manager.add_empty_pages("empty.db", 2);

        let page_id_1 = {
            let page = manager.next_free_page();
            let mut page = page.write().expect("Failed to unlock mutex");
            page.data.fill(88);
            page.page_id
        };

        let page_id_2 = {
            let page = manager.next_free_page();
            let mut page = page.write().expect("Failed to unlock mutex");
            page.data.fill(77);
            page.page_id
        };

        assert_eq!(manager.pages.len(), 1);
        assert_eq!(manager.usage_tracker.last_used.len(), 1);

        {
            let page = manager.find_page(page_id_1);
            let page = page.read().expect("Failed to unlock mutex");

            assert_eq!(page.data.len() as u64, PAGE_SIZE_BYTES);
            for b in page.data.iter() {
                assert_eq!(*b, 88);
            }
        }

        assert_eq!(manager.pages.len(), 1);
        assert_eq!(manager.usage_tracker.last_used.len(), 1);

        {
            let page = manager.find_page(page_id_2);
            let page = page.read().expect("Failed to unlock mutex");

            assert_eq!(page.data.len() as u64, PAGE_SIZE_BYTES);
            for b in page.data.iter() {
                assert_eq!(*b, 77);
            }
        }

        cleanup(base_dir);
    }

    #[test]
    fn evict_lru_page() {
        let base_dir = "./test2";
        setup_test_dir(base_dir);

        let mut manager = PageManager::new(2, base_dir);
        manager.add_empty_pages("empty.db", 3);

        let page_id_1 = {
            let page = manager.next_free_page();
            let page = page.read().unwrap();
            page.page_id
        };

        let page_id_2 = {
            let page = manager.next_free_page();
            let page = page.read().unwrap();
            page.page_id
        };

        let page_id_3 = {
            let page = manager.next_free_page();
            let page = page.read().unwrap();
            page.page_id
        };

        assert_eq!(manager.empty_pages.len(), 0);
        assert_eq!(manager.pages.len(), 2);

        // Ensure that pages 1 and 2 are the most recently used
        {
            let _page_1 = manager.find_page(page_id_1);
            let _page_2 = manager.find_page(page_id_2);
        }

        let (id, _) = manager.usage_tracker.last_used.peek().unwrap();
        assert_eq!(*id, page_id_1);

        {
            let _page = manager.find_page(page_id_3);
        }

        let (id, _) = manager.usage_tracker.last_used.peek().unwrap();
        assert_eq!(*id, page_id_2);

        cleanup(base_dir);
    }

    #[test]
    pub fn page_with_reference_is_not_evicted() {
        let base_dir = "./test3";
        setup_test_dir(base_dir);

        let mut manager = PageManager::new(2, base_dir);
        manager.add_empty_pages("empty.db", 3);

        let page_id_1 = {
            let page = manager.next_free_page();
            let page = page.read().unwrap();
            page.page_id
        };

        let page_id_2 = {
            let page = manager.next_free_page();
            let page = page.read().unwrap();
            page.page_id
        };

        let page_id_3 = {
            let page = manager.next_free_page();
            let page = page.read().unwrap();
            page.page_id
        };

        // Hold on to the reference to page_1
        let _page_1 = manager.find_page(page_id_1);
        {
            let _page_2 = manager.find_page(page_id_2);
        }

        let (id, _) = manager.usage_tracker.last_used.peek().unwrap();
        assert_eq!(*id, page_id_1);

        {
            let _page = manager.find_page(page_id_3);
        }

        let (id, _) = manager.usage_tracker.last_used.peek().unwrap();
        assert_eq!(*id, page_id_1);

        cleanup(base_dir);
    }
}
