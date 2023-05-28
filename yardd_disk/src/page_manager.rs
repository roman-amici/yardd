use std::{
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
    pub fn new() -> PageManager {
        let mut manager = PageManager {
            disk_manager: DiskManager::new("./"),
            usage_tracker: UsageTracker::new(),
            pages: BTreeMap::new(),
            empty_pages: vec![],
            max_num_pages: 50,
        };

        let empty_pages = manager.disk_manager.allocate_pages(100, "db.yard").unwrap();

        let len = manager.empty_pages.len() / 2;

        for id in empty_pages.iter().rev().take(len) {
            manager.add_free_page(*id);
        }

        manager.empty_pages = empty_pages;

        manager
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
    }

    pub fn next_free_page(&mut self) -> PagePointer {
        if (self.empty_pages.len() == 0) {
            panic!("No empty pages left"); // out of memory
        }

        // Future optimization: try to find one that's in memory already
        let page_id = self.empty_pages.pop().unwrap();

        let page = self.find_page(page_id);

        self.usage_tracker.touch(page_id);

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
