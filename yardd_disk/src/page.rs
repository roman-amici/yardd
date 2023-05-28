pub const PAGE_HEADER_SIZE: u64 = 8;
pub const PAGE_SIZE_BYTES: u64 = 1024;

pub type PageId = u64;

pub struct Page {
    pub page_id: PageId,
    pub data: Vec<u8>,
    pub is_dirty: bool,
}
