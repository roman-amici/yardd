use crate::{disk_btree::IndexPage, serialization_helpers::*};
use std::mem::size_of;

pub const PAGE_SIZE_BYTES: u16 = 1024;
pub const PAGE_MAGIC_NUMBER: u32 = 0xFBEA82B9;

pub type PageId = u64;
pub type SlotIndex = u16;

pub struct Page {
    pub page_id: PageId,
    pub data: Vec<u8>,
    pub is_dirty: bool,
}

#[derive(Debug, PartialEq)]
pub enum PageType {
    IndexNode = 1,
    IndexLeaf = 2,
    DataPage = 3,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            1 => PageType::IndexNode,
            2 => PageType::IndexLeaf,
            3 => PageType::DataPage,
            _ => panic!("Unknown page type"),
        }
    }
}

pub const MAGIC_NUMBER_START: usize = 0;
pub const PAGE_TYPE_START: usize = MAGIC_NUMBER_START + size_of::<u32>();
pub const LOG_SEQUENCE_NUMBER_START: usize = PAGE_TYPE_START + size_of::<u8>();
pub const PARENT_PAGE_ID_START: usize = LOG_SEQUENCE_NUMBER_START + size_of::<u32>();
pub const PAGE_ID_START: usize = PARENT_PAGE_ID_START + size_of::<PageId>();
pub const BYTES_REMAINING_START: usize = PAGE_ID_START + size_of::<PageId>();
pub const HEADER_SIZE: usize = PAGE_ID_START + size_of::<u16>();

pub const SLOTS_HEADER_START: usize = HEADER_SIZE;
pub const SLOTS_OCCUPIED_SLOTS_START: usize = SLOTS_HEADER_START;
pub const SLOTS_FRAGMENTED_SLOTS_START: usize = SLOTS_OCCUPIED_SLOTS_START + size_of::<u16>();
pub const SLOTS_NEXT_EMPTY_OFFSET_START: usize = SLOTS_FRAGMENTED_SLOTS_START + size_of::<u16>();
pub const SLOTS_HEADER_SIZE: usize = size_of::<u16>() * 3;
pub const SLOTS_START: usize = SLOTS_HEADER_START + SLOTS_HEADER_SIZE;

pub struct SlotHeader {
    pub occupied_slots: u16,
    pub fragmented_slots: u16,
    pub next_empty_offset: u16,
}

pub struct PageHeader {
    pub magic_number: u32,
    pub page_type: PageType,
    pub log_sequence_number: u32,
    pub parent_page_id: PageId,
    pub page_id: PageId,
    pub bytes_remaining: u16,
}

impl Page {
    pub fn read_header(&self) -> PageHeader {
        PageHeader {
            magic_number: read_u32(&self.data, MAGIC_NUMBER_START),
            page_type: self.read_page_type(),
            log_sequence_number: read_u32(&self.data, LOG_SEQUENCE_NUMBER_START),
            parent_page_id: read_u64(&self.data, PARENT_PAGE_ID_START),
            page_id: self.read_page_id(),
            bytes_remaining: self.read_bytes_remaining(),
        }
    }

    pub fn write_header(&mut self, header: PageHeader) {
        self.page_id = header.page_id;

        write_u32(&mut self.data, MAGIC_NUMBER_START, header.magic_number);
        self.data[4] = header.page_type as u8;
        write_u32(
            &mut self.data,
            LOG_SEQUENCE_NUMBER_START,
            header.log_sequence_number,
        );
        write_u64(&mut self.data, PARENT_PAGE_ID_START, header.parent_page_id);
        write_u64(&mut self.data, PAGE_ID_START, header.page_id);
        write_u16(
            &mut self.data,
            BYTES_REMAINING_START,
            header.bytes_remaining,
        );
    }

    pub fn read_page_id(&self) -> PageId {
        read_u64(&self.data, PAGE_ID_START)
    }

    pub fn read_page_type(&self) -> PageType {
        self.data[PAGE_TYPE_START].into()
    }

    pub fn read_bytes_remaining(&self) -> u16 {
        read_u16(&self.data, BYTES_REMAINING_START)
    }

    pub fn as_index_node<'a, KeyType>(&'a self) -> IndexPage<'a, KeyType>
    where
        KeyType: DbColumn,
    {
        let page_type = self.read_page_type();
        if page_type != PageType::IndexNode && page_type != PageType::IndexLeaf {
            panic!("Can't read page as index page. Type = {:?}", page_type)
        }

        IndexPage::read_existing_page(self)
    }

    pub fn get_slot_offset(&self, slot_index: SlotIndex) -> usize {
        let start = SLOTS_START + std::mem::size_of::<i16>() * slot_index as usize;
        read_u16(&self.data, start) as usize
    }
}

pub trait DbColumn
where
    Self: PartialEq + PartialOrd,
{
    fn from_bytes(bytes: &[u8], start: usize) -> Self;
    fn to_bytes(&self) -> Vec<u8>;
}

impl DbColumn for u64 {
    fn from_bytes(bytes: &[u8], start: usize) -> Self {
        read_u64(bytes, start)
    }

    fn to_bytes(&self) -> Vec<u8> {
        u64::to_be_bytes(*self).into_iter().collect()
    }
}
