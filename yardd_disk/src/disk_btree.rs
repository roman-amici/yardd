use std::{marker::PhantomData, mem::size_of};

use crate::{
    page::{
        DbColumn, Page, PageHeader, PageId, PageType, SlotHeader, SlotIndex, HEADER_SIZE,
        PAGE_MAGIC_NUMBER, PAGE_SIZE_BYTES, SLOTS_FRAGMENTED_SLOTS_START, SLOTS_HEADER_SIZE,
        SLOTS_HEADER_START, SLOTS_NEXT_EMPTY_OFFSET_START, SLOTS_OCCUPIED_SLOTS_START, SLOTS_START,
    },
    serialization_helpers::*,
};

pub trait IndexPageRead<'a, KeyType>
where
    KeyType: DbColumn,
{
    fn inner_page(&'a self) -> &'a Page;

    fn read_n_slots(&'a self) -> u16 {
        read_u16(&self.inner_page().data, SLOTS_OCCUPIED_SLOTS_START)
    }

    fn read_fragmented_slots(&'a self) -> u16 {
        read_u16(&self.inner_page().data, SLOTS_FRAGMENTED_SLOTS_START)
    }

    fn read_next_empty_offset(&'a self) -> u16 {
        read_u16(&self.inner_page().data, SLOTS_NEXT_EMPTY_OFFSET_START)
    }

    fn read_key_node(&'a self, slot_index: SlotIndex) -> KeyEntry<KeyType> {
        let offset = self.inner_page().get_slot_offset(slot_index);

        let page_id = read_u64(&self.inner_page().data, offset);

        // Since this is a B+ tree, only leaf nodes have tuple pointers
        let slot_index = if self.inner_page().read_page_type() == PageType::IndexLeaf {
            Some(read_u16(
                &self.inner_page().data,
                offset + size_of::<PageId>(),
            ))
        } else {
            None
        };

        let key = KeyType::from_bytes(
            &self.inner_page().data,
            offset + size_of::<PageId>() + size_of::<SlotIndex>(),
        );

        KeyEntry {
            key,
            page_id,
            slot_index,
        }
    }

    fn get_entry_offset(&'a self, slot_index: SlotIndex) -> usize {
        let start = SLOTS_START + std::mem::size_of::<u16>() * slot_index as usize;
        read_u16(&self.inner_page().data, start) as usize
    }

    fn get_fragmented_slots(&'a self) -> Vec<usize> {
        let mut slots = vec![];

        for i in 0..self.read_fragmented_slots() {
            slots.push(self.get_fragmented_slot_offset(i))
        }

        slots
    }

    fn get_occupied_slots(&'a self) -> Vec<usize> {
        let mut slots = vec![];

        for i in 0..self.read_n_slots() {
            slots.push(self.get_entry_offset(i));
        }

        slots
    }

    fn get_fragmented_slot_offset(&'a self, slot_index: SlotIndex) -> usize {
        let u16_size = size_of::<u16>();
        let start = SLOTS_START
            + u16_size * self.read_n_slots() as usize
            + (slot_index as usize) * u16_size;

        read_u16(&self.inner_page().data, start) as usize
    }

    fn read_slots_header(&'a self) -> SlotHeader {
        SlotHeader {
            occupied_slots: self.read_n_slots(),
            fragmented_slots: self.read_fragmented_slots(),
            next_empty_offset: self.read_next_empty_offset(),
        }
    }
}

pub struct KeyEntry<KeyType>
where
    KeyType: DbColumn,
{
    key: KeyType,
    page_id: PageId,
    slot_index: Option<SlotIndex>,
}

pub struct IndexPage<'a, KeyType>
where
    KeyType: DbColumn,
{
    phantom: PhantomData<KeyType>,
    inner_page: &'a Page,
}

pub struct IndexPageMut<'a, KeyType>
where
    KeyType: DbColumn,
{
    phantom: PhantomData<KeyType>,
    inner_page: &'a mut Page,
}

// Regular page header + page slots
pub const INDEX_PAGE_HEADER_SIZE: usize = HEADER_SIZE + SLOTS_HEADER_SIZE;
pub const TUPLE_HEADER_SIZE: usize = size_of::<PageId>() + size_of::<SlotIndex>();

impl<'a, KeyType> IndexPageMut<'a, KeyType>
where
    KeyType: DbColumn,
{
    pub fn init_page(page_type: PageType, parent_page_id: PageId, page: &'a mut Page) -> Self {
        let header = PageHeader {
            magic_number: PAGE_MAGIC_NUMBER,
            bytes_remaining: PAGE_SIZE_BYTES as u16 - INDEX_PAGE_HEADER_SIZE as u16,
            page_type,
            log_sequence_number: 0,
            parent_page_id,
            page_id: page.page_id,
        };

        page.write_header(header);

        let mut node_page = Self {
            inner_page: page,
            phantom: PhantomData,
        };

        node_page.set_n_slots(0);

        node_page
    }

    pub fn set_n_slots(&mut self, n_slots: u16) {
        write_u16(&mut self.inner_page.data, SLOTS_HEADER_START, n_slots);
    }

    pub fn as_read_only(&'a self) -> IndexPage<'a, KeyType> {
        IndexPage {
            inner_page: self.inner_page,
            phantom: PhantomData,
        }
    }

    pub fn append_key(&mut self, new_entry: KeyEntry<KeyType>) {
        let key_bytes = new_entry.key.to_bytes();
        let entry_size_bytes = key_bytes.len() + TUPLE_HEADER_SIZE;

        // size of entry + a new slot
        if entry_size_bytes + size_of::<u16>() > self.inner_page.read_bytes_remaining() as usize {
            // TODO: Add linked pages
            panic!("No more space left for page!")
        }

        let slots_header = self.read_slots_header();

        let mut insert_index = slots_header.occupied_slots;
        for (slot_index, entry) in self.iter().enumerate() {
            if new_entry.key > entry.key {
                insert_index = slot_index as u16;
                break;
            }
        }

        let offset_start = slots_header.next_empty_offset as usize - entry_size_bytes;

        self.insert_slot(insert_index as usize, offset_start);
    }

    pub fn write_slots_header(&mut self, slots_header: &SlotHeader) {
        write_u16(
            &mut self.inner_page.data,
            SLOTS_OCCUPIED_SLOTS_START,
            slots_header.occupied_slots,
        );
        write_u16(
            &mut self.inner_page.data,
            SLOTS_FRAGMENTED_SLOTS_START,
            slots_header.fragmented_slots,
        );
        write_u16(
            &mut self.inner_page.data,
            SLOTS_NEXT_EMPTY_OFFSET_START,
            slots_header.next_empty_offset,
        );
    }

    pub fn update_slots(
        &mut self,
        slots: Vec<usize>,
        slots_fragmented: Vec<usize>,
        next_empty_offset: usize,
    ) {
        let header = SlotHeader {
            occupied_slots: slots.len() as u16,
            fragmented_slots: slots_fragmented.len() as u16,
            next_empty_offset: next_empty_offset as u16,
        };

        self.write_slots_header(&header);

        for (i, offset) in slots.iter().enumerate() {
            let start = SLOTS_START + size_of::<u16>() * i;
            write_u16(&mut self.inner_page.data, start, *offset as u16);
        }

        for (i, offset) in slots_fragmented.iter().enumerate() {
            let start = SLOTS_START + size_of::<u16>() * (header.occupied_slots as usize + i);
            write_u16(&mut self.inner_page.data, start, *offset as u16);
        }
    }

    fn insert_slot(&mut self, insert_index: usize, offset_start: usize) {
        // Do this with the most possible copying...

        let mut slots = self.get_occupied_slots();
        let slots_fragmented = self.get_fragmented_slots();

        slots.insert(insert_index as usize, offset_start);

        self.update_slots(slots, slots_fragmented, offset_start - 1);
    }

    fn iter(&'a self) -> PageIterator<'a, KeyType> {
        PageIterator {
            index_node: self,
            n_slots: self.read_n_slots(),
            slot_index: 0,
        }
    }
}

impl<'a, KeyType> IndexPageRead<'a, KeyType> for IndexPageMut<'a, KeyType>
where
    KeyType: DbColumn,
{
    fn inner_page(&'a self) -> &'a Page {
        &self.inner_page
    }
}

impl<'a, KeyType> IndexPage<'a, KeyType>
where
    KeyType: DbColumn,
{
    pub fn read_existing_page(page: &'a Page) -> Self {
        IndexPage {
            inner_page: page,
            phantom: PhantomData,
        }
    }

    fn iter(&'a self) -> PageIterator<'a, KeyType> {
        PageIterator {
            index_node: self,
            n_slots: self.read_n_slots(),
            slot_index: 0,
        }
    }
}

impl<'a, KeyType> IndexPageRead<'a, KeyType> for IndexPage<'a, KeyType>
where
    KeyType: DbColumn,
{
    fn inner_page(&'a self) -> &'a Page {
        self.inner_page
    }
}

pub struct PageIterator<'a, KeyType>
where
    KeyType: DbColumn,
{
    index_node: &'a dyn IndexPageRead<'a, KeyType>,
    n_slots: u16,
    slot_index: u16,
}

impl<'a, KeyType> Iterator for PageIterator<'a, KeyType>
where
    KeyType: DbColumn,
{
    type Item = KeyEntry<KeyType>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.slot_index >= self.n_slots {
            None
        } else {
            let k = self.index_node.read_key_node(self.slot_index);
            self.slot_index += 1;
            Some(k)
        }
    }
}

mod test {
    use std::sync::{Arc, RwLock};

    use crate::page::{Page, PageType};

    use super::IndexPageMut;

    #[test]
    pub fn test() {
        let page = Page {
            data: vec![0; 1024],
            page_id: 0,
            is_dirty: false,
        };

        let x = Arc::new(RwLock::new(page));

        let mut page_guard = x.write().unwrap();

        let index_page = IndexPageMut::<u64>::init_page(PageType::IndexNode, 0, &mut page_guard);
    }
}
