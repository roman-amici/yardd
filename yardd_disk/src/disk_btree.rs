use std::{io::Write, marker::PhantomData, mem::size_of};

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
        let offset = self.get_entry_offset(slot_index);

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

    fn slots_end(&'a self) -> usize {
        let u16_size = size_of::<u16>();
        SLOTS_START + (self.read_n_slots() + self.read_fragmented_slots()) as usize * u16_size
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

pub trait IndexPageReadSized<'a, KeyType>
where
    Self: IndexPageRead<'a, KeyType> + Sized,
    KeyType: DbColumn,
{
    fn iter(&'a self) -> PageIterator<'a, KeyType> {
        PageIterator {
            index_node: self,
            n_slots: self.read_n_slots(),
            slot_index: 0,
        }
    }
}

#[derive(Clone)]
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

        let slots_header = SlotHeader {
            occupied_slots: 0,
            fragmented_slots: 0,
            next_empty_offset: (node_page.inner_page.page_size() - 1) as u16,
        };

        node_page.write_slots_header(&slots_header);

        node_page
    }

    pub fn as_read_only(&'a self) -> IndexPage<'a, KeyType> {
        IndexPage {
            inner_page: self.inner_page,
            phantom: PhantomData,
        }
    }

    fn write_entry(&mut self, new_entry: KeyEntry<KeyType>, offset: usize) {
        let mut cursor = offset;
        cursor = write_u64(&mut self.inner_page.data, cursor, new_entry.page_id);

        cursor = write_u16(
            &mut self.inner_page.data,
            cursor,
            new_entry.slot_index.unwrap_or_default(),
        );

        let bytes = new_entry.key.to_bytes();
        write_bytes(&mut self.inner_page.data, cursor, &bytes);
    }

    pub fn append_key(&mut self, new_entry: KeyEntry<KeyType>) {
        self.inner_page.is_dirty = true;

        let entry_size_bytes = new_entry.key.len() + TUPLE_HEADER_SIZE;

        let slots_header = self.read_slots_header();
        let offset_start = slots_header.next_empty_offset as usize - entry_size_bytes;

        // size of entry + a new slot
        if offset_start < self.slots_end() {
            // TODO: Add linked pages
            panic!("No more space left for page!")
        }

        let mut insert_index = slots_header.occupied_slots;
        for (slot_index, entry) in self.iter().enumerate() {
            if new_entry.key <= entry.key {
                insert_index = slot_index as u16;
                break;
            }
        }

        self.insert_slot(insert_index as usize, offset_start);
        self.write_entry(new_entry, offset_start);
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
        self.inner_page.is_dirty = true;

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

    // TODO: figure out a better way to do this rather than duplicating it
    fn find_entry(&self, key: &KeyType) -> Option<KeyEntry<KeyType>> {
        self.iter().find(|entry| entry.key == *key)
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

impl<'a, KeyType> IndexPageReadSized<'a, KeyType> for IndexPageMut<'a, KeyType> where
    KeyType: DbColumn
{
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
}

impl<'a, KeyType> IndexPageRead<'a, KeyType> for IndexPage<'a, KeyType>
where
    KeyType: DbColumn,
{
    fn inner_page(&'a self) -> &'a Page {
        self.inner_page
    }
}

impl<'a, KeyType> IndexPageReadSized<'a, KeyType> for IndexPage<'a, KeyType> where KeyType: DbColumn {}

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
    use std::{
        marker::PhantomData,
        sync::{Arc, RwLock},
    };

    use crate::{
        disk_btree::IndexPageRead,
        page::{Page, PageType, SlotHeader},
    };

    use super::{IndexPageMut, IndexPageReadSized, KeyEntry};

    #[test]
    pub fn read_write_slots_header() {
        let mut page = Page {
            data: vec![0; 1024],
            page_id: 0,
            is_dirty: false,
        };

        let mut index_page = IndexPageMut::<u64> {
            inner_page: &mut page,
            phantom: PhantomData,
        };

        let slots_header = SlotHeader {
            occupied_slots: 0xFAFA,
            fragmented_slots: 0xAFAF,
            next_empty_offset: 0xEAFA,
        };

        index_page.write_slots_header(&slots_header);

        let header_read = index_page.read_slots_header();

        assert_eq!(slots_header.occupied_slots, header_read.occupied_slots);
        assert_eq!(slots_header.fragmented_slots, header_read.fragmented_slots);
        assert_eq!(
            slots_header.next_empty_offset,
            header_read.next_empty_offset
        );
    }

    #[test]
    pub fn init_index_page() {
        let mut page = Page {
            data: vec![0; 1024],
            page_id: 0,
            is_dirty: false,
        };
        let index_page = IndexPageMut::<u64>::init_page(PageType::IndexNode, 123, &mut page);

        let slots_header = index_page.read_slots_header();

        assert_eq!(0, slots_header.occupied_slots);
        assert_eq!(0, slots_header.fragmented_slots);
        assert_eq!(1023, slots_header.next_empty_offset);

        let header = page.read_header();
        assert_eq!(PageType::IndexNode, header.page_type);
        assert_eq!(page.page_id, header.page_id);
        assert_eq!(123, header.parent_page_id);
        assert!(page.is_dirty);
    }

    #[test]
    pub fn add_key() {
        let mut page = Page {
            data: vec![0; 1024],
            page_id: 0,
            is_dirty: false,
        };

        let mut index_page = IndexPageMut::<u64>::init_page(PageType::IndexNode, 123, &mut page);

        index_page.append_key(KeyEntry {
            key: 23,
            page_id: 345,
            slot_index: Some(289),
        });

        assert!(index_page.inner_page.is_dirty);

        let slots_header = index_page.read_slots_header();
        assert_eq!(0, slots_header.fragmented_slots);
        assert_eq!(1, slots_header.occupied_slots);

        let entry = index_page.find_entry(&23).expect("Key not found");
        assert_eq!(23, entry.key);
        assert_eq!(345, entry.page_id);
        assert_eq!(None, entry.slot_index);
    }

    #[test]
    pub fn add_key_reverse_insertion_order() {
        let mut page = Page {
            data: vec![0; 1024],
            page_id: 0,
            is_dirty: false,
        };

        let mut index_page = IndexPageMut::<u64>::init_page(PageType::IndexNode, 123, &mut page);

        index_page.append_key(KeyEntry {
            key: 3,
            page_id: 14,
            slot_index: None,
        });

        index_page.append_key(KeyEntry {
            key: 2,
            page_id: 15,
            slot_index: None,
        });

        index_page.append_key(KeyEntry {
            key: 1,
            page_id: 16,
            slot_index: None,
        });

        let mut iterator = index_page.iter();
        let entry1 = iterator.next().expect("Expected key");
        assert_eq!(1, entry1.key);
        assert_eq!(16, entry1.page_id);

        let entry2 = iterator.next().expect("Expected key");
        assert_eq!(2, entry2.key);
        assert_eq!(15, entry2.page_id);

        let entry3 = iterator.next().expect("Expected key");
        assert_eq!(3, entry3.key);
        assert_eq!(14, entry3.page_id);
    }

    #[test]
    pub fn add_key_insertion_order() {
        let mut page = Page {
            data: vec![0; 1024],
            page_id: 0,
            is_dirty: false,
        };

        let mut index_page = IndexPageMut::<u64>::init_page(PageType::IndexNode, 123, &mut page);

        index_page.append_key(KeyEntry {
            key: 1,
            page_id: 14,
            slot_index: None,
        });

        index_page.append_key(KeyEntry {
            key: 2,
            page_id: 15,
            slot_index: None,
        });

        index_page.append_key(KeyEntry {
            key: 3,
            page_id: 16,
            slot_index: None,
        });

        let mut iterator = index_page.iter();
        let entry1 = iterator.next().expect("Expected key");
        assert_eq!(1, entry1.key);
        assert_eq!(14, entry1.page_id);

        let entry2 = iterator.next().expect("Expected key");
        assert_eq!(2, entry2.key);
        assert_eq!(15, entry2.page_id);

        let entry3 = iterator.next().expect("Expected key");
        assert_eq!(3, entry3.key);
        assert_eq!(16, entry3.page_id);
    }
}
