use std::collections::{HashMap, LinkedList};
use std::io::Read;
use lru_cache::LruCache;
use soupdb::error::{Result, Error};
use soupdb::io::page::{PAGE_SIZE, PageId};

/// An LRU cache which maps page IDs to segments of working memory.
struct PageTable {
    page_count: u64,
    page_lru: LruCache<PageId, u64>,
}

impl PageTable {
    pub fn new(size: u64) -> PageTable {
        let pages = size / PAGE_SIZE;
        let actual_size = pages * PAGE_SIZE;
        let page_lru = LruCache::new(pages as usize);

        PageTable {
            page_count: pages,
            page_lru: page_lru
        }
    }

    /// Get the page index of a specified page ID. If this page is currently
    /// loaded, returns the index at which its data resides (in units of
    /// pages); otherwise, adds the ID to the LRU cache first, returning a new
    /// index into which it can be loaded.
    pub fn page_index(&mut self, id: PageId) -> u64 {
        if self.page_lru.contains_key(&id) {
            return self.page_lru.insert(id, 0).unwrap();
        }
        let mut next_index = self.page_lru.len() as u64;
        if self.page_lru.len() >= self.page_count as usize {
            match self.page_lru.remove_lru() {
                Some((k, v)) => {
                    next_index = v;
                }
                _ => {}
            }
        }
        self.page_lru.insert(id, next_index);
        next_index
    }

    /// Returns true if working memory already contains this page.
    pub fn contains_page(&mut self, id: &PageId) -> bool {
        return self.page_lru.contains_key(id);
    }
}

/// A block of memory for caching pages from database files.
pub struct WorkingMemory {
    page_data: Box<[u8]>,
    page_table: PageTable,
}

impl WorkingMemory {
    pub fn new(size: u64) -> WorkingMemory {
        let pages = size / PAGE_SIZE;
        let actual_size = pages * PAGE_SIZE;
        let mut zero_data = Vec::with_capacity(actual_size as usize);
        for i in 0..actual_size {
            zero_data.push(0);
        }
        let page_data = zero_data.into_boxed_slice();
        let page_table = PageTable::new(actual_size);

        WorkingMemory {
            page_data: page_data,
            page_table: page_table,
        }
    }

    /// Get a page from working memory. If the page is not present in memory,
    /// it will be loaded first, replacing the least recently used page if
    /// working memory is full.
    pub fn get_page<'a>(&'a mut self, page_id: PageId, buffer: &mut Read) -> Result<&'a [u8]> {
        let load = !self.page_table.contains_page(&page_id);
        let index = self.page_table.page_index(page_id);
        let buf = &mut self.page_data[(index*PAGE_SIZE) as usize .. ((index+1)*PAGE_SIZE) as usize];
        if load {
            // load from the provided buffer
            buffer.read(buf);
        }
        Ok(buf)
    }
}

#[test]
fn test_page_cache() {
    let mut cache = PageTable::new(PAGE_SIZE * 3);

    assert!(!cache.contains_page(&0));
    assert_eq!(cache.page_index(0), 0);
    assert!(cache.contains_page(&0));
    assert_eq!(cache.page_index(0), 0);

    assert_eq!(cache.page_index(2), 1);
    assert_eq!(cache.page_index(2), 1);
    assert_eq!(cache.page_index(0), 0);
    assert!(cache.contains_page(&0));
    assert!(cache.contains_page(&2));

    assert_eq!(cache.page_index(1), 2);
    assert_eq!(cache.page_index(1), 2);
    assert!(cache.contains_page(&0));
    assert!(cache.contains_page(&1));
    assert!(cache.contains_page(&2));

    assert_eq!(cache.page_index(3), 0);
    assert!(cache.contains_page(&1));
    assert!(cache.contains_page(&2));
    assert!(cache.contains_page(&3));
    assert!(!cache.contains_page(&0));
}

#[test]
fn test_working_memory() {
    let mut mem = WorkingMemory::new(PAGE_SIZE * 2);
}
