use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::io::Read;
use std::ops::Drop;
use std::sync::Arc;
use lru_cache::LruCache;
use soupdb::{Result, Error};
use soupdb::io::page::{PAGE_SIZE, PageId};

/// Prevents a page from being overwritten while in use. The PageTable uses
/// these objects to count active references to a given page in working memory;
/// once all active references are dropped, the page will re-enter the LRU
/// cache and may be overwritten.
#[derive(Debug)]
pub struct LruPageLock {
    page_table: Arc<RefCell<PageTable>>,
    page_id: PageId,
    index: u64,
}

impl LruPageLock {
    pub fn new(page_table: Arc<RefCell<PageTable>>, page_id: PageId, index: u64) -> LruPageLock {
        page_table.borrow_mut().incr_ref_count(&page_id);
        LruPageLock {page_table: page_table, page_id: page_id, index: index}
    }
}

impl Drop for LruPageLock {
    fn drop (&mut self) {
        self.page_table.borrow_mut().decr_ref_count(&self.page_id);
    }
}

/// An LRU cache which maps page IDs to segments of working memory.
#[derive(Debug)]
pub struct PageTable {
    page_count: u64,
    page_map: HashMap<PageId, u64>,
    page_lru: LruCache<PageId, u64>,
    ref_count: HashMap<PageId, u64>,
    available_slots: Vec<u64>,
}

impl PageTable {
    pub fn new(size: u64) -> PageTable {
        let pages = size / PAGE_SIZE;
        let actual_size = pages * PAGE_SIZE;
        let page_lru = LruCache::new(pages as usize);
        let available_slots = (0..pages).rev().collect();

        PageTable {
            page_count: pages,
            page_map: HashMap::new(),
            page_lru: page_lru,
            ref_count: HashMap::new(),
            available_slots: available_slots,
        }
    }

    pub fn incr_ref_count(&mut self, page_id: &PageId) {
        if self.page_lru.contains_key(page_id) {
            self.page_lru.remove(page_id);
        }
        *self.ref_count.entry(*page_id).or_insert(0) += 1;
    }

    pub fn decr_ref_count(&mut self, page_id: &PageId) {
        *self.ref_count.entry(*page_id).or_insert(0) -= 1;
        if *self.ref_count.get(page_id).unwrap() == 0 {
            self.ref_count.remove(page_id);
            self.page_lru.insert(*page_id, self.page_map.get(page_id).unwrap().clone());
        }
    }

    /// Get the page index of a specified page ID. If this page is currently
    /// loaded, returns the index at which its data resides (in units of
    /// pages); otherwise, adds the ID to the LRU cache first, returning a new
    /// index into which it can be loaded.
    pub fn page_index(&mut self, id: PageId) -> Option<u64> {
        if self.page_lru.contains_key(&id) {
            // no active references to this page, but it's still in working
            // memory; reuse it
            let index = self.page_lru.insert(id, 0).unwrap();
            return Some(index);
        }
        else if self.ref_count.contains_key(&id) {
            // there are other active references to this page already
            let index = self.page_map.get(&id).unwrap().clone();
            return Some(index);
        }
        match self.available_slots.pop() {
            Some(index) => {
                // fill a previously empty block of working memory
                self.page_map.insert(id, index);
                Some(index)
            },
            None => match self.page_lru.remove_lru() {
                Some((_, index)) => {
                    // expire a block of working memory and overwrite it
                    self.page_map.insert(id, index);
                    Some(index)
                },
                // working memory is completely full
                _ => None
            }
        }
    }

    /// Returns true if working memory already contains this page. This method
    /// is mutable because contains_key pushes the key to the front of the LRU
    /// if it exists.
    pub fn contains_page(&mut self, id: &PageId) -> bool {
        return self.ref_count.contains_key(id) || self.page_lru.contains_key(id);
    }
}

/// A block of memory for caching pages from database files.
pub struct WorkingMemory {
    page_data: Box<[u8]>,
    page_table: Arc<RefCell<PageTable>>,
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
            page_table: Arc::new(RefCell::new(page_table)),
        }
    }

    pub fn contains_page(&self, page_id: &PageId) -> bool {
        self.page_table.borrow_mut().contains_page(page_id)
    }

    /// This method returns an Option<LruPageLock> with the page lock if it has
    /// been loaded into memory, or None if working memory is full and
    /// completely locked.
    pub fn page_index(&self, page_id: PageId) -> Option<LruPageLock> {
        let index_result = {
            let mut page_table = self.page_table.borrow_mut();
            page_table.page_index(page_id)
        };
        match index_result {
            Some(index) => Some(LruPageLock::new(Arc::clone(&self.page_table), page_id, index)),
            None => None
        }
    }

    /// Get a page from working memory. If the page is not present in memory,
    /// it will be loaded first, replacing the least recently used page if
    /// working memory is full.
    pub fn get_page<'a>(&'a mut self, page_id: PageId, buffer: &mut Read) -> Result<Option<(LruPageLock, &'a [u8])>> {
        let load = !self.page_table.borrow_mut().contains_page(&page_id);
        let result = self.page_index(page_id);
        match result {
            Some(lock) => {
                let mut index = match &lock {
                    &LruPageLock {page_table: _, page_id: _, index} => {
                        let buf = &mut self.page_data[(index*PAGE_SIZE) as usize .. ((index+1)*PAGE_SIZE) as usize];
                        if load {
                            // load from the provided buffer
                            buffer.read(buf);
                        }
                        index
                    }
                };
                let buf = &mut self.page_data[(index*PAGE_SIZE) as usize .. ((index+1)*PAGE_SIZE) as usize];
                Ok(Some((lock, buf)))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_page_index(cache: &mut WorkingMemory, id: PageId, expected_index: u64) -> Option<LruPageLock> {
        let result = cache.page_index(id);
        match result {
            Some(LruPageLock {page_table: _, page_id: _, index: result_index}) => {
                assert_eq!(expected_index, result_index);
            },
            None => {
                panic!("expected page index for page {} not found", id);
            }
        };
        result
    }

    #[test]
    fn test_page_cache() {
        let mut cache = WorkingMemory::new(PAGE_SIZE * 3);

        // adding a page to working memory: cache now contains that page
        assert!(!cache.contains_page(&0));
        check_page_index(&mut cache, 0, 0);
        assert!(cache.contains_page(&0));
        check_page_index(&mut cache, 0, 0);

        // adding another page takes up the next slow, and doesn't evict the
        // previously added page
        assert!(!cache.contains_page(&2));
        check_page_index(&mut cache, 2, 1);
        assert!(cache.contains_page(&0));
        assert!(cache.contains_page(&2));
        check_page_index(&mut cache, 2, 1);
        check_page_index(&mut cache, 0, 0);
        assert!(cache.contains_page(&0));
        assert!(cache.contains_page(&2));

        // adding one more page results in all three pages loaded
        check_page_index(&mut cache, 1, 2);
        check_page_index(&mut cache, 1, 2);
        assert!(cache.contains_page(&0));
        assert!(cache.contains_page(&1));
        assert!(cache.contains_page(&2));

        // adding a new page ejects the first page loaded and overwrites it
        check_page_index(&mut cache, 3, 0);
        assert!(cache.contains_page(&1));
        assert!(cache.contains_page(&2));
        assert!(cache.contains_page(&3));
        assert!(!cache.contains_page(&0));
    }

    #[test]
    fn test_lru_page_lock() {
        let mut cache = WorkingMemory::new(PAGE_SIZE * 3);
        {
            // we start by adding page 5 and holding the lock...
            let lock = check_page_index(&mut cache, 5, 0).unwrap();
            assert!(cache.contains_page(&5));
            // page 5 can't be overwritten until our lock goes out of scope
            check_page_index(&mut cache, 6, 1);
            check_page_index(&mut cache, 7, 2);
            check_page_index(&mut cache, 8, 1);
            assert!(cache.contains_page(&5));
        }
        // now it can
        assert!(cache.contains_page(&5));
        check_page_index(&mut cache, 7, 2);
        check_page_index(&mut cache, 8, 1);
        // it's now the LRU and will be ejected here
        check_page_index(&mut cache, 6, 0);
        assert!(!cache.contains_page(&5));
    }
}
