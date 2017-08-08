use std::collections::{HashMap, LinkedList};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::Read;
use std::ops::Drop;
use std::sync::mpsc::{channel, Sender, Receiver};
use lru_cache::LruCache;
use ::{Result, Error};
use ::io::page::{PAGE_SIZE, PageId};

/// Prevents a page from being overwritten while in use. The PageTable uses
/// these objects to count active references to a given page in working memory;
/// once all active references are dropped, the page will re-enter the LRU
/// cache and may be overwritten.
#[derive(Debug)]
pub struct LruPageLock<T: Hash + Debug + Eq + Clone> {
    channel: Sender<T>,
    page_id: T,
    index: u64,
}

impl<T: Hash + Debug + Eq + Clone> LruPageLock<T> {
    pub fn new(page_table: &mut PageTable<T>, page_id: T, index: u64) -> LruPageLock<T> {
        page_table.incr_ref_count(&page_id);
        LruPageLock {channel: page_table.sender.clone(), page_id: page_id, index: index}
    }
}

impl<T: Hash + Debug + Eq + Clone> Drop for LruPageLock<T> {
    fn drop (&mut self) {
        self.channel.send(self.page_id.clone());
    }
}

/// An LRU cache which maps page IDs to segments of working memory.
#[derive(Debug)]
pub struct PageTable<T: Hash + Debug + Eq + Clone> {
    page_count: u64,
    page_map: HashMap<T, u64>,
    page_lru: LruCache<T, u64>,
    ref_count: HashMap<T, u64>,
    available_slots: Vec<u64>,
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T: Hash + Debug + Eq + Clone> PageTable<T> {
    pub fn new(size: u64) -> PageTable<T> {
        let pages = size / PAGE_SIZE;
        let actual_size = pages * PAGE_SIZE;
        let page_lru = LruCache::new(pages as usize);
        let available_slots = (0..pages).rev().collect();

        let (sender, receiver) = channel();

        PageTable {
            page_count: pages,
            page_map: HashMap::new(),
            page_lru: page_lru,
            ref_count: HashMap::new(),
            available_slots: available_slots,
            sender: sender,
            receiver: receiver,
        }
    }

    pub fn incr_ref_count(&mut self, page_id: &T) {
        if self.page_lru.contains_key(page_id) {
            self.page_lru.remove(page_id);
        }
        *self.ref_count.entry(page_id.clone()).or_insert(0) += 1;
    }

    pub fn decr_ref_count(&mut self, page_id: &T) {
        *self.ref_count.entry(page_id.clone()).or_insert(0) -= 1;
        if *self.ref_count.get(page_id).unwrap() == 0 {
            self.ref_count.remove(page_id);
            self.page_lru.insert(page_id.clone(), self.page_map.get(page_id).unwrap().clone());
        }
    }

    /// Get the page index of a specified page ID. If this page is currently
    /// loaded, returns the index at which its data resides (in units of
    /// pages); otherwise, adds the ID to the LRU cache first, returning a new
    /// index into which it can be loaded.
    pub fn page_index(&mut self, page_id: T) -> Option<LruPageLock<T>> {
        self.check_messages();
        if self.page_lru.contains_key(&page_id) {
            // no active references to this page, but it's still in working
            // memory; reuse it
            let index = self.page_lru.remove(&page_id).unwrap();
            return Some(LruPageLock::new(self, page_id, index));
        }
        else if self.ref_count.contains_key(&page_id) {
            // there are other active references to this page already
            let index = self.page_map.get(&page_id).unwrap().clone();
            return Some(LruPageLock::new(self, page_id, index));
        }
        match self.available_slots.pop() {
            Some(index) => {
                // fill a previously empty block of working memory
                self.page_map.insert(page_id.clone(), index);
                Some(LruPageLock::new(self, page_id, index))
            },
            None => match self.page_lru.remove_lru() {
                Some((_, index)) => {
                    // expire a block of working memory and overwrite it
                    self.page_map.insert(page_id.clone(), index);
                    Some(LruPageLock::new(self, page_id, index))
                },
                // working memory is completely full
                _ => None
            }
        }
    }

    /// Returns true if working memory already contains this page. This method
    /// is mutable because contains_key pushes the key to the front of the LRU
    /// if it exists.
    pub fn contains_page(&mut self, id: &T) -> bool {
        self.check_messages();
        return self.ref_count.contains_key(id) || self.page_lru.contains_key(id);
    }

    fn check_messages(&mut self) {
        loop {
            let msg = self.receiver.try_recv();
            match msg {
                Ok(page_id) => {
                    self.decr_ref_count(&page_id)
                }
                _ => {
                    break;
                }
            }
        }
    }
}

/// A block of memory for caching pages from database files.
pub struct WorkingMemory<T: Hash + Debug + Eq + Clone> {
    page_data: Box<[u8]>,
    page_table: PageTable<T>,
}

impl<T: Hash + Debug + Eq + Clone> WorkingMemory<T> {
    pub fn new(size: u64) -> WorkingMemory<T> {
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
    pub fn get_page<'a, R: FnOnce(&mut [u8]) -> ()>(&'a mut self, page_id: T, reader: R) -> Result<Option<(LruPageLock<T>, &'a [u8])>> {
        let load = !self.page_table.contains_page(&page_id);
        let result = self.page_table.page_index(page_id);
        match result {
            Some(lock) => {
                let mut index = match &lock {
                    &LruPageLock {index, ..} => {
                        let buf = &mut self.page_data[(index*PAGE_SIZE) as usize .. ((index+1)*PAGE_SIZE) as usize];
                        if load {
                            // load from the provided buffer
                            reader(buf);
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

    fn check_page_index(cache: &mut PageTable<u8>, id: u8, expected_index: u64) -> Option<LruPageLock<u8>> {
        let result = cache.page_index(id);
        match result {
            Some(LruPageLock {index, ..}) => {
                assert_eq!(expected_index, index);
            },
            None => {
                panic!("expected page index for page {} not found", id);
            }
        };
        result
    }

    #[test]
    fn test_page_cache() {
        let mut cache = PageTable::new(PAGE_SIZE * 3);

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
        let mut cache = PageTable::new(PAGE_SIZE * 3);
        {
            // we start by adding page 5 and holding the lock...
            let lock = check_page_index(&mut cache, 5, 0).unwrap();
            assert!(cache.contains_page(&5));
            // page 5 can't be overwritten until our lock goes out of scope, no
            // matter how many other pages we cache
            check_page_index(&mut cache, 6, 1);
            check_page_index(&mut cache, 7, 2);
            check_page_index(&mut cache, 8, 1);
            check_page_index(&mut cache, 9, 2);
            check_page_index(&mut cache, 10, 1);
            assert!(cache.contains_page(&5));
        }
        // this post-drop check places page 5 in the LRU, but it's still cached
        assert!(cache.contains_page(&5));
        check_page_index(&mut cache, 7, 2);
        check_page_index(&mut cache, 8, 1);
        // make sure our page is still cached - but contains_page has the side
        // effect of pushing it to the front of the LRU
        assert!(cache.contains_page(&5));
        check_page_index(&mut cache, 4, 2);
        check_page_index(&mut cache, 6, 1);
        // one more confirmation, we should still have our page!
        assert!(cache.contains_page(&5));
        check_page_index(&mut cache, 7, 2);
        check_page_index(&mut cache, 8, 1);
        // it will finally be ejected here
        check_page_index(&mut cache, 6, 0);
        assert!(!cache.contains_page(&5));
    }

    #[test]
    fn test_get_page() {
        let mut working_memory = WorkingMemory::new(PAGE_SIZE * 3);

        // check that the result buffer includes data from the reader
        {
            let reader = |buf: &mut [u8]| buf[0] = 5;
            let (lock, buf) = working_memory.get_page(0_u8, reader).unwrap().unwrap();
            assert_eq!(buf[0], 5);
            assert_eq!(buf[1], 0);
        }
        {
            let reader = |buf: &mut [u8]| buf[0] = 8;
            let (lock, buf) = working_memory.get_page(1_u8, reader).unwrap().unwrap();
            assert_eq!(buf[0], 8);
            assert_eq!(buf[1], 0);
        }

        // these readers shouldn't be used because the page is still cached
        {
            let reader = |buf: &mut [u8]| panic!("reading when page should be cached");
            let (lock, buf) = working_memory.get_page(0_u8, reader).unwrap().unwrap();
            assert_eq!(buf[0], 5);
            assert_eq!(buf[1], 0);
        }
        {
            let reader = |buf: &mut [u8]| panic!("reading when page should be cached");
            let (lock, buf) = working_memory.get_page(1_u8, reader).unwrap().unwrap();
            assert_eq!(buf[0], 8);
            assert_eq!(buf[1], 0);
        }
    }
}
