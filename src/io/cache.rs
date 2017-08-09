use std::collections::{HashMap, LinkedList, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::Read;
use std::ops::{Deref, DerefMut, Drop};
use std::sync::mpsc::{channel, Sender, Receiver};
use lru_cache::LruCache;
use ::{Result, Error};
use ::io::page::{PAGE_SIZE, PageId};

/// Many read locks, or a single write lock, can be held against a single page
/// at one time.
#[derive(Debug, PartialEq, Clone)]
pub enum LockType {
    Read,
    Write,
}

/// Represents a request for a lock that we couldn't yet satisfy. Try again
/// later in the order it was received.
#[derive(Debug)]
pub struct LockRequest<T: Hash + Debug + Eq + Clone> {
    lock_type: LockType,
    channel: Sender<PageLock<T>>,
}

impl<T: Hash + Debug + Eq + Clone> LockRequest<T> {
    pub fn new(lock_type: LockType, channel: Sender<PageLock<T>>) -> LockRequest<T> {
        LockRequest {lock_type, channel}
    }
}

/// Prevents a page from being overwritten while in use. The PageTable uses
/// these objects to count active references to a given page in working memory;
/// once all active references are dropped, the page will re-enter the LRU
/// cache and may be overwritten.
#[derive(Debug)]
pub struct PageLock<T: Hash + Debug + Eq + Clone> {
    channel: Sender<T>,
    page_id: T,
    index: u64,
    lock_type: LockType,
}

impl<T: Hash + Debug + Eq + Clone> PageLock<T> {
    pub fn new(channel: Sender<T>, page_id: T, index: u64, lock_type: LockType) -> PageLock<T> {
        PageLock {channel, page_id, index, lock_type}
    }
}

impl<T: Hash + Debug + Eq + Clone> Drop for PageLock<T> {
    fn drop (&mut self) {
        self.channel.send(self.page_id.clone());
    }
}

/// A counter of active references to a page with a bidirectional channel for
/// notification of expired references.
#[derive(Debug)]
pub struct ActiveRefCount<T: Hash + Debug + Eq + Clone> {
    active_count: HashMap<T, u64>,
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T: Hash + Debug + Eq + Clone> ActiveRefCount<T> {
    pub fn new(size: usize) -> ActiveRefCount<T> {
        let (sender, receiver) = channel();
        ActiveRefCount {
            active_count: HashMap::with_capacity(size),
            sender,
            receiver,
        }
    }

    pub fn active(&self, key: &T) -> u64 {
        match self.active_count.get(key){
            Some(n) => *n,
            None => 0,
        }
    }
}

impl<T: Hash + Debug + Eq + Clone> Deref for ActiveRefCount<T> {
    type Target = HashMap<T, u64>;
    fn deref(&self) -> &HashMap<T, u64> {
        &self.active_count
    }
}

impl<T: Hash + Debug + Eq + Clone> DerefMut for ActiveRefCount<T> {
    fn deref_mut(&mut self) -> &mut HashMap<T, u64> {
        &mut self.active_count
    }
}

/// An LRU cache which maps page IDs to segments of working memory.
#[derive(Debug)]
pub struct PageTable<T: Hash + Debug + Eq + Clone> {
    page_count: u64,
    /// map of pages currently cached -> index
    page_map: HashMap<T, u64>,
    /// LRU of page -> index
    page_lru: LruCache<T, u64>,
    /// count of currently active readers
    reader_count: ActiveRefCount<T>,
    /// count of currently active writers (max 1 per page)
    writer_count: ActiveRefCount<T>,
    /// requests for a lock that haven't been responded to yet
    pending_requests: HashMap<T, VecDeque<LockRequest<T>>>,
    /// pages waiting for available cache space; actual requests will be
    /// found in pending_requests
    pending_pages: VecDeque<T>,
    /// cache indexes that are as of yet unused
    available_slots: Vec<u64>,
}

impl<T: Hash + Debug + Eq + Clone> PageTable<T> {
    pub fn new(size: u64) -> PageTable<T> {
        let pages = (size / PAGE_SIZE) as usize;
        let actual_size = (pages as u64) * PAGE_SIZE;
        let page_lru = LruCache::new(pages);
        let available_slots = (0..pages as u64).rev().collect();

        PageTable {
            page_count: pages as u64,
            page_map: HashMap::new(),
            page_lru: page_lru,
            reader_count: ActiveRefCount::new(pages),
            writer_count: ActiveRefCount::new(pages),
            pending_requests: HashMap::with_capacity(0x100),
            pending_pages: VecDeque::with_capacity(0x100),
            available_slots: available_slots,
        }
    }

    /// Returns true if working memory already contains this page. This method
    /// is mutable because contains_key pushes the key to the front of the LRU
    /// if it exists.
    pub fn contains_page(&mut self, id: &T) -> bool {
        self.check_messages();
        self._contains_page(&id)
    }

    pub fn request_lock(&mut self, page_id: &T, lock_type: &LockType, channel: &mut Sender<PageLock<T>>) {
        self.check_messages();
        if !self.pending_requests.contains_key(&page_id) {
            self.pending_requests.insert(page_id.clone(), VecDeque::with_capacity(0x100));
        }
        self.pending_requests.get_mut(&page_id).unwrap().push_back(
            LockRequest::new(lock_type.clone(), channel.clone())
        );
        self.handle_pending_requests(&page_id);
    }

    fn incr_ref_count(&mut self, page_id: &T, lock_type: &LockType) -> Sender<T> {
        if self.page_lru.contains_key(page_id) {
            self.page_lru.remove(page_id);
        }
        let mut ref_count = match lock_type {
            &LockType::Read => &mut self.reader_count,
            &LockType::Write => &mut self.writer_count,
        };
        {
            let entry = ref_count.entry(page_id.clone()).or_insert(0);
            *entry += 1;
        }
        ref_count.sender.clone()
    }

    fn decr_ref_count(&mut self, page_id: &T, lock_type: &LockType) {
        let rc = {
            let mut ref_count = match lock_type {
                &LockType::Read => &mut self.reader_count,
                &LockType::Write => &mut self.writer_count,
            };
            let entry = ref_count.entry(page_id.clone()).or_insert(0);
            *entry -= 1;
            entry.clone()
        };
        if rc == 0 {
            if self.pending_requests.contains_key(&page_id) && self.pending_requests.get(&page_id).unwrap().len() > 0 {
                self.handle_pending_requests(&page_id);
            }
            if self.reader_count.active(&page_id) == 0 && self.writer_count.active(&page_id) == 0 {
                self.page_lru.insert(page_id.clone(), self.page_map.get(page_id).unwrap().clone());
            }
        }
    }

    /// Get the page index of a specified page ID. If this page is currently
    /// loaded, returns the index at which its data resides (in units of
    /// pages); otherwise, adds the ID to the LRU cache first, returning a new
    /// index into which it can be loaded.
    pub(self) fn page_index(&mut self, page_id: T, lock_type: LockType) -> Option<PageLock<T>> {
        if self._contains_page(&page_id) {
            // no active references to this page, but it's still in working
            // memory; reuse it
            let index = self.get_index_for_lock(&page_id).unwrap();
            return Some(self.create_lock(page_id, index, lock_type));
        }
        match self.available_slots.pop() {
            Some(index) => {
                // fill a previously empty block of working memory
                self.page_map.insert(page_id.clone(), index);
                Some(self.create_lock(page_id, index, lock_type))
            },
            None => match self.page_lru.remove_lru() {
                Some((_, index)) => {
                    // expire a block of working memory and overwrite it
                    self.page_map.insert(page_id.clone(), index);
                    Some(self.create_lock(page_id, index, lock_type))
                },
                // working memory is completely full
                _ => None
            }
        }
    }

    pub fn tick(&mut self) {
        self.check_messages();
    }

    fn handle_pending_requests(&mut self, page_id: &T) {
        if self.pending_requests.contains_key(&page_id) {
            let mut pending = self.pending_requests.remove(&page_id).unwrap();
            while pending.len() > 0 {
                if self.can_grant_lock(&page_id, &pending[0].lock_type) {
                    let mut request = pending.pop_front().unwrap();
                    let lock = self.page_index(page_id.clone(), request.lock_type.clone()).unwrap();
                    request.channel.send(lock);
                } else {
                    break;
                }
            }
            self.pending_requests.insert(page_id.clone(), pending);
        }
    }

    fn can_grant_lock(&mut self, page_id: &T, lock_type: &LockType) -> bool {
        (self._contains_page(&page_id) || self.can_load_page()) && match lock_type {
            &LockType::Read => match self.writer_count.get(&page_id) {
                Some(n) if *n > 0 => false,
                _ => match self.pending_requests.get(&page_id) {
                    Some(v) => {
                        // if the next request is a write request, don't accept any
                        // more read requests
                        v.len() == 0 || v[0].lock_type == LockType::Write
                    },
                    _ => true,
                },
            },
            &LockType::Write => match self.writer_count.get(&page_id) {
                Some(n) if *n > 0 => false,
                _ => match self.reader_count.get(&page_id) {
                    Some(n) if *n > 0 => false,
                    _ => true
                }
            },
        }
    }

    fn get_index_for_lock(&mut self, page_id: &T) -> Option<u64> {
        if self.page_lru.contains_key(&page_id) {
            // no active references to this page, but it's still in working
            // memory; reuse it
            Some(self.page_lru.remove(&page_id).unwrap())
        }
        else if self.reader_count.contains_key(&page_id) {
            // there are other active references to this page already
            Some(self.page_map.get(&page_id).unwrap().clone())
        }
        else {
            None
        }
    }

    fn create_lock(&mut self, page_id: T, index: u64, lock_type: LockType) -> PageLock<T> {
        let channel = self.incr_ref_count(&page_id, &lock_type);
        PageLock::new(channel, page_id, index, lock_type)
    }

    fn can_load_page(&mut self) -> bool {
        !(self.available_slots.is_empty() && self.page_lru.len() == 0)
    }

    fn _contains_page(&mut self, id: &T) -> bool {
        self.page_lru.contains_key(id) ||
            self.reader_count.active(id) > 0 ||
            self.writer_count.active(id) > 0
    }

    fn check_messages(&mut self) {
        loop {
            match self.reader_count.receiver.try_recv() {
                Ok(page_id) => {
                    self.decr_ref_count(&page_id, &LockType::Read);
                }
                _ => {
                    break;
                }
            }
        }
        loop {
            match self.writer_count.receiver.try_recv() {
                Ok(page_id) => {
                    self.decr_ref_count(&page_id, &LockType::Write);
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
    pub fn get_page<'a, R: FnOnce(&mut [u8]) -> ()>(&'a mut self, page_id: T, reader: R) -> Result<Option<(PageLock<T>, &'a [u8])>> {
        let load = !self.page_table.contains_page(&page_id);
        let result = self.page_table.page_index(page_id, LockType::Read);
        match result {
            Some(lock) => {
                let mut index = match &lock {
                    &PageLock {index, ..} => {
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

    fn check_page_index(cache: &mut PageTable<u8>, id: u8, expected_index: u64) -> Option<PageLock<u8>> {
        cache.tick();
        let result = cache.page_index(id, LockType::Read);
        match result {
            Some(PageLock {index, ..}) => {
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

        // adding another page takes up the next slot, and doesn't evict the
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
    fn test_page_lock() {
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

    #[test]
    fn test_get_lock() {
        let mut cache = PageTable::new(PAGE_SIZE * 3);
        let (mut sender, mut receiver) = channel();
        let (mut sender2, mut receiver2) = channel();
        let (mut sender3, mut receiver3) = channel();

        {
            cache.request_lock(&0, &LockType::Read, &mut sender);
            let result = receiver.try_recv();
            assert!(result.is_ok());
        }

        {
            cache.request_lock(&0, &LockType::Read, &mut sender);
            let page0_1 = receiver.try_recv();
            assert!(page0_1.is_ok());

            cache.request_lock(&0, &LockType::Read, &mut sender);
            let page0_2 = receiver.try_recv();
            assert!(page0_2.is_ok());

            cache.request_lock(&1, &LockType::Read, &mut sender);
            let page1_1 = receiver.try_recv();
            assert!(page1_1.is_ok());

            cache.request_lock(&2, &LockType::Read, &mut sender);
            let page2_1 = receiver.try_recv();
            assert!(page2_1.is_ok());

            // too many cached pages; can't get this one immediately
            cache.request_lock(&3, &LockType::Read, &mut sender2);
            let r = receiver2.try_recv();
            assert!(r.is_err());

            // this one is still cached so we can get additional read locks
            cache.request_lock(&0, &LockType::Read, &mut sender);
            let page0_3 = receiver.try_recv();
            assert!(page0_3.is_ok());

            // can't get a write lock while there are active read locks
            cache.request_lock(&0, &LockType::Write, &mut sender);
            let result = receiver.try_recv();
            assert!(result.is_err());

            // after drop, we can get the write lock
            ::std::mem::drop(page0_1);
            ::std::mem::drop(page0_2);
            ::std::mem::drop(page0_3);
            // we need to drop all references to another page, since the page
            // 3 request is still pending
            ::std::mem::drop(page1_1);

            // first writer succeeds; can't get a second concurrent writer
            cache.request_lock(&0, &LockType::Write, &mut sender3);
            assert_eq!(cache.reader_count.active(&0), 0);
            assert_eq!(cache.writer_count.active(&0), 1);
            let result = receiver3.try_recv();
            assert!(result.is_err());
            let page0_w1 = receiver.try_recv();
            assert!(page0_w1.is_ok());

            // can't get a concurrent read lock
            cache.request_lock(&0, &LockType::Read, &mut sender);
            let page0_4 = receiver.try_recv();
            assert!(page0_4.is_err());
        }
    }
}
