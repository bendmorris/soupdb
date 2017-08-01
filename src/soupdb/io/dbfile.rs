use soupdb::io::page::{Page, PageId};

/// A segment of metadata contained on every page.
#[repr(C)]
struct PageMetadata {
    prev_page: PageId,
    next_page: PageId,
}

impl PageMetadata {
    pub fn from_page(page_data: &Page) -> PageMetadata {
        let meta: PageMetadata = unsafe {
            ::std::ptr::read(page_data.as_ptr() as *const _)
        };
        return meta;
    }
}

/// A segment of metadata that follows the PageMetadata of the first page in a
/// DB file.
#[repr(C)]
struct DbMetadata {
    first_free_page: PageId,
    last_page: PageId,
}

impl DbMetadata {
    pub fn from_page(page_data: &Page) -> DbMetadata {
        let meta: DbMetadata = unsafe {
            ::std::ptr::read((page_data.as_ptr() as *const PageMetadata).offset(1) as *const _)
        };
        return meta;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page() {
        let page_data = [
            0xa, 0, 0, 0, 0, 0, 0, 0, 0x14, 0, 0, 0, 0, 0, 0, 0,
            0xb, 0, 0, 0, 0, 0, 0, 0, 0x15, 0, 0, 0, 0, 0, 0, 0
        ];
        let page = PageMetadata::from_page(&page_data);
        assert_eq!(page.prev_page, 10);
        assert_eq!(page.next_page, 20);
        let db = DbMetadata::from_page(&page_data);
        assert_eq!(db.first_free_page, 11);
        assert_eq!(db.last_page, 21);
    }
}
