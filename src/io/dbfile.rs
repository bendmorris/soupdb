use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use ::io::page::{Page, PageId, PAGE_SIZE};
use ::{Result, Error};

trait BinaryStruct: Sized {
    fn from_bytes(data: &[u8]) -> Self {
        unsafe {
            ::std::ptr::read(data.as_ptr() as *const _)
        }
    }

    fn to_bytes<'a>(&'a self) -> &'a [u8] {
        unsafe {
            ::std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                ::std::mem::size_of::<Self>(),
            )
        }
    }

    fn write_to_buf(&self, mut buf: &mut [u8]) {
        buf.write(self.to_bytes());
    }
}

/// A segment of metadata contained on every page.
#[repr(C)]
pub struct PageMetadata {
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

impl BinaryStruct for PageMetadata {}

/// A segment of DB metadata that follows the PageMetadata of the first page in
/// a DB file.
#[repr(C)]
pub struct DbMetadata {
    first_free_page: PageId,
    last_page: PageId,
}

impl BinaryStruct for DbMetadata {}

pub struct DbFile {
    name: String,
    meta: DbMetadata,
    handle: File,
}

impl DbFile {
    pub fn create(name: String, path: &Path) -> Result<DbFile> {
        let meta = DbMetadata {
            first_free_page: 1,
            last_page: 1,
        };
        let page_meta = PageMetadata {
            prev_page: 0,
            next_page: 0,
        };

        let buf_size = PAGE_SIZE * 2;
        let mut buf = Vec::with_capacity(buf_size as usize);
        for _ in 0 .. buf_size {
            buf.push(0);
        }

        let page_header_len = size_of::<PageMetadata>();
        let db_header_len = size_of::<DbMetadata>();
        page_meta.write_to_buf(&mut buf);
        meta.write_to_buf(&mut buf[page_header_len .. page_header_len + db_header_len]);
        page_meta.write_to_buf(&mut buf[PAGE_SIZE as usize .. (PAGE_SIZE * 2) as usize]);

        let mut file = File::create(path).unwrap();
        file.write(&buf);
        file.flush();

        Ok(DbFile {
            name,
            meta,
            handle: file,
        })
    }

    pub fn open(name: String, path: &Path) -> Result<DbFile> {
        let mut file = File::open(path).unwrap();
        let buf_size = size_of::<DbMetadata>();
        let mut buf = Vec::with_capacity(buf_size);
        for i in 0 .. buf_size {
            buf.push(0);
        }
        DbFile::read_page(&mut file, 0, &mut buf);
        let page_header_len = size_of::<PageMetadata>();
        let db_header_len = size_of::<DbMetadata>();
        let meta = DbMetadata::from_bytes(&buf[page_header_len .. page_header_len + db_header_len]);

        Ok(DbFile {
            name,
            meta,
            handle: file,
        })
    }

    fn read_page<R: Read + Seek>(handle: &mut R, page_index: u64, buffer: &mut [u8]) {
        handle.seek(SeekFrom::Start(page_index * PAGE_SIZE));
        handle.read_exact(buffer);
    }

    fn write_to_page<W: Write + Seek>(handle: &mut W, page_index: u64, bytes: &[u8]) {
        handle.seek(SeekFrom::Start(page_index * PAGE_SIZE));
        handle.write(bytes);
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
        let page = PageMetadata::from_bytes(&page_data);
        assert_eq!(page.prev_page, 10);
        assert_eq!(page.next_page, 20);
        let db = DbMetadata::from_bytes(&page_data[size_of::<PageMetadata>() .. page_data.len()]);
        assert_eq!(db.first_free_page, 11);
        assert_eq!(db.last_page, 21);
    }
}
