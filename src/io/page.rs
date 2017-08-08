pub const PAGE_SIZE: u64 = 0x2000;

pub type Page = [u8];
pub type PageId = u64;
pub type ScopedPageId<'a> = (&'a str, PageId);
