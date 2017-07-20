use std::result;

#[derive(Debug, PartialEq)]
pub enum Error {
    NotYetImplemented,
    TypeError(String),
    IoError(String),
    ParseError(String),
    Custom(String),
}

pub type Result<T> = result::Result<T, Error>;
