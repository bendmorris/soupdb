pub mod ast;
pub mod config;
pub mod db;
pub mod io;
pub mod model;
pub mod value;

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
