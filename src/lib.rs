#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_must_use)]
#![allow(unused_mut)]
#![allow(unused_variables)]

#[macro_use] extern crate nom;
extern crate byteorder;
extern crate glob;
extern crate lru_cache;

pub mod ast;
pub mod config;
pub mod db;
pub mod io;
pub mod model;

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
