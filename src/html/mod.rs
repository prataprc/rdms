mod dom;
mod grammar;
mod parser;

pub use dom::{Attribute, Doctype, Dom};
pub use grammar::{new_parser, prepare_text};
use parser::Parsec;

#[cfg(test)]
#[path = "html_test.rs"]
mod html_test;
