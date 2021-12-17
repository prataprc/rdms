mod dom;
mod grammar;
mod lex;
mod parsec;

pub use grammar::{new_parser, prepare_text};
use lex::Lex;
pub use parsec::{parse, Node, Parser, S};
