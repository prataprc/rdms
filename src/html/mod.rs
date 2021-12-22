mod dom;
mod grammar;
mod parser;

pub use dom::{Attribute, Doctype, Dom};
pub use grammar::{new_parser, prepare_text};
use parser::Parsec;

#[cfg(test)]
#[path = "html_test.rs"]
mod html_test;

use crate::{
    parsec::{Lexer, Node},
    Error, Result,
};

pub fn parse<L>(
    parser: &crate::parsec::Parsec<Parsec>,
    lex: &mut L,
) -> Result<Option<Node>>
where
    L: Lexer + Clone,
{
    match parser.parse(lex)? {
        Some(node) => Ok(Some(node)),
        None => {
            let pos = lex.to_position();
            let cur = lex.to_cursor();
            err_at!(InvalidInput, msg: "parse failed at {} cursor:{}", pos, cur)
        }
    }
}

pub fn parse_full<L>(
    parser: &crate::parsec::Parsec<Parsec>,
    lex: &mut L,
) -> Result<Option<Node>>
where
    L: Lexer + Clone,
{
    match parser.parse(lex)? {
        Some(node) if lex.as_str().len() == 0 => Ok(Some(node)),
        Some(_) => {
            let pos = lex.to_position();
            let cur = lex.to_cursor();
            err_at!(InvalidInput, msg: "partial parse till {} cursor:{}", pos, cur)
        }
        None => {
            let pos = lex.to_position();
            let cur = lex.to_cursor();
            err_at!(InvalidInput, msg: "parse failed at {} cursor:{}", pos, cur)
        }
    }
}
