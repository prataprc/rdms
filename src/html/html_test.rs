use crate::parsec::Lex;

use super::*;

#[test]
fn test_html1() {
    let text = r"<!DOCTYPE html>";

    let mut lex = Lex::new(text.to_string());

    let parser = new_parser().unwrap();
    let node = parser.parse(&mut lex).unwrap();
    match node {
        Some(node) => node.pretty_print(""),
        None => unreachable!(),
    }
}
