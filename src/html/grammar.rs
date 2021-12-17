use std::rc::Rc;

use crate::{atom, html::Parser, maybe_ws, re, Result};

pub fn prepare_text(text: String) -> String {
    // ASCII whitespace before the html element, at the start of the html element
    // and before the head element, will be dropped when the document is parsed;
    // ASCII whitespace after the html element will be parsed as if it were at the
    // end of the body element. Thus, ASCII whitespace around the document element
    // does not round-trip.
    let a: usize = text
        .chars()
        .take_while(|ch| ch.is_ascii_whitespace())
        .map(|ch| ch.len_utf8())
        .sum();
    let b: usize = text
        .chars()
        .rev()
        .take_while(|ch| ch.is_ascii_whitespace())
        .map(|ch| ch.len_utf8())
        .sum();
    let b = text.len() - b;
    text[a..=b].to_string()
}

pub fn new_parser() -> Result<Parser> {
    Ok(parse_doc_type()?
        .maybe()
        .and(Rc::new(parse_element()?.many("ROOT_ELEMENTS"))))
}

fn parse_doc_type() -> Result<Parser> {
    let p = atom!("<!DOCTYPE", "DOCTYPE")
        .and(Rc::new(maybe_ws!()))
        .and(Rc::new(atom!("html")))
        .and(Rc::new(re!(r"[^>\s]*")))
        .and(Rc::new(maybe_ws!()))
        .and(Rc::new(atom!(">")));

    Ok(p)
}

fn parse_element() -> Result<Parser> {
    let maybe_text1 = re!("TEXT", r"[^<]*").maybe();
    let maybe_text2 = re!("TEXT", r"[^<]*").maybe();

    let element_ref = Rc::new(Parser::new_ref("ELEMENT"));

    let element_multi = start_tag()?
        .and(Rc::new(
            maybe_text1
                .and(element_ref.clone())
                .aas("TEXT_ELEMENT")
                .kleene("TEXT_ELEMENTS"),
        ))
        .and(Rc::new(maybe_text2))
        .and(Rc::new(end_tag()?));

    let element = Rc::new(element_inline()?.or(Rc::new(element_multi)));

    element_ref.update_ref(element.clone());

    Ok(Rc::try_unwrap(element).ok().unwrap())
}

fn element_inline() -> Result<Parser> {
    let p = atom!("<", "TAG_INLINE")
        .and(Rc::new(atom!("[a-zA-Z][a-zA-Z0-9]*")))
        .and(Rc::new(atom!("/>")));

    Ok(p)
}

fn start_tag() -> Result<Parser> {
    let tag = atom!("<", "TAG")
        .and(Rc::new(atom!("[a-zA-Z][a-zA-Z0-9]*")))
        .and(Rc::new(atom!(">")));

    let attrs = maybe_ws!()
        .and(Rc::new(attributes()?))
        .aas("ATTRIBUTE")
        .kleene("ATTRIBUTES");

    let tag_attrs = atom!("<", "TAG_ATTRS")
        .and(Rc::new(atom!("[a-zA-Z][a-zA-Z0-9]*")))
        .and(Rc::new(attrs))
        .and(Rc::new(atom!(">")));

    Ok(tag.or(Rc::new(tag_attrs)))
}

fn end_tag() -> Result<Parser> {
    let p = atom!("</", "END_TAG")
        .and(Rc::new(atom!("[a-zA-Z][a-zA-Z0-9]*")))
        .and(Rc::new(maybe_ws!()))
        .and(Rc::new(atom!(">")));

    Ok(p)
}

fn attributes() -> Result<Parser> {
    let attr_values = atom!("ATTR_VALUE", r#"[^\s'"=<>`]+"#)
        .or(Rc::new(Parser::new_attribute_value("ATTR_VALUE_STR")));

    let key = atom!(r"[^\s>]+", "ATTR_KEY");

    let key_value = atom!(r"[^\s>]+", "ATTR_KEY_VALUE")
        .and(Rc::new(maybe_ws!()))
        .and(Rc::new(atom!("=")))
        .and(Rc::new(maybe_ws!()))
        .and(Rc::new(attr_values));

    Ok(key.or(Rc::new(key_value)))
}
