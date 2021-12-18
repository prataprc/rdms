use std::rc::Rc;

use crate::{and, atom, html, kleene, maybe, maybe_ws, or, parsec::Parsec, re, Result};

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

pub fn new_parser() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = and!(
        "DOC",
        maybe!(parse_doc_type()?),
        kleene!("ROOT_ELEMENTS", parse_element()?)
    );

    Ok(p)
}

fn parse_doc_type() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = and!(
        "DOC_TYPE",
        atom!("DOCTYPE_OPEN", "<!DOCTYPE"),
        maybe_ws!(),
        atom!("DOCTYPE_HTML", "html"),
        maybe!(re!("DOCTYPE_TEXT", r"[^>\s]+")),
        maybe_ws!(),
        atom!("DOCTYPE_CLOSE", ">")
    );

    Ok(p)
}

fn parse_element() -> Result<Rc<Parsec<html::Parsec>>> {
    let maybe_text1 = maybe!(re!("TEXT", r"[^<]*"));
    let maybe_text2 = maybe!(re!("TEXT", r"[^<]*"));

    let element_ref = Parsec::new_ref()?;

    let element_multi = and!(
        "ELEMENT",
        start_tag()?,
        kleene!(
            "TEXT_ELEMENTS",
            and!("TEXT_ELEMENT", maybe_text1, element_ref.clone())
        ),
        maybe_text2,
        end_tag()?
    );

    let element = or!("ELEMENT", element_inline()?, element_multi);

    element_ref.update_ref(element.clone());

    Ok(element)
}

fn element_inline() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = and!(
        "TAG_INLINE",
        atom!("TAG_INLINE", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        atom!("TAG_CLOSE", "/>")
    );

    Ok(p)
}

fn start_tag() -> Result<Rc<Parsec<html::Parsec>>> {
    let tag = and!(
        "START_TAG",
        atom!("TAG_OPEN", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        atom!("TAG_CLOSE", ">")
    );

    let attrs = kleene!(
        "ATTRIBUTES",
        and!("WS_ATTRIBUTE", maybe_ws!(), attribute()?)
    );

    let tag_attrs = and!(
        "START_TAG_ATTRS",
        atom!("TAG_OPEN", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        attrs,
        atom!("TAG_CLOSE", ">")
    );

    Ok(or!("TAG_START", tag, tag_attrs))
}

fn end_tag() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = and!(
        "END_TAG",
        atom!("TAG_OPEN", "</"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        maybe_ws!(),
        atom!("TAG_CLOSE", ">")
    );

    Ok(p)
}

fn attribute() -> Result<Rc<Parsec<html::Parsec>>> {
    let key = atom!(r"[^\s>]+", "ATTR_KEY_TOK");

    let attr_value = or!(
        "ATTR_VALUE",
        atom!("ATTR_VALUE_TOK", r#"[^\s'"=<>`]+"#),
        Parsec::with_parser(
            "ATTR_VALUE_STR",
            html::Parsec::new_attribute_value("ATTR_VALUE_STR")?
        )?
    );

    let key_value = and!(
        "ATTR_KEY_VALUE",
        atom!("ATTR_KEY", r"[^\s>]+"),
        maybe_ws!(),
        atom!("EQ", "="),
        maybe_ws!(),
        attr_value
    );

    Ok(or!("ATTRIBUTE", key, key_value))
}
