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
        kleene!(
            "ROOT_ELEMENTS",
            and!("ROOT_ELEMENT", maybe_ws!(), parse_element()?)
        )
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
        start_tag_attrs()?,
        kleene!(
            "TEXT_ELEMENTS",
            and!("TEXT_ELEMENT", maybe_text1, element_ref.clone())
        ),
        maybe_text2,
        end_tag()?
    );

    let element_comment =
        Parsec::with_parser("COMMENT", html::Parsec::new_comment("COMMENT")?)?;
    let element_cdata = Parsec::with_parser("CDATA", html::Parsec::new_cdata("CDATA")?)?;

    let element = or!(
        "OR_ELEMENT",
        element_inline()?,
        element_inline_tag_attrs()?,
        element_multi,
        element_comment,
        element_cdata
    );

    element_ref.update_ref(element.clone());

    Ok(element)
}

fn element_inline() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = and!(
        "ELEMENT_INLINE",
        atom!("TAG_OPEN", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        atom!("TAG_CLOSE", "/>")
    );

    Ok(p)
}

fn element_inline_tag_attrs() -> Result<Rc<Parsec<html::Parsec>>> {
    let attrs = kleene!(
        "ATTRIBUTES",
        and!("WS_ATTRIBUTE", maybe_ws!(), attribute()?)
    );

    let p = and!(
        "ELEMENT_INLINE_TAG_ATTRS",
        atom!("TAG_OPEN", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        attrs,
        atom!("TAG_CLOSE", "/>")
    );

    Ok(p)
}

fn start_tag_attrs() -> Result<Rc<Parsec<html::Parsec>>> {
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

    Ok(or!("OR_TAG", tag, tag_attrs))
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
    let key = re!("ATTR_KEY_TOK", r"[^\s/>=]+");

    let attr_value = or!(
        "OR_ATTR_VALUE",
        re!("ATTR_VALUE_TOK", r#"[^\s'"=<>`]+"#),
        Parsec::with_parser(
            "ATTR_VALUE_STR",
            html::Parsec::new_attribute_value("HTML_ATTR_STR")?
        )?
    );

    let key_value = and!(
        "ATTR_KEY_VALUE",
        key.clone(),
        maybe_ws!(),
        atom!("EQ", "="),
        maybe_ws!(),
        attr_value
    );

    Ok(or!("OR_ATTR", key_value, key))
}
