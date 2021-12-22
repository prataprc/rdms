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
    text[a..b].to_string()
}

pub fn new_parser() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = kleene!("ROOT_ITEMS", parse_item()?);

    Ok(p)
}

fn parse_item() -> Result<Rc<Parsec<html::Parsec>>> {
    let text = re!("TEXT", r"[^<]+");
    let comment = Parsec::with_parser("COMMENT", html::Parsec::new_comment("COMMENT")?)?;
    let cdata = Parsec::with_parser("CDATA", html::Parsec::new_cdata("CDATA")?)?;

    let item = or!(
        "OR_ITEM",
        text,
        tag_inline()?,
        tag_start()?,
        tag_end()?,
        doc_type()?,
        comment,
        cdata
    );

    Ok(item)
}

fn doc_type() -> Result<Rc<Parsec<html::Parsec>>> {
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

fn tag_inline() -> Result<Rc<Parsec<html::Parsec>>> {
    let attrs = kleene!(
        "ATTRIBUTES",
        and!("WS_ATTRIBUTE", maybe_ws!(), attribute()?)
    );

    let p = and!(
        "TAG_INLINE",
        atom!("TAG_OPEN", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        maybe!(attrs),
        maybe_ws!(),
        atom!("TAG_CLOSE", "/>")
    );

    Ok(p)
}

fn tag_start() -> Result<Rc<Parsec<html::Parsec>>> {
    let attrs = kleene!(
        "ATTRIBUTES",
        and!("WS_ATTRIBUTE", maybe_ws!(), attribute()?)
    );

    let p = and!(
        "TAG_START",
        atom!("TAG_OPEN", "<"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        maybe!(attrs),
        maybe_ws!(),
        atom!("TAG_CLOSE", ">")
    );

    Ok(p)
}

fn tag_end() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = and!(
        "TAG_END",
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
