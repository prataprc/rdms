use std::rc::Rc;

use crate::{and, atom, html, kleene, maybe, maybe_ws, or, parsec::Parsec, re, Result};

pub struct SelectorParsec;

enum Selector {
    Any,
    Tag {
        name: String,
    },
    WithAttr {
        tag: Option<String>,
        key: String,
    },
    EqAttrVal {
        tag: Option<String>,
        key: String,
        Value: String,
    },
    AsAttrVal {
        tag: Option<String>,
        key: String,
        Value: String,
    },
    BeginAttrVal {
        tag: Option<String>,
        key: String,
        Value: String,
    },
    EndAttrVal {
        tag: Option<String>,
        key: String,
        Value: String,
    },
    HasAttrVal {
        tag: Option<String>,
        key: String,
        Value: String,
    },
    StartAttrVal {
        tag: Option<String>,
        key: String,
        Value: String,
    },
    Root {
        tag: Option<String>,
    },
    NthChild {
        tag: Option<String>,
        n: usize,
    },
    NthChildR {
        tag: Option<String>,
        n: usize,
    },
    NthSibling {
        tag: Option<String>,
        n: usize,
    },
    NthSiblingR {
        tag: Option<String>,
        n: usize,
    },
    FirstChild {
        tag: Option<String>,
    },
    LastChild {
        tag: Option<String>,
    },
    OnlyChild {
        tag: Option<String>,
    }
    OnlySibling {
        tag: Option<String>,
    }
    Empty {
        tag: Option<String>,
    }
    Target {
        tag: Option<String>,
    }
    Lang {
        tag: Option<String>,
        lang: String,
    }
    Class {
        tag: Option<String>,
        class: String,
    }
    Id {
        tag: Option<String>,
        id: String,
    }
    Not {
        tag: Option<String>,
        selector: Box<Selector>
    }
    Descendant {
        ancestor: Box<Selector>,
        descendant: Box<Selector>,
    }
    Child {
        parent: Box<Selector>,
        child: Box<Selector>,
    }
    Preceed {
        first: Box<Selector>,
        second: Box<Selector>,
    }
    Order {
        before: Box<Selector>,
        after: Box<Selector>,
    }
}

pub fn new_selector_parser() -> Result<Rc<Parsec<html::Parsec>>> {
    let p = kleene!("ROOT_ITEMS", parse_item()?);

    Ok(p)
}

impl Parser for SelectorParsec {
    fn to_name(&self) -> String {
        "".to_string()
    }

    fn parse<L>(&self, lex: &mut L) -> Result<Option<Node>>
    where
        L: Lexer,
    {
        Ok(None)
    }
}

fn parse_pattern() -> Result<Rc<Parsec<SelectorParsec>>> {
    let any = atom!("ANY", "*");
    let tag_name = re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*");
    let tag_with_attr = and!(
        "TAG_WITH_ATTR",
        tag_name.clone(),
        atom!("OPEN_SQR", "["),
        re!("ATTR_KEY_TOK", r"[^\s/>=]+"),
        atom!("CLOSE_SQR", "]")
    );
    let with_attr = and!(
        "WITH_ATTR",
        atom!("OPEN_SQR", "["),
        re!("ATTR_KEY_TOK", r"[^\s/>=]+"),
        atom!("CLOSE_SQR", "]")
    );
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
