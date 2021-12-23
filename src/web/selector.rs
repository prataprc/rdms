use std::{iter::FromIterator, rc::Rc};

use crate::{and, atom, maybe, maybe_ws, or, re, ws};
use crate::{
    parsec::{self, Lexer, Node, Parser},
    Error, Result,
};

pub enum Parsec {
    QuotedStr { name: String },
}

impl Parsec {
    pub fn new_quoted_string(name: &str) -> Result<Self> {
        let p = Parsec::QuotedStr {
            name: name.to_string(),
        };

        Ok(p)
    }
}

impl Parser for Parsec {
    fn to_name(&self) -> String {
        match self {
            Parsec::QuotedStr { name } => name.clone(),
        }
    }
    fn parse<L>(&self, lex: &mut L) -> Result<Option<Node>>
    where
        L: Lexer,
    {
        let text = lex.as_str();
        let text = match self {
            Parsec::QuotedStr { .. } => {
                let quotes = ['"', '\''];
                let mut q = '"';
                let mut iter = text.chars().enumerate();
                loop {
                    match iter.next() {
                        Some((0, ch)) if quotes.contains(&ch) => q = ch,
                        Some((0, _)) => break None,
                        Some((n, ch)) if ch == q && n > 0 => {
                            let t = String::from_iter(text.chars().take(n + 1));
                            break Some(t);
                        }
                        Some((_, _)) => (),
                        None => err_at!(
                            InvalidInput,
                            msg: "unexpected EOF for attribute value {}", lex.to_position()
                        )?,
                    }
                }
            }
        };

        let node = text.map(|text| {
            lex.move_cursor(text.len());
            let node = Node::Token {
                name: self.to_name(),
                text,
            };
            node
        });

        Ok(node)
    }
}

/**************** GRAMMAR ******************/

pub fn new_parser() -> Result<Rc<parsec::Parsec<Parsec>>> {
    let selector_ref = parsec::Parsec::new_ref()?;

    let attr_key = re!("ATTR_KEY_TOK", r"[^\s/>=]+");
    let class = re!("ID", "[a-zA-Z][a-zA-Z0-9-]*");
    let id = re!("ID", "[a-zA-Z][a-zA-Z0-9-]*");
    let lang = re!("LANG", "[a-zA-Z][a-zA-Z0-9-]*");
    let num = re!("NUM", "[0-9]+");

    let open_sqr = atom!("OPEN_SQR", "[");
    let close_sqr = atom!("CLOSE_SQR", "]");
    let open_paran = atom!("OPEN_PARAN", "(");
    let close_paran = atom!("CLOSE_PARAN", ")");
    let eq = atom!("EQ", "=");
    let tilda = atom!("TILDA", "~");
    let caret = atom!("CARET", "^");
    let pipe = atom!("PIPE", "|");
    let dollar = atom!("DOLLAR", "$");
    let star = atom!("STAR", "*");
    let gt = atom!("GT", ">");
    let plus = atom!("PLUS", "+");
    let dot = atom!("DOT", ".");
    let qstr = parsec::Parsec::with_parser(
        "ATTR_VALUE",
        Parsec::new_quoted_string("ATTR_VALUE")?,
    )?;

    let any = atom!("ANY", "*");
    let e = re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*");
    let with_attr_key = and!(
        "WITH_ATTR_KEY",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let with_attr = and!(
        "WITH_ATTR",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let with_attr_contains = and!(
        "WITH_ATTR_CONTAINS",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        tilda.clone(),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let with_attr_begins = and!(
        "WITH_ATTR_BEGINS",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        caret.clone(),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let with_attr_ends = and!(
        "WITH_ATTR_ENDS",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        dollar.clone(),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let with_attr_has = and!(
        "WITH_ATTR_HAS",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        pipe.clone(),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let with_attr_start = and!(
        "WITH_ATTR_START",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        star.clone(),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let e_root = and!("E_ROOT", maybe!(e.clone()), atom!("ROOT", ":root"));
    let e_nth_child = and!(
        "E_NTH_CHILD",
        maybe!(e.clone()),
        atom!("NTH_CHILD", ":nth-child"),
        open_paran.clone(),
        maybe_ws!(),
        num.clone(),
        maybe_ws!(),
        close_paran.clone()
    );
    let e_nth_last_child = and!(
        "E_NTH_LAST_CHILD",
        maybe!(e.clone()),
        atom!("NTH_LAST_CHILD", ":nth-last-child"),
        open_paran.clone(),
        maybe_ws!(),
        num.clone(),
        maybe_ws!(),
        close_paran.clone()
    );
    let e_nth_type = and!(
        "E_NTH_TYPE",
        maybe!(e.clone()),
        atom!("NTH_TYPE", ":nth-of-type"),
        open_paran.clone(),
        maybe_ws!(),
        num.clone(),
        maybe_ws!(),
        close_paran.clone()
    );
    let e_nth_last_type = and!(
        "E_NTH_LAST_TYPE",
        maybe!(e.clone()),
        atom!("NTH_LAST_TYPE", ":nth-last-of-type"),
        open_paran.clone(),
        maybe_ws!(),
        num.clone(),
        maybe_ws!(),
        close_paran.clone()
    );
    let e_first_child = and!(
        "E_FIRST_CHILD",
        maybe!(e.clone()),
        atom!("FIRST_CHILD", ":first-child")
    );
    let e_last_child = and!(
        "E_LAST_CHILD",
        maybe!(e.clone()),
        atom!("LAST_CHILD", ":last-child")
    );
    let e_first_type = and!(
        "E_FIRST_TYPE",
        maybe!(e.clone()),
        atom!("FIRST_TYPE", ":first-of-type")
    );
    let e_last_type = and!(
        "E_LAST_TYPE",
        maybe!(e.clone()),
        atom!("LAST_TYPE", ":last-of-type")
    );
    let e_only_child = and!(
        "E_ONLY_CHILD",
        maybe!(e.clone()),
        atom!("ONLY_CHILD", ":only-child")
    );
    let e_only_type = and!(
        "E_ONLY_TYPE",
        maybe!(e.clone()),
        atom!("ONLY_TYPE", ":only-of-type")
    );
    let e_empty = and!("E_EMPTY", maybe!(e.clone()), atom!("EMPTY", ":empty"));
    let e_target = and!("E_TARGET", maybe!(e.clone()), atom!("TARGET", ":target"));
    let e_lang = and!(
        "E_LANG",
        maybe!(e.clone()),
        atom!("LANG", ":lang"),
        open_paran.clone(),
        maybe_ws!(),
        lang.clone(),
        maybe_ws!(),
        close_paran.clone()
    );

    let e_class = and!("E_CLASS", maybe!(e.clone()), dot.clone(), class.clone());
    let e_id = and!("E_ID", maybe!(e.clone()), dot.clone(), id.clone());

    let e_descendant = and!("E_DESCENDANT", selector_ref.clone(), ws!(), e.clone());
    let e_child = and!(
        "E_CHILD",
        selector_ref.clone(),
        maybe_ws!(),
        gt.clone(),
        maybe_ws!(),
        e.clone()
    );
    let e_precede = and!(
        "E_PRECEDE",
        selector_ref.clone(),
        maybe_ws!(),
        plus.clone(),
        maybe_ws!(),
        e.clone()
    );
    let e_order = and!(
        "E_ORDER",
        selector_ref.clone(),
        maybe_ws!(),
        tilda.clone(),
        maybe_ws!(),
        e.clone()
    );

    let p = or!(
        "SELECTOR",
        any,
        e,
        with_attr_key,
        with_attr,
        with_attr_contains,
        with_attr_begins,
        with_attr_ends,
        with_attr_has,
        with_attr_start,
        e_root,
        e_nth_child,
        e_nth_last_child,
        e_nth_type,
        e_nth_last_type,
        e_first_child,
        e_last_child,
        e_first_type,
        e_last_type,
        e_only_child,
        e_only_type,
        e_empty,
        e_target,
        e_lang,
        e_class,
        e_id,
        e_descendant,
        e_child,
        e_precede,
        e_order
    );

    Ok(p)
}

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
    ContainAttrVal {
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
    NthLastChild {
        tag: Option<String>,
        n: usize,
    },
    NthType {
        tag: Option<String>,
        n: usize,
    },
    NthLastType {
        tag: Option<String>,
        n: usize,
    },
    FirstChild {
        tag: Option<String>,
    },
    LastChild {
        tag: Option<String>,
    },
    FirstType {
        tag: Option<String>,
    },
    LastType {
        tag: Option<String>,
    },
    OnlyChild {
        tag: Option<String>,
    },
    OnlyType {
        tag: Option<String>,
    },
    Empty {
        tag: Option<String>,
    },
    Target {
        tag: Option<String>,
    },
    Lang {
        tag: Option<String>,
        lang: String,
    },
    Class {
        tag: Option<String>,
        class: String,
    },
    Id {
        tag: Option<String>,
        id: String,
    },
    Not {
        tag: Option<String>,
        selector: Box<Selector>,
    },
    Descendant {
        ancestor: Box<Selector>,
        descendant: Box<Selector>,
    },
    Child {
        parent: Box<Selector>,
        child: Box<Selector>,
    },
    Precede {
        first: Box<Selector>,
        second: Box<Selector>,
    },
    Order {
        before: Box<Selector>,
        after: Box<Selector>,
    },
}
