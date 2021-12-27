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
    let num = re!("NUM", "-?[0-9]+");

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
    let index = and!(
        "INDEX",
        open_paran.clone(),
        maybe_ws!(),
        num,
        maybe_ws!(),
        close_paran.clone()
    );

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
    let with_attr_as = and!(
        "WITH_ATTR_AS",
        maybe!(e.clone()),
        open_sqr.clone(),
        maybe_ws!(),
        attr_key.clone(),
        maybe_ws!(),
        maybe!(or!("AS_OP", tilda.clone(), caret, dollar, pipe, star)),
        eq.clone(),
        maybe_ws!(),
        qstr.clone(),
        maybe_ws!(),
        close_sqr.clone()
    );
    let colonizer = and!(
        "COLONIZER",
        maybe!(e.clone()),
        or!(
            "PICKER",
            atom!(":root"),
            atom!(":nth-child"),
            atom!(":nth-last-child"),
            atom!(":nth-of-type"),
            atom!(":nth-last-of-type"),
            atom!(":first-child"),
            atom!(":last-child"),
            atom!(":first-of-type"),
            atom!(":last-of-type"),
            atom!(":only-child"),
            atom!(":only-of-type"),
            atom!(":empty"),
            atom!(":target")
        ),
        maybe!(index)
    );
    let e_lang = and!(
        "E_LANG",
        maybe!(e.clone()),
        atom!("LANG", ":lang"),
        open_paran.clone(),
        maybe_ws!(),
        lang,
        maybe_ws!(),
        close_paran.clone()
    );

    let e_class = and!("E_CLASS", maybe!(e.clone()), dot.clone(), class);
    let e_id = and!("E_ID", maybe!(e.clone()), dot.clone(), id);

    let selector_unary = or!(
        "SELECTOR_UNARY",
        any,
        e,
        with_attr_key,
        with_attr_as,
        colonizer,
        e_lang,
        e_class,
        e_id
    );

    let e_descendant = and!(
        "E_DESCENDANT",
        selector_unary.clone(),
        ws!(),
        selector_ref.clone()
    );
    let e_child = and!(
        "E_CHILD",
        selector_unary.clone(),
        maybe_ws!(),
        gt,
        maybe_ws!(),
        selector_ref.clone()
    );
    let e_precede = and!(
        "E_PRECEDE",
        selector_unary.clone(),
        maybe_ws!(),
        plus,
        maybe_ws!(),
        selector_ref.clone()
    );
    let e_order = and!(
        "E_ORDER",
        selector_unary.clone(),
        maybe_ws!(),
        tilda,
        maybe_ws!(),
        selector_ref.clone()
    );

    let selector_binary =
        or!("SELECTOR_BINARY", e_descendant, e_child, e_precede, e_order);

    let selector = or!("SELECTOR", selector_binary, selector_unary);
    selector_ref.update_ref(selector.clone());

    Ok(selector)
}

pub enum Selector {
    Any,
    Tag {
        name: String,
    },
    WithAttr {
        tag: Option<String>,
        key: String,
    },
    AsAttrVal {
        tag: Option<String>,
        key: String,
        op: Option<String>,
        value: String,
    },
    Colonizer {
        tag: Option<String>,
        picker: String,
        n: Option<isize>,
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
    Descendant {
        ancestor: Box<Rc<Selector>>,
        descendant: Box<Rc<Selector>>,
    },
    Child {
        parent: Box<Rc<Selector>>,
        child: Box<Rc<Selector>>,
    },
    Precede {
        first: Box<Rc<Selector>>,
        second: Box<Rc<Selector>>,
    },
    Order {
        before: Box<Rc<Selector>>,
        after: Box<Rc<Selector>>,
    },
}

impl Selector {
    pub fn from_node(node: Node) -> Result<Rc<Selector>> {
        let selector = match node.to_name().as_str() {
            "ANY" => Selector::Any,
            "TAG_NAME" => {
                let name = node.into_text();
                Selector::Tag { name }
            }
            "WITH_ATTR_KEY" => {
                let mut children = node.into_children();
                let tag = children.remove(0).into_child().map(|n| n.into_text());
                let key = children.remove(2).into_text();
                Selector::WithAttr { tag, key }
            }
            "WITH_ATTR_AS" => {
                let mut children = node.into_children();
                let tag = children.remove(0).into_child().map(|n| n.into_text());
                let key = children.remove(2).into_text();
                let op = children.remove(3).into_child().map(|n| n.into_text());
                let value = children.remove(6).into_text();
                Selector::AsAttrVal {
                    tag,
                    key,
                    op,
                    value,
                }
            }
            "COLONIZER" => {
                let mut children = node.into_children();
                let tag = children.remove(0).into_child().map(|n| n.into_text());
                let picker = children.remove(0).to_name();
                let n: Option<isize> = match children.remove(0).into_child() {
                    Some(n) => {
                        let n = err_at!(
                            FailConvert,
                            n.into_children().remove(2).into_text().parse()
                        )?;
                        Some(n)
                    }
                    None => None,
                };
                Selector::Colonizer { tag, picker, n }
            }
            "E_LANG" => {
                let mut children = node.into_children();
                let tag = children.remove(0).into_child().map(|n| n.into_text());
                let lang = children.remove(3).into_text();
                Selector::Lang { tag, lang }
            }
            "E_CLASS" => {
                let mut children = node.into_children();
                let tag = children.remove(0).into_child().map(|n| n.into_text());
                let class = children.remove(1).into_text();
                Selector::Class { tag, class }
            }
            "E_ID" => {
                let mut children = node.into_children();
                let tag = children.remove(0).into_child().map(|n| n.into_text());
                let id = children.remove(1).into_text();
                Selector::Id { tag, id }
            }
            "E_DESCENDANT" => {
                let mut children = node.into_children();
                let ancestor = Box::new(Selector::from_node(children.remove(0))?);
                let descendant = Box::new(Selector::from_node(children.remove(1))?);
                Selector::Descendant {
                    ancestor,
                    descendant,
                }
            }
            "E_CHILD" => {
                let mut children = node.into_children();
                let parent = Box::new(Selector::from_node(children.remove(0))?);
                let child = Box::new(Selector::from_node(children.remove(3))?);
                Selector::Child { parent, child }
            }
            "E_PRECEDE" => {
                let mut children = node.into_children();
                let first = Box::new(Selector::from_node(children.remove(0))?);
                let second = Box::new(Selector::from_node(children.remove(3))?);
                Selector::Precede { first, second }
            }
            "E_ORDER" => {
                let mut children = node.into_children();
                let before = Box::new(Selector::from_node(children.remove(0))?);
                let after = Box::new(Selector::from_node(children.remove(3))?);
                Selector::Order { before, after }
            }
            _ => unreachable!(),
        };

        Ok(Rc::new(selector))
    }
}

pub fn parse<L>(parser: &parsec::Parsec<Parsec>, lex: &mut L) -> Result<Option<Node>>
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

pub fn parse_full<L>(parser: &parsec::Parsec<Parsec>, lex: &mut L) -> Result<Option<Node>>
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
