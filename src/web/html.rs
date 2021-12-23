use std::{
    cell::RefCell,
    iter::FromIterator,
    rc::{Rc, Weak},
};

use crate::{and, atom, kleene, maybe, maybe_ws, or, re};
use crate::{
    parsec::{self, Lexer, Node, Parser},
    Error, Result,
};

pub enum Parsec {
    AttrValue { name: String },
    Comment { name: String },
    Cdata { name: String },
}

impl Parsec {
    pub fn new_attribute_value(name: &str) -> Result<Self> {
        let p = Parsec::AttrValue {
            name: name.to_string(),
        };

        Ok(p)
    }

    pub fn new_cdata(name: &str) -> Result<Self> {
        let p = Parsec::Cdata {
            name: name.to_string(),
        };

        Ok(p)
    }

    pub fn new_comment(name: &str) -> Result<Self> {
        let p = Parsec::Comment {
            name: name.to_string(),
        };

        Ok(p)
    }
}

impl Parser for Parsec {
    fn to_name(&self) -> String {
        match self {
            Parsec::AttrValue { name } => name.clone(),
            Parsec::Comment { name } => name.clone(),
            Parsec::Cdata { name } => name.clone(),
        }
    }

    fn parse<L>(&self, lex: &mut L) -> Result<Option<Node>>
    where
        L: Lexer,
    {
        let text = lex.as_str();
        let text = match self {
            Parsec::AttrValue { .. } => {
                let bads = ['\'', '"', '<', '>', '`'];
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
                        Some((_, ch)) if bads.contains(&ch) => {
                            #[cfg(feature = "debug")]
                            println!("Contains bad attribute char {:?}", ch);

                            err_at!(
                                InvalidInput,
                                msg: "bad attribute value {}", lex.to_position()
                            )?
                        }
                        Some((_, _)) => (),
                        None => err_at!(
                            InvalidInput,
                            msg: "unexpected EOF for attribute value {}", lex.to_position()
                        )?,
                    }
                }
            }
            Parsec::Comment { .. } if text.len() <= 4 => None,
            Parsec::Comment { .. } => {
                // println!("comment *** {:?}", &text[..4]);
                if &text[..4] == "<!--" {
                    let mut end = "";
                    let mut iter = text.chars().enumerate();
                    loop {
                        end = match iter.next() {
                            Some((_, '-')) if end == "" => "-",
                            Some((_, '-')) if end == "-" => "--",
                            Some((n, '>')) if end == "--" => {
                                break Some(String::from_iter(text.chars().take(n + 1)));
                            }
                            Some((_, _)) => "",
                            None => err_at!(
                                InvalidInput,
                                msg: "unexpected EOF for attribute value {}",
                                lex.to_position()
                            )?,
                        }
                    }
                } else {
                    None
                }
            }
            Parsec::Cdata { .. } if text.len() <= 9 => None,
            Parsec::Cdata { .. } => {
                if &text[..9] == "<![CDATA[" {
                    let mut end = "";
                    let mut iter = text.chars().enumerate();
                    loop {
                        end = match iter.next() {
                            Some((_, ']')) if end == "" => "]",
                            Some((_, ']')) if end == "]" => "]]",
                            Some((n, '>')) if end == "]]" => {
                                break Some(String::from_iter(text.chars().take(n)));
                            }
                            Some((_, _)) => "",
                            None => err_at!(
                                InvalidInput,
                                msg: "unexpected EOF for attribute value {}",
                                lex.to_position()
                            )?,
                        }
                    }
                } else {
                    None
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
    let p = kleene!("ROOT_ITEMS", parse_item()?);

    Ok(p)
}

fn parse_item() -> Result<Rc<parsec::Parsec<Parsec>>> {
    let text = re!("TEXT", r"[^<]+");
    let comment =
        parsec::Parsec::with_parser("COMMENT", Parsec::new_comment("COMMENT")?)?;
    let cdata = parsec::Parsec::with_parser("CDATA", Parsec::new_cdata("CDATA")?)?;

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

fn doc_type() -> Result<Rc<parsec::Parsec<Parsec>>> {
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

fn tag_inline() -> Result<Rc<parsec::Parsec<Parsec>>> {
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

fn tag_start() -> Result<Rc<parsec::Parsec<Parsec>>> {
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

fn tag_end() -> Result<Rc<parsec::Parsec<Parsec>>> {
    let p = and!(
        "TAG_END",
        atom!("TAG_OPEN", "</"),
        re!("TAG_NAME", "[a-zA-Z][a-zA-Z0-9]*"),
        maybe_ws!(),
        atom!("TAG_CLOSE", ">")
    );

    Ok(p)
}

fn attribute() -> Result<Rc<parsec::Parsec<Parsec>>> {
    let key = re!("ATTR_KEY_TOK", r"[^\s/>=]+");

    let attr_value = or!(
        "OR_ATTR_VALUE",
        re!("ATTR_VALUE_TOK", r#"[^\s'"=<>`]+"#),
        parsec::Parsec::with_parser(
            "ATTR_VALUE_STR",
            Parsec::new_attribute_value("HTML_ATTR_STR")?
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

/********************* DOM **********************/

#[derive(Clone, Debug, PartialEq)]
pub struct Doctype {
    pub legacy: Option<String>,
}

impl ToString for Doctype {
    fn to_string(&self) -> String {
        format!(
            "{{ legacy: {} }}",
            self.legacy.as_ref().map(|x| x.as_str()).unwrap_or("")
        )
    }
}

impl Doctype {
    fn from_node(node: Node) -> Doctype {
        #[cfg(feature = "debug")]
        println!("Doctype::from_node {}", node.to_name());

        let val = Doctype {
            legacy: match node.into_children().remove(3).into_child() {
                Some(node) => Some(node.into_text()),
                None => None,
            },
        };
        val
    }

    fn pretty_string(&self, _oneline: bool) -> String {
        match &self.legacy {
            Some(legacy) if legacy.len() < 20 => format!("<Doctype {}>", legacy),
            Some(legacy) => format!("<Doctype {}..>", &legacy[..20]),
            None => format!("<Doctype>"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub key: String,
    pub value: Option<String>,
}

impl ToString for Attribute {
    fn to_string(&self) -> String {
        match &self.value {
            Some(value) => format!("{}={}", self.key, value),
            None => format!("{}", self.key),
        }
    }
}

impl Attribute {
    fn from_node(node: Node) -> Attribute {
        match node.to_name().as_str() {
            "WS_ATTRIBUTE" => Attribute::from_node(node.into_children().remove(1)),
            "ATTR_KEY_TOK" => {
                let attr = Attribute {
                    key: node.into_text(),
                    value: None,
                };
                attr
            }
            "ATTR_KEY_VALUE" => {
                let mut children = node.into_children();
                let attr = Attribute {
                    key: children.remove(0).into_text(),
                    value: Some(children.remove(3).into_text()),
                };
                attr
            }
            _ => unreachable!(),
        }
    }

    pub fn unwrap(self) -> (String, Option<String>) {
        let value = match self.value {
            Some(value) => {
                let (a, z) = (0, value.len().saturating_sub(1));
                let chars: Vec<char> = value.clone().chars().collect();
                let value = match (chars.first(), chars.last()) {
                    (Some('"'), Some('"')) => value[(a + 1)..z].to_string(),
                    (Some('\''), Some('\'')) => value[(a + 1)..z].to_string(),
                    (_, _) => value,
                };
                Some(value)
            }
            None => None,
        };
        (self.key, value)
    }

    fn pretty_string(&self, _oneline: bool) -> String {
        match &self.value {
            Some(value) if value.len() < 20 => format!("{}={}", self.key, value),
            Some(value) => format!("{}={}..", self.key, &value[..20]),
            None => format!("{}", self.key),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Dom {
    Doc {
        doc_type: Option<Doctype>,
        root_elements: Vec<Rc<Dom>>,
    },
    Tag {
        tag_name: String,
        attrs: Vec<Attribute>,
        tag_children: Vec<Rc<Dom>>,
        parent: RefCell<Weak<Dom>>,
    },
    TagEnd {
        tag_name: String,
        parent: RefCell<Weak<Dom>>,
    },
    Text {
        text: String,
        parent: RefCell<Weak<Dom>>,
    },
    Comment {
        text: String,
        parent: RefCell<Weak<Dom>>,
    },
}

impl PartialEq for Dom {
    fn eq(&self, other: &Dom) -> bool {
        use Dom::{Comment, Doc, Tag, TagEnd, Text};

        match (self, other) {
            (
                Doc {
                    doc_type,
                    root_elements,
                },
                Doc {
                    doc_type: d,
                    root_elements: r,
                },
            ) => {
                doc_type == d
                    && root_elements.len() == r.len()
                    && root_elements.iter().zip(r.iter()).all(|(a, b)| a == b)
            }
            (
                Tag {
                    tag_name,
                    attrs,
                    tag_children,
                    ..
                },
                Tag {
                    tag_name: t,
                    attrs: a,
                    tag_children: c,
                    ..
                },
            ) => tag_name == t && attrs == a && tag_children == c,
            (TagEnd { tag_name, .. }, TagEnd { tag_name: t, .. }) => tag_name == t,
            (Text { text, .. }, Text { text: t, .. }) => text == t,
            (Comment { text, .. }, Comment { text: t, .. }) => text == t,
            _ => false,
        }
    }
}

impl Dom {
    pub fn from_node(node: Node) -> Option<Rc<Dom>> {
        #[cfg(feature = "debug")]
        println!("Dom for node {}", node.to_name());
        #[cfg(feature = "debug")]
        assert_eq!(node.to_name().as_str(), "ROOT_ITEMS");

        let mut items = node.into_children();

        let doc_type = match items.len() {
            0 => None,
            _ if items[0].to_name().as_str() == "DOC_TYPE" => {
                Some(Doctype::from_node(items.remove(0)))
            }
            _ => None,
        };

        let mut root_elements = vec![];
        while items.len() > 0 {
            match Dom::build_doms(&mut items) {
                Some(doms) => root_elements.extend_from_slice(&doms),
                None => (),
            }
        }
        let dom = Rc::new(Dom::Doc {
            doc_type,
            root_elements,
        });
        Some(dom)
    }

    fn build_doms(items: &mut Vec<Node>) -> Option<Vec<Rc<Dom>>> {
        #[cfg(feature = "debug")]
        println!(
            "build_doms: items:{} {:?}",
            items.len(),
            items.first().map(|n| n.to_name())
        );

        let node = items.remove(0);
        match node.to_name().as_str() {
            "TAG_INLINE" => {
                let mut children = node.into_children();
                let tag_name = children.remove(1).into_text();
                let attrs: Vec<Attribute> = match children.remove(1).into_child() {
                    Some(node) => node
                        .into_children()
                        .into_iter()
                        .filter_map(|n| Some(Attribute::from_node(n)))
                        .collect(),
                    None => vec![],
                };

                let dom = Rc::new(Dom::Tag {
                    tag_name,
                    attrs,
                    tag_children: Vec::default(),
                    parent: RefCell::new(Weak::new()),
                });
                Some(vec![dom])
            }
            "TAG_START" => {
                let mut children = node.into_children();
                let tag_name = children.remove(1).into_text();
                let attrs: Vec<Attribute> = match children.remove(1).into_child() {
                    Some(node) => node
                        .into_children()
                        .into_iter()
                        .filter_map(|n| Some(Attribute::from_node(n)))
                        .collect(),
                    None => vec![],
                };

                let mut tag_children = vec![];
                let doms = Dom::build_children(&tag_name, items, &mut tag_children);
                #[cfg(feature = "debug")]
                println!(
                    "build_children: tag:{} children:{}, doms:{:?}",
                    tag_name,
                    tag_children.len(),
                    doms.as_ref().map(|x| x.len())
                );

                let dom = Rc::new(Dom::Tag {
                    tag_name,
                    attrs,
                    tag_children,
                    parent: RefCell::new(Weak::new()),
                });

                // Set parent for each dom.
                let parent = Rc::downgrade(&dom);
                match dom.as_ref() {
                    Dom::Tag { tag_children, .. } => {
                        for x in tag_children.iter() {
                            x.set_parent(parent.clone())
                        }
                    }
                    _ => unreachable!(),
                }

                // wire up doms.
                match doms {
                    Some(mut doms) => {
                        doms.insert(0, dom);
                        Some(doms)
                    }
                    None => Some(vec![dom]),
                }
            }
            "TAG_END" => {
                let tag_name = node.into_children().remove(1).into_text();
                let dom = Rc::new(Dom::TagEnd {
                    tag_name,
                    parent: RefCell::new(Weak::new()),
                });
                Some(vec![dom])
            }
            "TEXT" => {
                let dom = Rc::new(Dom::Text {
                    text: node.into_text(),
                    parent: RefCell::new(Weak::new()),
                });
                Some(vec![dom])
            }
            "COMMENT" => {
                let dom = Rc::new(Dom::Comment {
                    text: node.into_text(),
                    parent: RefCell::new(Weak::new()),
                });
                Some(vec![dom])
            }
            "CDATA" => unimplemented!(),
            name => panic!("{}", name),
        }
    }

    fn build_children(
        tname: &str,
        items: &mut Vec<Node>,
        children: &mut Vec<Rc<Dom>>,
    ) -> Option<Vec<Rc<Dom>>> {
        #[cfg(feature = "debug")]
        println!("build_children-enter: tag:{} items:{}", tname, items.len(),);

        while items.len() > 0 {
            if let Some(doms) = Dom::build_doms(items) {
                let mut iter = doms.into_iter();
                loop {
                    match iter.next() {
                        Some(dom) => match Rc::try_unwrap(dom).unwrap() {
                            Dom::TagEnd { tag_name, .. } if &tag_name == tname => {
                                return None;
                            }
                            dom @ Dom::TagEnd { .. } => {
                                children.push(Rc::new(dom));
                                return Some(children.drain(..).collect());
                            }
                            dom => children.push(Rc::new(dom)),
                        },
                        None => break,
                    }
                }
            }
        }

        let doms: Vec<Rc<Dom>> = children.drain(..).collect();
        Some(doms)
    }
}

impl Dom {
    pub fn set_parent(&self, par: Weak<Dom>) {
        use Dom::{Comment, Doc, Tag, TagEnd, Text};
        match self {
            Doc { .. } => (),
            Text { parent, .. } => *parent.borrow_mut() = par,
            Comment { parent, .. } => *parent.borrow_mut() = par,
            Tag { parent, .. } => *parent.borrow_mut() = par,
            TagEnd { parent, .. } => *parent.borrow_mut() = par,
        }
    }

    pub fn pretty_print(&self, prefix: &str, oneline: bool) {
        match self {
            Dom::Doc {
                doc_type,
                root_elements,
            } => {
                match doc_type {
                    Some(dt) => println!("{}{}", prefix, dt.pretty_string(oneline)),
                    None => (),
                }
                let prefix = prefix.to_string() + "  ";
                root_elements
                    .iter()
                    .for_each(|dom| dom.pretty_print(&prefix, oneline));
            }
            Dom::Tag {
                tag_name,
                attrs,
                tag_children,
                ..
            } => {
                if attrs.is_empty() {
                    println!("{}<{}>", prefix, tag_name);
                } else {
                    let attrs = attrs
                        .iter()
                        .map(|a| a.pretty_string(oneline))
                        .collect::<Vec<String>>()
                        .join(" ");
                    println!("{}<{} {}>", prefix, tag_name, attrs);
                };
                let prefix = prefix.to_string() + "  ";
                tag_children
                    .iter()
                    .for_each(|t| t.pretty_print(&prefix, oneline));
            }
            Dom::Text { text, .. } if text.trim().is_empty() => (),
            Dom::Text { text, .. } => match text.lines().next() {
                Some(text) if text.len() < 20 => println!("{}{}", prefix, text),
                Some(text) => println!("{}{}", prefix, &text[..20]),
                None => (),
            },
            Dom::Comment { text, .. } => match text.lines().next() {
                Some(text) if text.len() < 20 => println!("{}<Comment {}>", prefix, text),
                Some(text) => println!("{}<Comment {}>", prefix, &text[..20]),
                None => println!("{}<Comment>", prefix),
            },
            Dom::TagEnd { tag_name, .. } => println!("{}</{}>", prefix, tag_name),
        }
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

#[cfg(test)]
#[path = "html_test.rs"]
mod html_test;
