use regex::{Regex, RegexSet};

use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use crate::{
    parsec::{Lexer, Node, Parser},
    Error, Result,
};

#[macro_export]
macro_rules! maybe_ws {
    () => {
        Parsec::new_regx("WS", r#"\s+"#.to_string()).unwrap()
    };
}

#[macro_export]
macro_rules! atom {
    ($s:expr) => {
        Parsec::new_atom($s, $s.to_string()).unwrap()
    };
    ($n:expr, $s:expr) => {
        Parsec::new_atom($n, $s.to_string()).unwrap()
    };
}

#[macro_export]
macro_rules! re {
    ($s:expr) => {
        Parsec::new_regx($s, $s.to_string()).unwrap()
    };
    ($n:expr, $s:expr) => {
        Parsec::new_regx($n, $s.to_string()).unwrap()
    };
}

#[macro_export]
macro_rules! kleene {
    ($parser:expr) => {{
        let p = Parsec::Kleene {
            name: $parser.to_name(),
            parser: $parser,
        };
        Rc::new(p)
    }};
    ($name:expr, $parser:expr) => {{
        let p = Parsec::Kleene {
            name: $name.to_string(),
            parser: $parser,
        };
        Rc::new(p)
    }};
}

#[macro_export]
macro_rules! many {
    ($parser:expr) => {{
        let p = Parsec::Many {
            name: $parser.to_name(),
            parser: $parser,
        };
        Rc::new(p)
    }};
    ($name:expr, $parser:expr) => {{
        let p = Parsec::Many {
            name: $name.to_string(),
            parser: $parser,
        };
        Rc::new(p)
    }};
}

#[macro_export]
macro_rules! maybe {
    ($parser:expr) => {{
        let p = Parsec::Maybe { parser: $parser };
        Rc::new(p)
    }};
}

#[macro_export]
macro_rules! and {
    ($name:expr, $($parser:expr),+) => {{
        let p = Parsec::And {
            name: $name.to_string(),
            parsers: vec![ $($parser),+ ],
        };
        Rc::new(p)
    }};
}

#[macro_export]
macro_rules! or {
    ($name:expr, $($parser:expr),+) => {{
        let p = Parsec::Or {
            name: $name.to_string(),
            parsers: vec![ $($parser),+ ],
        };
        Rc::new(p)
    }};
}

#[macro_export]
macro_rules! aas {
    ($name:expr, $parser:expr) => {{
        let p = Parsec::P {
            name: $name.to_string(),
            parser: $parser,
        };
        Rc::new(p)
    }};
}

#[derive(Clone)]
pub enum Parsec<P>
where
    P: Parser,
{
    Atom {
        name: String,
        tok: String,
        n: usize,
    },
    Regx {
        name: String,
        re: Regex,
    },
    Ext {
        name: String,
        parser: P,
    },
    P {
        name: String,
        parser: Rc<Self>,
    },
    And {
        name: String,
        parsers: Vec<Rc<Self>>,
    },
    Or {
        name: String,
        parsers: Vec<Rc<Self>>,
    },
    Maybe {
        parser: Rc<Self>,
    },
    Kleene {
        name: String,
        parser: Rc<Self>,
    },
    Many {
        name: String,
        parser: Rc<Self>,
    },
    Ref {
        parser: RefCell<Weak<Self>>,
    },
}

impl<P> Parsec<P>
where
    P: Parser,
{
    pub fn new_atom(name: &str, tok: String) -> Result<Rc<Self>> {
        let name = name.to_string();
        let n = tok.len();
        let p = Parsec::Atom { name, tok, n };

        Ok(Rc::new(p))
    }

    pub fn new_regx(name: &str, expr: String) -> Result<Rc<Self>> {
        let name = name.to_string();
        let re = err_at!(InvalidInput, Regex::new(&expr), "bad re:{:?}", expr)?;
        let p = Parsec::Regx { name, re };

        Ok(Rc::new(p))
    }

    pub fn new_ref() -> Result<Rc<Self>> {
        let p = Parsec::Ref {
            parser: RefCell::new(Weak::new()),
        };

        Ok(Rc::new(p))
    }

    pub fn with_parser(name: &str, parser: P) -> Result<Rc<Self>> {
        let name = name.to_string();
        let p = Parsec::Ext { name, parser };

        Ok(Rc::new(p))
    }

    pub fn update_ref(&self, actual: Rc<Self>) {
        match self {
            Parsec::Ref { parser } => {
                *parser.borrow_mut() = Rc::downgrade(&actual);
            }
            _ => unreachable!(),
        }
    }

    pub fn is_literal(&self) -> bool {
        match self {
            Parsec::Atom { .. } => true,
            Parsec::Regx { .. } => true,
            _ => false,
        }
    }

    pub fn to_pattern(&self) -> String {
        match self {
            Parsec::Regx { re, .. } => re.to_string(),
            Parsec::Atom { tok, .. } => tok.to_string(),
            _ => unreachable!(),
        }
    }
}

impl<P> Parsec<P>
where
    P: Parser,
{
    pub fn parse<L>(&self, lex: &mut L) -> Result<Option<Node>>
    where
        L: Lexer,
    {
        let node = match self {
            Parsec::Atom { name, tok, n } if tok == &lex.as_str()[..*n] => {
                lex.move_cursor(*n);
                let node = Node::Token {
                    name: name.to_string(),
                    text: tok.to_string(),
                };
                Some(node)
            }
            Parsec::Atom { .. } => None,
            Parsec::Regx { name, re } => match re.find(lex.as_str()) {
                Some(m) => {
                    let text = m.as_str().to_string();
                    lex.move_cursor(text.len());
                    let node = Node::Token {
                        name: name.to_string(),
                        text,
                    };
                    Some(node)
                }
                None => None,
            },
            Parsec::Ext { parser, .. } => parser.parse(lex)?,
            Parsec::P { name, parser } => match parser.parse(lex)? {
                Some(mut node) => {
                    node.set_name(name.as_str());
                    Some(node)
                }
                None => None,
            },
            Parsec::And { name, parsers } => {
                let mut children = vec![];
                let mut iter = parsers.iter();
                loop {
                    match iter.next() {
                        Some(parser) => match parser.parse(lex)? {
                            Some(node) => children.push(node),
                            None => break None,
                        },
                        None => {
                            let node = Node::M {
                                name: name.to_string(),
                                children,
                            };
                            break Some(node);
                        }
                    }
                }
            }
            Parsec::Or { parsers, .. } if parsers.iter().all(|p| p.is_literal()) => {
                let re = RegexSet::new(parsers.iter().map(|p| p.to_pattern())).unwrap();
                match re.matches(lex.as_str()).iter().next() {
                    Some(n) => match parsers[n].parse(lex)? {
                        Some(node) => Some(node),
                        None => None,
                    },
                    None => None,
                }
            }
            Parsec::Or { parsers, .. } => {
                let mut iter = parsers.iter();
                loop {
                    match iter.next() {
                        Some(parser) => match parser.parse(lex)? {
                            Some(node) => break Some(node),
                            None => (),
                        },
                        None => break None,
                    }
                }
            }
            Parsec::Maybe { parser } => {
                let node = parser.parse(lex)?.map(Box::new);
                Some(Node::Maybe {
                    name: parser.to_name(),
                    node,
                })
            }
            Parsec::Kleene { name, parser } => {
                let mut children = vec![];
                while let Some(node) = parser.parse(lex)? {
                    children.push(node)
                }
                let node = Node::M {
                    name: name.to_string(),
                    children,
                };
                Some(node)
            }
            Parsec::Many { name, parser } => {
                let mut children = vec![];
                while let Some(node) = parser.parse(lex)? {
                    children.push(node)
                }
                match children.len() {
                    0 => None,
                    _ => {
                        let node = Node::M {
                            name: name.to_string(),
                            children,
                        };
                        Some(node)
                    }
                }
            }
            Parsec::Ref { parser } => {
                let parser = parser.borrow().upgrade().unwrap();
                match parser.parse(lex)? {
                    Some(node) => Some(node),
                    None => None,
                }
            }
        };

        Ok(node)
    }
}

impl<P> Parsec<P>
where
    P: Parser,
{
    pub fn to_name(&self) -> String {
        match self {
            Parsec::Atom { name, .. } => name.clone(),
            Parsec::Regx { name, .. } => name.clone(),
            Parsec::Ext { name, .. } => name.clone(),
            Parsec::P { name, .. } => name.clone(),
            Parsec::And { name, .. } => name.clone(),
            Parsec::Or { name, .. } => name.clone(),
            Parsec::Maybe { parser } => parser.to_name(),
            Parsec::Kleene { name, .. } => name.clone(),
            Parsec::Many { name, .. } => name.clone(),
            Parsec::Ref { parser } => {
                format!("&{}", parser.borrow().upgrade().unwrap().to_name())
            }
        }
    }

    pub fn pretty_print(&self, prefix: &str) {
        match self {
            Parsec::Atom { name, tok, .. } => {
                println!("{}Atom#{:15}  {:?}", prefix, name, tok)
            }
            Parsec::Regx { name, re } => {
                println!("{}Regx#{:15}  {:?}", prefix, name, re.to_string())
            }
            Parsec::Ext { name, .. } => {
                println!("{}Ext#{:15}", prefix, name)
            }
            Parsec::P { name, parser } => {
                println!("{}P#{:15}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                parser.pretty_print(&prefix)
            }
            Parsec::And { name, parsers } => {
                println!("{}And#{:15}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                for parser in parsers.iter() {
                    parser.pretty_print(&prefix);
                }
            }
            Parsec::Or { name, parsers } => {
                println!("{}Or#{:15}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                for parser in parsers.iter() {
                    parser.pretty_print(&prefix);
                }
            }
            Parsec::Maybe { parser } => {
                println!("{}Maybe", prefix);
                let prefix = prefix.to_string() + "  ";
                parser.pretty_print(&prefix)
            }
            Parsec::Kleene { name, parser } => {
                println!("{}Kleene#{:15}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                parser.pretty_print(&prefix)
            }
            Parsec::Many { name, parser } => {
                println!("{}Many#{:15}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                parser.pretty_print(&prefix)
            }
            Parsec::Ref { parser } => {
                println!(
                    "{}Ref#{:15}",
                    prefix,
                    parser.borrow().upgrade().unwrap().to_name()
                )
            }
        }
    }
}

pub struct S<L> {
    lex: L,
    root: Node,
}

impl<L> S<L> {
    pub fn unwrap(self) -> (L, Node) {
        (self.lex, self.root)
    }
}

pub fn parse<P, L>(parser: Parsec<P>, mut lex: L) -> Result<Option<S<L>>>
where
    P: Parser,
    L: Lexer,
{
    let s = match parser.parse(&mut lex)? {
        Some(root) => Some(S { lex, root }),
        None => None,
    };

    Ok(s)
}
