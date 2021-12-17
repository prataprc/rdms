use regex::{Regex, RegexSet};

use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use crate::{html::Lex, Error, Result};

#[macro_export]
macro_rules! maybe_ws {
    () => {
        Parser::new_regx("WS", r#"\s+"#.to_string()).unwrap()
    };
}

#[macro_export]
macro_rules! atom {
    ($s:expr) => {
        Parser::new_atom($s, $s.to_string()).unwrap()
    };
    ($s:expr, $n:expr) => {
        Parser::new_atom($n, $s.to_string()).unwrap()
    };
}

#[macro_export]
macro_rules! re {
    ($s:expr) => {
        Parser::new_regx($s, $s.to_string()).unwrap()
    };
    ($s:expr, $n:expr) => {
        Parser::new_regx($n, $s.to_string()).unwrap()
    };
}

#[derive(Clone)]
pub enum Parser {
    Atom {
        name: String,
        tok: String,
        n: usize,
    },
    Regx {
        name: String,
        re: Regex,
    },
    AttrValue {
        name: String,
    },
    P {
        name: String,
        parser: Rc<Parser>,
    },
    And {
        name: String,
        parsers: Vec<Rc<Parser>>,
    },
    Or {
        name: String,
        parsers: Vec<Rc<Parser>>,
    },
    Maybe {
        parser: Rc<Parser>,
    },
    Kleene {
        name: String,
        parser: Rc<Parser>,
    },
    Many {
        name: String,
        parser: Rc<Parser>,
    },
    Ref {
        name: String,
        parser: RefCell<Weak<Parser>>,
    },
}

impl Parser {
    pub fn new_atom(name: &str, tok: String) -> Result<Parser> {
        let name = name.to_string();
        let n = tok.len();
        let p = Parser::Atom { name, tok, n };

        Ok(p)
    }

    pub fn new_regx(name: &str, expr: String) -> Result<Parser> {
        let name = name.to_string();
        let re = err_at!(InvalidInput, Regex::new(&expr), "bad re:{:?}", expr)?;
        let p = Parser::Regx { name, re };

        Ok(p)
    }

    pub fn new_ref(name: &str) -> Parser {
        let name = name.to_string();
        Parser::Ref {
            name,
            parser: RefCell::new(Weak::new()),
        }
    }

    pub fn new_attribute_value(name: &str) -> Parser {
        let name = name.to_string();
        Parser::AttrValue { name }
    }

    pub fn update_ref(&self, actual: Rc<Parser>) {
        match self {
            Parser::Ref { parser, .. } => {
                *parser.borrow_mut() = Rc::downgrade(&actual);
            }
            _ => unreachable!(),
        }
    }

    pub fn is_literal(&self) -> bool {
        match self {
            Parser::Atom { .. } => true,
            Parser::Regx { .. } => true,
            _ => false,
        }
    }

    pub fn to_pattern(&self) -> String {
        match self {
            Parser::Regx { re, .. } => re.to_string(),
            Parser::Atom { tok, .. } => tok.to_string(),
            _ => unreachable!(),
        }
    }

    pub fn kleene(self, name: &str) -> Parser {
        let name = name.to_string();
        Parser::Kleene {
            name,
            parser: Rc::new(self),
        }
    }

    pub fn many(self, name: &str) -> Parser {
        let name = name.to_string();
        Parser::Many {
            name,
            parser: Rc::new(self),
        }
    }

    pub fn maybe(self) -> Parser {
        Parser::Maybe {
            parser: Rc::new(self),
        }
    }

    pub fn and(self, next: Rc<Parser>) -> Parser {
        match self {
            Parser::And { name, mut parsers } => {
                parsers.push(next);
                Parser::And { name, parsers }
            }
            this => Parser::And {
                name: this.to_name(),
                parsers: vec![Rc::new(this), next],
            },
        }
    }

    pub fn or(self, or: Rc<Parser>) -> Parser {
        match self {
            Parser::Or { name, mut parsers } => {
                parsers.push(or);
                Parser::Or { name, parsers }
            }
            either => Parser::Or {
                name: either.to_name(),
                parsers: vec![Rc::new(either), or],
            },
        }
    }

    pub fn aas(self, name: &str) -> Parser {
        Parser::P {
            name: name.to_string(),
            parser: Rc::new(self),
        }
    }
}

impl Parser {
    pub fn parse(&self, lex: &mut Lex) -> Result<Option<Node>> {
        let node = match self {
            Parser::Atom { name, tok, n } if tok == &lex.as_str()[..*n] => {
                lex.move_cursor(*n);
                let node = Node::Token {
                    name: name.to_string(),
                    text: tok.to_string(),
                };
                Some(node)
            }
            Parser::Atom { .. } => None,
            Parser::Regx { name, re } => match re.find(lex.as_str()) {
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
            Parser::AttrValue { name } => match lex.quoted_attribute_value()? {
                Some(text) => {
                    lex.move_cursor(text.len());
                    let node = Node::Token {
                        name: name.to_string(),
                        text,
                    };
                    Some(node)
                }
                None => None,
            },
            Parser::P { name, parser } => match parser.parse(lex)? {
                Some(mut node) => {
                    node.set_name(name.as_str());
                    Some(node)
                }
                None => None,
            },
            Parser::And { name, parsers } => {
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
            Parser::Or { parsers, .. } if parsers.iter().all(|p| p.is_literal()) => {
                let re = RegexSet::new(parsers.iter().map(|p| p.to_pattern())).unwrap();
                match re.matches(lex.as_str()).iter().next() {
                    Some(n) => match parsers[n].parse(lex)? {
                        Some(node) => Some(node),
                        None => None,
                    },
                    None => None,
                }
            }
            Parser::Or { parsers, .. } => {
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
            Parser::Maybe { parser } => {
                let node = parser.parse(lex)?.map(Box::new);
                Some(Node::Maybe {
                    name: parser.to_name(),
                    node,
                })
            }
            Parser::Kleene { name, parser } => {
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
            Parser::Many { name, parser } => {
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
            Parser::Ref { name, parser } => {
                let parser = parser.borrow().upgrade().unwrap();
                match parser.parse(lex)? {
                    Some(mut node) => {
                        node.set_name(name.as_str());
                        Some(node)
                    }
                    None => None,
                }
            }
        };

        Ok(node)
    }
}

impl Parser {
    pub fn to_name(&self) -> String {
        match self {
            Parser::Atom { name, .. } => name.clone(),
            Parser::Regx { name, .. } => name.clone(),
            Parser::AttrValue { name, .. } => name.clone(),
            Parser::P { name, .. } => name.clone(),
            Parser::And { name, .. } => name.clone(),
            Parser::Or { name, .. } => name.clone(),
            Parser::Maybe { parser } => parser.to_name(),
            Parser::Kleene { name, .. } => name.clone(),
            Parser::Many { name, .. } => name.clone(),
            Parser::Ref { name, .. } => name.clone(),
        }
    }

    pub fn pretty_print(&self, prefix: &str) -> String {
        match self {
            Parser::Atom { name, tok, .. } => {
                format!("{}Atom#{}({})", prefix, name, tok)
            }
            Parser::Regx { name, re } => {
                format!("{}Regx#{}({:?})", prefix, name, re)
            }
            Parser::AttrValue { name } => {
                format!("{}AttrValue#{}", prefix, name)
            }
            Parser::P { name, parser } => {
                let mut s = format!("{}P#{}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                s.push_str(&parser.pretty_print(&prefix));
                s
            }
            Parser::And { name, parsers } => {
                let mut s = format!("{}And#{}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                for parser in parsers.iter() {
                    s.push_str(&parser.pretty_print(&prefix));
                }
                s
            }
            Parser::Or { name, parsers } => {
                let mut s = format!("{}Or#{}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                for parser in parsers.iter() {
                    s.push_str(&parser.pretty_print(&prefix));
                }
                s
            }
            Parser::Maybe { parser } => {
                let mut s = format!("{}Maybe", prefix);
                let prefix = prefix.to_string() + "  ";
                s.push_str(&parser.pretty_print(&prefix));
                s
            }
            Parser::Kleene { name, parser } => {
                let mut s = format!("{}Kleen#{}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                s.push_str(&parser.pretty_print(&prefix));
                s
            }
            Parser::Many { name, parser } => {
                let mut s = format!("{}Many#{}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                s.push_str(&parser.pretty_print(&prefix));
                s
            }
            Parser::Ref { name, parser } => {
                let mut s = format!("{}Ref#{}", prefix, name);
                let prefix = prefix.to_string() + "  ";
                s.push_str(&parser.borrow().upgrade().unwrap().pretty_print(&prefix));
                s
            }
        }
    }
}

pub struct S {
    lex: Lex,
    root: Node,
}

impl S {
    pub fn unwrap(self) -> (Lex, Node) {
        (self.lex, self.root)
    }
}

#[derive(Clone)]
pub enum Node {
    Maybe {
        name: String,
        node: Option<Box<Node>>,
    },
    Token {
        name: String,
        text: String,
    },
    Ws {
        name: String,
        text: String,
    },
    M {
        name: String,
        children: Vec<Node>,
    },
}

impl Node {
    pub fn set_name(&mut self, nm: &str) {
        let nm = nm.to_string();
        match self {
            Node::Maybe { name, .. } => *name = nm,
            Node::Token { name, .. } => *name = nm,
            Node::Ws { name, .. } => *name = nm,
            Node::M { name, .. } => *name = nm,
        }
    }

    pub fn to_text(&self) -> String {
        match self {
            Node::Maybe { node, .. } => {
                node.as_ref().map(|n| n.to_text()).unwrap_or("".to_string())
            }
            Node::Token { text, .. } => text.to_string(),
            Node::Ws { text, .. } => text.to_string(),
            Node::M { children, .. } => {
                let ss: Vec<String> = children.iter().map(|n| n.to_text()).collect();
                ss.join("")
            }
        }
    }

    pub fn pretty_print(&self, prefix: &str) -> String {
        match self {
            Node::Maybe { name, node } => {
                let mut s = format!("{}Maybe({:?})", prefix, name);
                let prefix = prefix.to_string() + "  ";
                node.as_ref().map(|n| s.push_str(&n.pretty_print(&prefix)));
                s
            }
            Node::Token { name, text } => {
                format!("{}Token({:?}, {})", prefix, name, text)
            }
            Node::Ws { name, text } => format!("{}Ws({:?}, {:?})", prefix, name, text),
            Node::M { name, children } => {
                let mut s = format!("{}M({:?})", prefix, name);
                let prefix = prefix.to_string() + "  ";
                for child in children.iter() {
                    s.push_str(&child.pretty_print(&prefix));
                }
                s
            }
        }
    }
}

pub fn parse(parser: Parser, mut lex: Lex) -> Result<Option<S>> {
    let s = match parser.parse(&mut lex)? {
        Some(root) => Some(S { lex, root }),
        None => None,
    };

    Ok(s)
}
