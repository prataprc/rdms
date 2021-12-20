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

#[macro_export]
macro_rules! maybe_ws {
    () => {
        maybe!(Parsec::new_regx("WS", r#"\s+"#.to_string()).unwrap())
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

#[derive(Clone)]
pub enum Parsec<P>
where
    P: Parser,
{
    Atom {
        name: String,
        tok: String,
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
        let p = Parsec::Atom { name, tok };

        Ok(Rc::new(p))
    }

    pub fn new_regx(name: &str, expr: String) -> Result<Rc<Self>> {
        let name = name.to_string();
        let expr = "^".to_string() + &expr;
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
        L: Lexer + Clone,
    {
        #[cfg(feature = "debug")]
        self.debug_print();

        let mut saved_lex = lex.save();
        let node = match self {
            Parsec::Atom { name, tok } => {
                let n = tok.len();
                let text = {
                    let text = lex.as_str();
                    if text.len() >= n && tok == &text[..n] {
                        Some(text[..n].to_string())
                    } else {
                        None
                    }
                };

                text.map(|text| {
                    #[cfg(feature = "debug")]
                    println!("atom {} tok:{:?}", name, tok);

                    lex.move_cursor(text[..n].chars().collect::<Vec<char>>().len());
                    Node::Token {
                        name: name.to_string(),
                        text: tok.to_string(),
                    }
                })
            }
            Parsec::Regx { name, re } => match re.find(lex.as_str()) {
                Some(m) => {
                    let text = m.as_str().to_string();
                    lex.move_cursor(text.chars().collect::<Vec<char>>().len());
                    let node = Node::Token {
                        name: name.to_string(),
                        text,
                    };
                    Some(node)
                }
                None => None,
            },
            Parsec::Ext { parser, .. } => match parser.parse(lex) {
                Err(err) => {
                    lex.restore(saved_lex.clone());
                    Err(err)
                }
                res => res,
            }?,
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
                    saved_lex = lex.save();
                    match iter.next() {
                        Some(parser) => match parser.parse(lex) {
                            Ok(Some(node)) => children.push(node),
                            Ok(None) => {
                                lex.restore(saved_lex.clone());
                                err_at!(
                                    InvalidInput,
                                    msg: "and-parsec fail at cursor:{} coord:{}",
                                    lex.to_cursor(), lex.to_position()
                                )?
                            }
                            Err(err) => {
                                lex.restore(saved_lex.clone());
                                Err(err)?
                            }
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
            Parsec::Or { name: _n, parsers }
                if parsers.iter().all(|p| p.is_literal()) =>
            {
                let re = RegexSet::new(parsers.iter().map(|p| p.to_pattern())).unwrap();
                match re.matches(lex.as_str()).iter().next() {
                    Some(n) => match parsers[n].parse(lex) {
                        Ok(Some(node)) => Some(node),
                        Ok(None) => None,
                        Err(err) => Err(err)?,
                    },
                    None => {
                        #[cfg(feature = "debug")]
                        println!("Parsec::Or failed all alternatives {}", _n);

                        None
                    }
                }
            }
            Parsec::Or { name: _n, parsers } => {
                let mut iter = parsers.iter();
                let node = loop {
                    match iter.next() {
                        Some(parser) => {
                            #[cfg(feature = "debug")]
                            println!("Parsec::Or trying {}", parser.to_name());

                            match parser.parse(lex) {
                                Ok(Some(node)) => break Some(node),
                                Ok(None) => {
                                    lex.restore(saved_lex.clone());
                                }
                                Err(_) => {
                                    lex.restore(saved_lex.clone());
                                }
                            }
                        }
                        None => {
                            #[cfg(feature = "debug")]
                            println!("Parsec::Or failed all alternatives {}", _n);

                            break None;
                        }
                    }
                };

                #[cfg(feature = "debug")]
                node.as_ref().map(|n| println!("{:?}", n));

                node
            }
            Parsec::Maybe { parser } => {
                let child = match parser.parse(lex) {
                    Ok(Some(node)) => Some(Box::new(node)),
                    Ok(None) => None,
                    Err(_) => {
                        lex.restore(saved_lex.clone());
                        None
                    }
                };

                Some(Node::Maybe {
                    name: parser.to_name(),
                    child,
                })
            }
            Parsec::Kleene { name, parser } => {
                let mut children = vec![];

                loop {
                    saved_lex = lex.save();
                    match parser.parse(lex) {
                        Ok(Some(node)) => children.push(node),
                        Ok(None) => {
                            lex.restore(saved_lex.clone());
                            break;
                        }
                        Err(_) => {
                            lex.restore(saved_lex.clone());
                            break;
                        }
                    }
                }

                let node = Node::M {
                    name: name.to_string(),
                    children,
                };
                Some(node)
            }
            Parsec::Many { name, parser } => {
                let mut children = vec![];

                loop {
                    saved_lex = lex.save();
                    match parser.parse(lex) {
                        Ok(Some(node)) => children.push(node),
                        Ok(None) if children.len() < 1 => {
                            lex.restore(saved_lex.clone());
                            err_at!(
                                InvalidInput,
                                msg: "many-parsec fail at cursor:{} coord:{}",
                                lex.to_cursor(), lex.to_position()
                            )?
                        }
                        Ok(None) => {
                            lex.restore(saved_lex.clone());
                            break;
                        }
                        Err(_) if children.len() < 1 => {
                            lex.restore(saved_lex.clone());
                            err_at!(
                                InvalidInput,
                                msg: "many-parsec fail at cursor:{} coord:{}",
                                lex.to_cursor(), lex.to_position()
                            )?
                        }
                        Err(_) => {
                            lex.restore(saved_lex.clone());
                            break;
                        }
                    }
                }

                match children.len() {
                    0 => err_at!(Fatal, msg: "invalid many-parsec construction")?,
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
            Parsec::Atom { name, tok } => {
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

    #[cfg(feature = "debug")]
    fn debug_print(&self) {
        match self {
            Parsec::Atom { name, .. } => println!("Parsec::Atom {}", name),
            Parsec::Regx { name, .. } => println!("Parsec::Regx {}", name),
            Parsec::Ext { name, .. } => println!("Parsec::Ext {}", name),
            Parsec::P { name, .. } => println!("Parsec::P {}", name),
            Parsec::And { name, .. } => println!("Parsec::And {}", name),
            Parsec::Or { name, parsers } if parsers.iter().all(|p| p.is_literal()) => {
                println!("Parsec::Or-literal {}", name)
            }
            Parsec::Or { name, .. } => println!("Parsec::Or {}", name),
            Parsec::Maybe { parser } => println!("Parsec::Maybe {}", parser.to_name()),
            Parsec::Kleene { name, .. } => println!("Parsec::Kleene {}", name),
            Parsec::Many { name, .. } => println!("Parsec::Many {}", name),
            Parsec::Ref { parser } => {
                let parser = parser.borrow().upgrade().unwrap();
                println!("Parsec::Ref {}", parser.to_name())
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
