use std::iter::FromIterator;

use crate::{
    parsec::{Lexer, Node, Parser},
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
                let bads = ['\'', '"', '=', '<', '>', '`'];
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
                        Some((_, ch)) if bads.contains(&ch) => err_at!(
                            InvalidInput,
                            msg: "bad attribute value {}", lex.to_position()
                        )?,
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
