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
        let cursor = lex.to_cursor();
        let text = match self {
            Parsec::AttrValue { .. } => {
                let bads = ['\'', '"', '=', '<', '>', '`'];
                let quotes = ['"', '\''];
                let mut q = '"';
                let mut iter = text[cursor..].chars().enumerate();
                loop {
                    match iter.next() {
                        Some((0, ch)) if quotes.contains(&ch) => q = ch,
                        Some((0, _)) => break None,
                        Some((n, ch)) if ch == q && n > 0 => {
                            let text = String::from_iter(text[cursor..].chars().take(n));
                            break Some(text);
                        }
                        Some((_, ch))
                            if ch.is_ascii_whitespace() || bads.contains(&ch) =>
                        {
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
            Parsec::Comment { .. } if text[cursor..].len() <= 4 => None,
            Parsec::Comment { .. } => {
                if &text[cursor..(cursor + 4)] == "<!--" {
                    let mut end = "";
                    let mut iter = text[cursor..].chars().enumerate();
                    loop {
                        end = match iter.next() {
                            Some((_, '-')) if end == "" => "-",
                            Some((_, '-')) if end == "-" => "--",
                            Some((n, '>')) if end == "--" => {
                                let text =
                                    String::from_iter(text[cursor..].chars().take(n));
                                break Some(text);
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
            Parsec::Cdata { .. } if text[cursor..].len() <= 9 => None,
            Parsec::Cdata { .. } => {
                if &text[cursor..(cursor + 9)] == "<![CDATA[" {
                    let mut end = "";
                    let mut iter = text[cursor..].chars().enumerate();
                    loop {
                        end = match iter.next() {
                            Some((_, ']')) if end == "" => "]",
                            Some((_, ']')) if end == "]" => "]]",
                            Some((n, '>')) if end == "]]" => {
                                let text =
                                    String::from_iter(text[cursor..].chars().take(n));
                                break Some(text);
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
