use std::{fmt, result};

use crate::Result;

mod lex;
mod parsec;

/// Type position in (line_no, col_no) format within the text. Both `line_no`
/// and `col_no` start from 1.
pub struct Position(usize, usize);

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "@({},{})", self.0, self.1)
    }
}

pub use lex::Lex;
pub use parsec::{parse, Parsec, S};

/// Trait implemented by lexer types.
pub trait Lexer {
    /// Return the position of cursor in (line_no, col_no) format within the text.
    fn to_position(&self) -> Position;

    /// Return cursor position as character offset within the text.
    fn to_cursor(&self) -> usize;

    /// Move cursor by `n` characters.
    fn move_cursor(&mut self, n: usize);

    /// Return the remaining text as string.
    fn as_str(&self) -> &str;
}

pub trait Parser {
    /// Return the name of the parser.
    fn to_name(&self) -> String;

    /// Parse text encapsulated by `lex`.
    fn parse<L>(&self, lex: &mut L) -> Result<Option<Node>>
    where
        L: Lexer;
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
