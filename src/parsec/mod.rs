//! Module implement parser-combinator library.

use std::{fmt, result};

use crate::Result;

mod lex;
mod parsec;

/// Type position in (line_no, col_no) format within the text. Both `line_no`
/// and `col_no` start from 1.
pub struct Position(usize, usize);

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "({},{})", self.0, self.1)
    }
}

pub use lex::Lex;
pub use parsec::{Parsec, S};

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

    /// Save current lexer state, typically a shallow clone for later `restore`.
    fn save(&self) -> Self;

    /// Update lexer state with saved lexer state.
    fn restore(&mut self, other: Self);
}

pub trait Parser {
    /// Return the name of the parser.
    fn to_name(&self) -> String;

    /// Parse text encapsulated by `lex`.
    fn parse<L>(&self, lex: &mut L) -> Result<Option<Node>>
    where
        L: Lexer;
}

#[derive(Clone, Debug)]
pub enum Node {
    Maybe {
        name: String,
        child: Option<Box<Node>>,
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

impl ToString for Node {
    fn to_string(&self) -> String {
        match self {
            Node::Maybe { child, .. } => child
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or("".to_string()),
            Node::Token { text, .. } => text.to_string(),
            Node::Ws { text, .. } => text.to_string(),
            Node::M { children, .. } => {
                let ss: Vec<String> = children.iter().map(|n| n.to_string()).collect();
                ss.join("")
            }
        }
    }
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

    pub fn into_child(self) -> Option<Node> {
        match self {
            Node::Maybe { child, .. } => child.map(|x| *x),
            _ => unreachable!(),
        }
    }

    pub fn into_children(self) -> Vec<Node> {
        match self {
            Node::M { children, .. } => children,
            _ => unreachable!(),
        }
    }

    pub fn into_text(self) -> String {
        match self {
            Node::Token { text, .. } => text,
            Node::Ws { text, .. } => text,
            _ => unreachable!(),
        }
    }

    pub fn to_name(&self) -> String {
        match self {
            Node::Maybe { name, .. } => name.clone(),
            Node::Token { name, .. } => name.clone(),
            Node::Ws { name, .. } => name.clone(),
            Node::M { name, .. } => name.clone(),
        }
    }

    pub fn pretty_print(&self, prefix: &str) {
        match self {
            Node::Maybe { name, child } if child.is_some() => {
                println!("{}Maybe#{} ok", prefix, name);
                let prefix = prefix.to_string() + "  ";
                child.as_ref().map(|n| n.pretty_print(&prefix));
            }
            Node::Maybe { name, .. } => println!("{}Maybe#{}", prefix, name),
            Node::Token { name, text } => {
                println!("{}Token#{} {:?}", prefix, name, text)
            }
            Node::Ws { name, text } => println!("{}Ws#{} {:?}", prefix, name, text),
            Node::M { name, children } => {
                println!("{}M#{} children:{}", prefix, name, children.len());
                let prefix = prefix.to_string() + "  ";
                for child in children.iter() {
                    child.pretty_print(&prefix);
                }
            }
        }
    }
}
