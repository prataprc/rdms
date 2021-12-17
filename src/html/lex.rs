use std::{fmt, iter::FromIterator, result};

use crate::{Error, Result};

pub struct Position(usize, usize); // (line_no, col_no) both start from 1.

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "@({},{})", self.0, self.1)
    }
}

pub struct Lex {
    text: String,
    row_no: usize, // start from ZERO
    col_no: usize, // start from ZERO
    cursor: usize, // start from ZERO
}

impl Lex {
    pub fn new(text: String) -> Lex {
        Lex {
            text,
            row_no: 0,
            col_no: 0,
            cursor: 0,
        }
    }

    pub fn to_position(&self) -> Position {
        Position(self.row_no + 1, self.col_no + 1)
    }

    pub fn quoted_attribute_value(&self) -> Result<Option<String>> {
        let bads = ['\'', '"', '=', '<', '>', '`'];
        let quotes = ['"', '\''];
        let mut q = '"';
        let mut iter = self.text[self.cursor..].chars().enumerate();
        loop {
            match iter.next() {
                Some((0, ch)) if quotes.contains(&ch) => q = ch,
                Some((0, _)) => break Ok(None),
                Some((n, ch)) if ch == q && n > 0 => {
                    let text =
                        String::from_iter(self.text[self.cursor..].chars().take(n));
                    break Ok(Some(text));
                }
                Some((_, ch)) if ch.is_ascii_whitespace() || bads.contains(&ch) => {
                    err_at!(
                        InvalidInput, msg: "bad attribute value {}", self.to_position()
                    )?;
                }
                Some((_, _)) => (),
                None => err_at!(
                    InvalidInput,
                    msg: "unexpected EOF for attribute value {}", self.to_position()
                )?,
            }
        }
    }

    pub fn comment(&self) -> Result<Option<String>> {
        if self.text[self.cursor..].len() <= 4 {
            return Ok(None);
        }

        if &self.text[self.cursor..(self.cursor + 4)] == "<!--" {
            let mut end = "";
            let mut iter = self.text[self.cursor..].chars().enumerate();
            loop {
                end = match iter.next() {
                    Some((_, '-')) if end == "" => "-",
                    Some((_, '-')) if end == "-" => "--",
                    Some((n, '>')) if end == "--" => {
                        let text =
                            String::from_iter(self.text[self.cursor..].chars().take(n));
                        break Ok(Some(text));
                    }
                    Some((_, _)) => "",
                    None => err_at!(
                        InvalidInput,
                        msg: "unexpected EOF for attribute value {}", self.to_position()
                    )?,
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn cdata(&self) -> Result<Option<String>> {
        if self.text[self.cursor..].len() <= 9 {
            return Ok(None);
        }

        if &self.text[self.cursor..(self.cursor + 9)] == "<![CDATA[" {
            let mut end = "";
            let mut iter = self.text[self.cursor..].chars().enumerate();
            loop {
                end = match iter.next() {
                    Some((_, ']')) if end == "" => "]",
                    Some((_, ']')) if end == "]" => "]]",
                    Some((n, '>')) if end == "]]" => {
                        let text =
                            String::from_iter(self.text[self.cursor..].chars().take(n));
                        break Ok(Some(text));
                    }
                    Some((_, _)) => "",
                    None => err_at!(
                        InvalidInput,
                        msg: "unexpected EOF for attribute value {}", self.to_position()
                    )?,
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn move_cursor(&mut self, n: usize) {
        let r = self.cursor..(self.cursor + n);
        for ch in self.text[r].chars() {
            match ch {
                '\n' => {
                    self.row_no += 1;
                    self.col_no = 0;
                }
                _ => self.col_no += 1,
            }
        }

        self.cursor += n;
    }

    pub fn as_str(&self) -> &str {
        &self.text[self.cursor..]
    }
}
