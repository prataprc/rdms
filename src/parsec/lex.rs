use crate::parsec::{Lexer, Position};

/// Lex type implementing a lexer compatible with rdms/parsec.
#[derive(Clone, Debug)]
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
}

impl Lexer for Lex {
    fn to_position(&self) -> Position {
        Position(self.row_no + 1, self.col_no + 1)
    }

    fn to_cursor(&self) -> usize {
        self.cursor
    }

    fn move_cursor(&mut self, n: usize) {
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

    fn as_str(&self) -> &str {
        &self.text[self.cursor..]
    }

    fn save(&self) -> Lex {
        #[cfg(feature = "debug")]
        println!(">>> save-lex @{}", self.to_position());

        Lex {
            text: String::default(),
            row_no: self.row_no,
            col_no: self.col_no,
            cursor: self.cursor,
        }
    }

    fn restore(&mut self, other: Self) {
        #[cfg(feature = "debug")]
        println!(">>> restore-lex @{}", other.to_position());

        self.row_no = other.row_no;
        self.col_no = other.col_no;
        self.cursor = other.cursor;
    }
}
