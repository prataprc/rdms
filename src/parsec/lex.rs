use crate::parsec::{Lexer, Position};

/// Lex type implementing a lexer compatible with rdms/parsec.
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
}
