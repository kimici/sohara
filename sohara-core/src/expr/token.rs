//! Tokenizer for the S1 expression subset

use super::ExprError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    Number(f64),
    String(String),
    Sym(Sym),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sym {
    Dollar,
    Dot,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    EqEq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Tokenize an expression string.
pub fn tokenize(input: &str) -> Result<Vec<Token>, ExprError> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let (token, next) = next_token(&chars, i)?;
        tokens.push(token);
        i = next;
    }
    Ok(tokens)
}

fn next_token(chars: &[char], i: usize) -> Result<(Token, usize), ExprError> {
    let c = chars[i];
    if let Some(sym) = symbol(c) {
        return Ok((Token::Sym(sym), i + 1));
    }
    match c {
        '=' | '!' | '>' | '<' => two_char_op(chars, i, c),
        '"' | '\'' => {
            let (value, next) = read_string(chars, i, c)?;
            Ok((Token::String(value), next))
        }
        _ if c.is_ascii_digit() => {
            let (value, next) = read_number(chars, i)?;
            Ok((Token::Number(value), next))
        }
        _ if c.is_ascii_alphabetic() || c == '_' => {
            let (name, next) = read_ident(chars, i);
            Ok((Token::Ident(name), next))
        }
        _ => Err(ExprError::Parse(format!("unexpected character '{c}'"))),
    }
}

const fn symbol(c: char) -> Option<Sym> {
    match c {
        '$' => Some(Sym::Dollar),
        '.' => Some(Sym::Dot),
        '(' => Some(Sym::LParen),
        ')' => Some(Sym::RParen),
        '[' => Some(Sym::LBracket),
        ']' => Some(Sym::RBracket),
        ',' => Some(Sym::Comma),
        '+' => Some(Sym::Plus),
        '-' => Some(Sym::Minus),
        '*' => Some(Sym::Star),
        '/' => Some(Sym::Slash),
        '%' => Some(Sym::Percent),
        _ => None,
    }
}

fn two_char_op(chars: &[char], i: usize, c: char) -> Result<(Token, usize), ExprError> {
    let two = chars.get(i + 1) == Some(&'=');
    let sym = match (c, two) {
        ('=', true) => Sym::EqEq,
        ('=', false) => return Err(ExprError::Parse("expected '=='".to_owned())),
        ('!', true) => Sym::NotEq,
        ('!', false) => return Err(ExprError::Parse("expected '!='".to_owned())),
        ('>', true) => Sym::Gte,
        ('>', false) => Sym::Gt,
        (_, true) => Sym::Lte,
        (_, false) => Sym::Lt,
    };
    Ok((Token::Sym(sym), i + usize::from(two) + 1))
}

fn read_string(chars: &[char], start: usize, quote: char) -> Result<(String, usize), ExprError> {
    let mut value = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == '\\' {
            let escaped = *chars
                .get(i + 1)
                .ok_or_else(|| ExprError::Parse("bad escape".to_owned()))?;
            value.push(escaped);
            i += 2;
        } else if c == quote {
            return Ok((value, i + 1));
        } else {
            value.push(c);
            i += 1;
        }
    }
    Err(ExprError::Parse("unterminated string".to_owned()))
}

fn read_number(chars: &[char], start: usize) -> Result<(f64, usize), ExprError> {
    let mut text = String::new();
    let mut i = start;
    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
        text.push(chars[i]);
        i += 1;
    }
    let value: f64 = text
        .parse()
        .map_err(|_| ExprError::Parse(format!("invalid number '{text}'")))?;
    Ok((value, i))
}

fn read_ident(chars: &[char], start: usize) -> (String, usize) {
    let mut name = String::new();
    let mut i = start;
    while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
        name.push(chars[i]);
        i += 1;
    }
    (name, i)
}
