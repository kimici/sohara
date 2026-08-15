//! Recursive-descent parser for the S1 expression subset

use serde_json::Value;

use super::ast::{CmpOp, Expr};
use super::token::{tokenize, Sym, Token};
use super::ExprError;

/// Parse an expression string into an AST.
pub fn parse(input: &str) -> Result<Expr, ExprError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.parse_or()?;
    if parser.peek().is_some() {
        return Err(parser.error("unexpected trailing input"));
    }
    Ok(expr)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn error(&self, message: &str) -> ExprError {
        ExprError::Parse(format!("{message} at token {}", self.pos))
    }

    fn expect_sym(&mut self, sym: Sym) -> Result<(), ExprError> {
        match self.next() {
            Some(Token::Sym(actual)) if actual == sym => Ok(()),
            other => Err(ExprError::Parse(format!(
                "expected symbol {sym:?}, got {other:?}"
            ))),
        }
    }

    fn expect_ident(&mut self) -> Result<String, ExprError> {
        match self.next() {
            Some(Token::Ident(name)) => Ok(name),
            other => Err(ExprError::Parse(format!(
                "expected identifier, got {other:?}"
            ))),
        }
    }

    fn parse_or(&mut self) -> Result<Expr, ExprError> {
        let mut expr = self.parse_and()?;
        while self.peek() == Some(&Token::Ident("or".to_owned())) {
            self.next();
            let rhs = self.parse_and()?;
            expr = Expr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, ExprError> {
        let mut expr = self.parse_unary()?;
        while self.peek() == Some(&Token::Ident("and".to_owned())) {
            self.next();
            let rhs = self.parse_unary()?;
            expr = Expr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, ExprError> {
        if self.peek() == Some(&Token::Ident("not".to_owned())) {
            self.next();
            return Ok(Expr::Not(Box::new(self.parse_unary()?)));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Expr, ExprError> {
        let mut expr = self.parse_add()?;
        loop {
            let op = match self.peek() {
                Some(Token::Sym(Sym::EqEq)) => Some(CmpOp::Eq),
                Some(Token::Sym(Sym::NotEq)) => Some(CmpOp::Neq),
                Some(Token::Sym(Sym::Gt)) => Some(CmpOp::Gt),
                Some(Token::Sym(Sym::Gte)) => Some(CmpOp::Gte),
                Some(Token::Sym(Sym::Lt)) => Some(CmpOp::Lt),
                Some(Token::Sym(Sym::Lte)) => Some(CmpOp::Lte),
                _ => None,
            };
            if let Some(op) = op {
                self.next();
                let rhs = self.parse_add()?;
                expr = Expr::Cmp(op, Box::new(expr), Box::new(rhs));
                continue;
            }
            if self.peek() == Some(&Token::Ident("in".to_owned())) {
                self.next();
                let rhs = self.parse_add()?;
                expr = Expr::In(Box::new(expr), Box::new(rhs));
                continue;
            }
            return Ok(expr);
        }
    }

    fn parse_add(&mut self) -> Result<Expr, ExprError> {
        let mut expr = self.parse_mul()?;
        loop {
            let is_add = self.peek() == Some(&Token::Sym(Sym::Plus));
            let is_sub = self.peek() == Some(&Token::Sym(Sym::Minus));
            if !is_add && !is_sub {
                return Ok(expr);
            }
            self.next();
            let rhs = self.parse_mul()?;
            expr = if is_add {
                Expr::Add(Box::new(expr), Box::new(rhs))
            } else {
                Expr::Sub(Box::new(expr), Box::new(rhs))
            };
        }
    }

    fn parse_mul(&mut self) -> Result<Expr, ExprError> {
        let mut expr = self.parse_atom()?;
        loop {
            let op = match self.peek() {
                Some(Token::Sym(Sym::Star)) => "mul",
                Some(Token::Sym(Sym::Slash)) => "div",
                Some(Token::Sym(Sym::Percent)) => "rem",
                _ => return Ok(expr),
            };
            self.next();
            let rhs = self.parse_atom()?;
            expr = match op {
                "mul" => Expr::Mul(Box::new(expr), Box::new(rhs)),
                "div" => Expr::Div(Box::new(expr), Box::new(rhs)),
                _ => Expr::Rem(Box::new(expr), Box::new(rhs)),
            };
        }
    }

    fn parse_atom(&mut self) -> Result<Expr, ExprError> {
        match self.next() {
            Some(Token::Number(n)) => number_to_value(n),
            Some(Token::String(s)) => Ok(Expr::Lit(Value::String(s))),
            Some(Token::Ident(name)) => self.parse_ident_or_call(name),
            Some(Token::Sym(Sym::Dollar)) => self.parse_dollar_path(),
            Some(Token::Sym(Sym::LBracket)) => self.parse_list(),
            Some(Token::Sym(Sym::LParen)) => {
                let expr = self.parse_or()?;
                self.expect_sym(Sym::RParen)?;
                Ok(expr)
            }
            other => Err(ExprError::Parse(format!("unexpected token: {other:?}"))),
        }
    }

    fn parse_ident_or_call(&mut self, name: String) -> Result<Expr, ExprError> {
        match name.as_str() {
            "true" => return Ok(Expr::Lit(Value::Bool(true))),
            "false" => return Ok(Expr::Lit(Value::Bool(false))),
            "null" => return Ok(Expr::Lit(Value::Null)),
            _ => {}
        }
        if self.peek() == Some(&Token::Sym(Sym::LParen)) {
            self.next();
            return self.parse_call(name);
        }
        self.parse_ident_path(name)
    }

    fn parse_call(&mut self, name: String) -> Result<Expr, ExprError> {
        if name == "env" || name == "var" {
            return self.parse_name_call(name);
        }
        let mut args = Vec::new();
        if self.peek() != Some(&Token::Sym(Sym::RParen)) {
            loop {
                args.push(self.parse_or()?);
                if self.peek() == Some(&Token::Sym(Sym::Comma)) {
                    self.next();
                } else {
                    break;
                }
            }
        }
        self.expect_sym(Sym::RParen)?;
        Ok(Expr::Call(name, args))
    }

    fn parse_name_call(&mut self, name: String) -> Result<Expr, ExprError> {
        let arg = match self.next() {
            Some(Token::Ident(ident)) => Expr::Lit(Value::String(ident)),
            Some(Token::String(text)) => Expr::Lit(Value::String(text)),
            other => {
                return Err(ExprError::Parse(format!(
                    "{name}() expects a name or a string, got {other:?}"
                )));
            }
        };
        self.expect_sym(Sym::RParen)?;
        Ok(Expr::Call(name, vec![arg]))
    }

    fn parse_ident_path(&mut self, name: String) -> Result<Expr, ExprError> {
        let mut path = vec![name];
        while self.peek() == Some(&Token::Sym(Sym::Dot)) {
            self.next();
            path.push(self.expect_ident()?);
        }
        Ok(Expr::Path(path))
    }

    fn parse_dollar_path(&mut self) -> Result<Expr, ExprError> {
        self.expect_sym(Sym::Dot)?;
        let mut path = vec![self.expect_ident()?];
        while self.peek() == Some(&Token::Sym(Sym::Dot)) {
            self.next();
            path.push(self.expect_ident()?);
        }
        Ok(Expr::Path(path))
    }

    fn parse_list(&mut self) -> Result<Expr, ExprError> {
        let mut items = Vec::new();
        if self.peek() != Some(&Token::Sym(Sym::RBracket)) {
            loop {
                items.push(self.parse_or()?);
                if self.peek() == Some(&Token::Sym(Sym::Comma)) {
                    self.next();
                } else {
                    break;
                }
            }
        }
        self.expect_sym(Sym::RBracket)?;
        Ok(Expr::List(items))
    }
}

fn number_to_value(number: f64) -> Result<Expr, ExprError> {
    if !number.is_finite() {
        return Err(ExprError::Parse("number literal must be finite".to_owned()));
    }
    Ok(Expr::Lit(Value::from(number)))
}
