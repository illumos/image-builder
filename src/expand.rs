use std::collections::HashMap;

use anyhow::{bail, Result};

pub struct Expansion {
    chunks: Vec<Chunk>,
}

enum Chunk {
    Char(char),
    Simple(String),
    IfLiteral(String, String),
    IfNotLiteral(String, String),
}

fn is_feature_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/*
 * Current expansion forms:
 *
 *  ${feature?literal}      expand to "literal" if feature is enabled,
 *                          otherwise the empty string
 *  ${feature!literal}      expand to "literal" if feature is NOT enabled,
 *                          otherwise the empty string
 *  ${feature}              expand to "feature" if set, or error if not
 */
fn expand(expand: &str) -> Result<Chunk> {
    enum State {
        Feature,
        Literal(bool),
    }

    let mut s = State::Feature;
    let mut chars = expand.chars();
    let mut feature = String::new();
    let mut literal = String::new();

    loop {
        match s {
            State::Feature => match chars.next() {
                Some('?') => {
                    if feature.is_empty() {
                        bail!("empty feature unexpected");
                    }
                    s = State::Literal(true);
                }
                Some('!') => {
                    if feature.is_empty() {
                        bail!("empty feature unexpected");
                    }
                    s = State::Literal(false);
                }
                Some(c) if is_feature_char(c) => feature.push(c),
                Some(c) => bail!("unexpected char in feature name: {:?}", c),
                None => {
                    if feature.is_empty() {
                        bail!("empty feature unexpected");
                    }
                    return Ok(Chunk::Simple(feature));
                }
            },
            State::Literal(yes) => match chars.next() {
                Some(c) => literal.push(c),
                None if yes => return Ok(Chunk::IfLiteral(feature, literal)),
                None => return Ok(Chunk::IfNotLiteral(feature, literal)),
            },
        }
    }
}

impl Expansion {
    pub fn parse(template: &str) -> Result<Expansion> {
        enum State {
            Rest,
            Dollar,
            Expansion,
        }

        let mut s = State::Rest;
        let mut chars = template.chars();
        let mut chunks = Vec::new();
        let mut exp = String::new();

        loop {
            match s {
                State::Rest => match chars.next() {
                    Some('$') => {
                        s = State::Dollar;
                    }
                    Some(c) => {
                        chunks.push(Chunk::Char(c));
                    }
                    None => {
                        return Ok(Expansion { chunks });
                    }
                },
                State::Dollar => match chars.next() {
                    Some('$') => {
                        chunks.push(Chunk::Char('$'));
                        s = State::Rest;
                    }
                    Some('{') => {
                        s = State::Expansion;
                    }
                    Some(c) => {
                        bail!("expected $ or {{ after $, not {:?}", c);
                    }
                    None => {
                        bail!("unexpected end of string after $");
                    }
                },
                State::Expansion => match chars.next() {
                    Some('}') => {
                        chunks.push(expand(&exp)?);
                        exp.clear();
                        s = State::Rest;
                    }
                    Some('$') => {
                        bail!("no nesting in expansions for now");
                    }
                    Some(c) => {
                        exp.push(c);
                    }
                    None => {
                        bail!("unexpected end of string after ${{");
                    }
                },
            }
        }
    }

    pub fn evaluate(
        &self,
        features: &HashMap<String, String>,
    ) -> Result<String> {
        let mut out = String::new();

        for ch in self.chunks.iter() {
            match ch {
                Chunk::Char(c) => {
                    out.push(*c);
                }
                Chunk::Simple(f) => {
                    if let Some(v) = features.get(f) {
                        out.push_str(v);
                    } else {
                        bail!("feature {:?} not defined", f);
                    }
                }
                Chunk::IfLiteral(f, l) => {
                    if features.contains_key(f) {
                        out.push_str(l);
                    }
                }
                Chunk::IfNotLiteral(f, l) => {
                    if !features.contains_key(f) {
                        out.push_str(l);
                    }
                }
            }
        }

        Ok(out)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn expand_simple() {
        match expand("variable").unwrap() {
            Chunk::Simple(name) => assert_eq!(name, "variable"),
            _ => panic!(),
        }
    }

    #[test]
    fn expand_if_literal() {
        match expand("variable?literal").unwrap() {
            Chunk::IfLiteral(name, literal) => {
                assert_eq!(name, "variable");
                assert_eq!(literal, "literal");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn expand_if_not_literal() {
        match expand("variable!literal").unwrap() {
            Chunk::IfNotLiteral(name, literal) => {
                assert_eq!(name, "variable");
                assert_eq!(literal, "literal");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn expand_basic_empty() {
        assert!(expand("").is_err());
    }

    #[test]
    fn expansion_not_enabled() {
        let e = Expansion::parse("${false!not enabled}").unwrap();
        let out = e.evaluate(&HashMap::default()).unwrap();
        assert_eq!(out, "not enabled");
    }

    #[test]
    fn expansion_not_enabled_empty() {
        let e = Expansion::parse("${false!}").unwrap();
        let out = e.evaluate(&HashMap::default()).unwrap();
        assert_eq!(out, "");
    }
}
