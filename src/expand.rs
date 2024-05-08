/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{bail, Result};

use crate::features::Features;

pub struct Expansion {
    chunks: Vec<Chunk>,
}

enum Chunk {
    Char(char),
    Simple(String),
    IfLiteral(String, String),
    IfNotLiteral(String, String),
    Multi(String, bool),
}

fn is_feature_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/*
 * Current expansion forms:
 *
 *  ${feature?literal}      expand to "literal" if feature is enabled,
 *                          otherwise the empty string
 *
 *  ${feature!literal}      expand to "literal" if feature is NOT enabled,
 *                          otherwise the empty string
 *
 *  ${feature}              expand to "feature" if set, or error if not
 *
 * These expansions forms can only be used where multiple expanded values are
 * tolerated:
 *
 *  ${@feature}             expand to ["feature", ...] if set with one
 *                          or more values, or an empty array if not
 *
 *  ${@@feature}            expand to ["feature", ...] if set with one
 *                          or more values, or error if not
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

    let mut multi = false;
    let mut multi_required = false;

    loop {
        match s {
            State::Feature => match chars.next() {
                Some('?') => {
                    if multi || multi_required {
                        bail!("cannot use ? with @");
                    }
                    if feature.is_empty() {
                        bail!("empty feature unexpected");
                    }
                    s = State::Literal(true);
                }
                Some('!') => {
                    if multi || multi_required {
                        bail!("cannot use ! with @");
                    }
                    if feature.is_empty() {
                        bail!("empty feature unexpected");
                    }
                    s = State::Literal(false);
                }
                Some('@') if feature.is_empty() && !multi => {
                    multi = true;
                }
                Some('@') if feature.is_empty() && multi && !multi_required => {
                    multi_required = true;
                }
                Some(c) if is_feature_char(c) => feature.push(c),
                Some(c) => bail!("unexpected char in feature name: {:?}", c),
                None => {
                    if feature.is_empty() {
                        bail!("empty feature unexpected");
                    }
                    return Ok(if multi {
                        Chunk::Multi(feature, multi_required)
                    } else {
                        Chunk::Simple(feature)
                    });
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

    pub fn evaluate(&self, features: &Features) -> Result<String> {
        let mut out = String::new();

        for ch in self.chunks.iter() {
            match ch {
                Chunk::Char(c) => {
                    out.push(*c);
                }
                Chunk::Simple(f) => {
                    out.push_str(features.get_single(f)?);
                }
                Chunk::IfLiteral(f, l) => {
                    if features.with(f) {
                        out.push_str(l);
                    }
                }
                Chunk::IfNotLiteral(f, l) => {
                    if !features.with(f) {
                        out.push_str(l);
                    }
                }
                Chunk::Multi(f, _) => {
                    bail!("cannot expand multiple values of {f:?} here");
                }
            }
        }

        Ok(out)
    }

    pub fn evaluate_multi(&self, features: &Features) -> Result<Vec<String>> {
        let multi = self
            .chunks
            .iter()
            .filter(|ch| matches!(ch, Chunk::Multi(_, _)))
            .collect::<Vec<_>>();

        let mut multivals = match multi.as_slice() {
            [] => {
                /*
                 * If there are no multi-value expansions, we can just do a
                 * regular expansion.
                 */
                return Ok(vec![self.evaluate(features)?]);
            }
            [Chunk::Multi(name, true)] => {
                features.get_multi(name)?.iter().collect::<Vec<_>>()
            }
            [Chunk::Multi(name, false)] => features
                .get_multi_maybe(name)
                .map(|set| set.iter().collect::<Vec<_>>())
                .unwrap_or_default(),
            _ => {
                /*
                 * Expanding one multi-valued macro is enough for now.  If we
                 * allowed more than one, we'd have to do the cross product,
                 * etc.
                 */
                bail!("multiple @ or @@ expansions are not supported");
            }
        };
        multivals.sort();

        let mut outs = Vec::new();
        for mv in multivals {
            let mut out = String::new();

            for ch in self.chunks.iter() {
                match ch {
                    Chunk::Char(c) => {
                        out.push(*c);
                    }
                    Chunk::Simple(f) => {
                        out.push_str(features.get_single(f)?);
                    }
                    Chunk::IfLiteral(f, l) => {
                        if features.with(f) {
                            out.push_str(l);
                        }
                    }
                    Chunk::IfNotLiteral(f, l) => {
                        if !features.with(f) {
                            out.push_str(l);
                        }
                    }
                    Chunk::Multi(_, _) => {
                        /*
                         * This only works because we restrict ourselves to a
                         * single multi-value expansion, which we have already
                         * expanded in the outer loop.
                         */
                        out.push_str(mv);
                    }
                }
            }

            outs.push(out);
        }

        Ok(outs)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::features::FeaturesParser;

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
    fn expand_multi_maybe() {
        match expand("@variable").unwrap() {
            Chunk::Multi(name, required) => {
                assert_eq!(name, "variable");
                assert!(!required);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn expand_multi() {
        match expand("@@variable").unwrap() {
            Chunk::Multi(name, required) => {
                assert_eq!(name, "variable");
                assert!(required);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn expand_multi_errors() {
        assert!(expand("@@@variable").is_err());
        assert!(expand("@variable!a").is_err());
        assert!(expand("@variable!").is_err());
        assert!(expand("@variable?a").is_err());
    }

    #[test]
    fn expand_basic_empty() {
        assert!(expand("").is_err());
    }

    #[test]
    fn expansion_not_enabled() {
        let e = Expansion::parse("${false!not enabled}").unwrap();
        let out = e.evaluate(&Features::default()).unwrap();
        assert_eq!(out, "not enabled");
        let out = e.evaluate_multi(&Features::default()).unwrap();
        assert_eq!(out, vec!["not enabled".to_string()]);
    }

    #[test]
    fn expansion_not_enabled_empty() {
        let e = Expansion::parse("${false!}").unwrap();
        let out = e.evaluate(&Features::default()).unwrap();
        assert_eq!(out, "");
        let out = e.evaluate_multi(&Features::default()).unwrap();
        assert_eq!(out, vec!["".to_string()]);
    }

    #[test]
    fn expansion_no_multis_here() {
        let e = Expansion::parse("${@multi}").unwrap();
        assert!(e.evaluate(&Features::default()).is_err());
    }

    #[test]
    fn expansion_missing_multi_maybe() {
        let e = Expansion::parse("${@multi}").unwrap();
        let out = e.evaluate_multi(&Features::default()).unwrap();
        assert_eq!(out, Vec::<String>::new());
    }

    #[test]
    fn expansion_missing_multi() {
        let e = Expansion::parse("${@@multi}").unwrap();
        assert!(e.evaluate_multi(&Features::default()).is_err());
    }

    #[test]
    fn expansion_populated_multi() {
        let e = Expansion::parse("pfx:/${@@multi}:suffix").unwrap();
        let f = FeaturesParser::default()
            .specify("multi+=a2")
            .unwrap()
            .specify("multi+=b1")
            .unwrap()
            .specify("multi+=a1")
            .unwrap()
            .specify("multi+=b2")
            .unwrap()
            .build();
        let out = e.evaluate_multi(&f).unwrap();
        assert_eq!(
            out,
            vec![
                "pfx:/a1:suffix",
                "pfx:/a2:suffix",
                "pfx:/b1:suffix",
                "pfx:/b2:suffix",
            ]
        );
    }
}
