/*
 * Copyright 2024 Oxide Computer
 */

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};

use crate::expand::Expansion;

#[derive(Default, Debug)]
pub struct Features {
    features: HashMap<String, HashSet<String>>,
}

impl Features {
    pub fn with<N: AsRef<str>>(&self, name: N) -> bool {
        self.features.contains_key(name.as_ref())
    }

    pub fn get_single<N: AsRef<str>>(&self, name: N) -> Result<&str> {
        let n = name.as_ref();

        if let Some(set) = self.features.get(n) {
            if set.len() > 1 {
                bail!(
                    "feature {n:?} has multiple values and cannot expand here"
                );
            }
            assert!(!set.is_empty());

            if let Some(value) = set.iter().next().map(String::as_str) {
                return Ok(value);
            }
        }

        bail!("feature {n:?} is not defined");
    }

    pub fn get_multi<N: AsRef<str>>(
        &self,
        name: N,
    ) -> Result<&HashSet<String>> {
        let n = name.as_ref();

        if let Some(set) = self.get_multi_maybe(n) {
            Ok(set)
        } else {
            bail!("feature {n:?} is not defined");
        }
    }

    pub fn get_multi_maybe<N: AsRef<str>>(
        &self,
        name: N,
    ) -> Option<&HashSet<String>> {
        self.features.get(name.as_ref())
    }

    /**
     * Expand feature macros in an optional string, if present.  Only
     * single-value features will be expanded here; expanding a multi-value
     * feature will result in an error.
     */
    pub fn expando(&self, value: Option<&str>) -> Result<Option<String>> {
        value.map(|value| self.expand(value)).transpose()
    }

    /**
     * Expand feature macros in a string.  Only single-value features will be
     * expanded here; expanding a multi-value feature will result in an error.
     */
    pub fn expand(&self, value: &str) -> Result<String> {
        Expansion::parse(value)?.evaluate(self)
    }

    /**
     * Expand feature macros in a string.  Multi-value features are allowed
     * here, as the caller is expecting a potentially multi-valued or empty
     * result.
     */
    pub fn expandm(&self, value: &[String]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for v in value.iter() {
            out.extend(Expansion::parse(v)?.evaluate_multi(self)?);
        }
        Ok(out)
    }
}

#[derive(Default, Debug)]
pub struct FeaturesParser {
    features: HashMap<String, HashSet<String>>,
}

impl FeaturesParser {
    /**
     * Parse a feature specification directive and add it to the feature set.
     * Directive order is important, as a subsequent directive can reset or
     * replace the features specified by an earlier directive.  This allows
     * control programs to provide defaults and allow later stages to override
     * them.
     */
    pub fn specify<S: AsRef<str>>(&mut self, spec: S) -> Result<&mut Self> {
        let spec = spec.as_ref();

        #[derive(Debug, PartialEq, Eq)]
        enum State {
            Rest,
            Identifier,
            MaybeIdentifier,
            Equals,
            Value,
        }

        #[derive(Debug, PartialEq, Eq)]
        enum Action {
            Set,
            Add,
            Subtract,
            Reset,
        }

        let mut st = State::Rest;
        let mut name = String::new();
        let mut value = String::new();
        let mut act = Action::Set;
        for c in spec.chars() {
            match st {
                State::Rest => {
                    if c == '^' && act != Action::Reset {
                        /*
                         * This is a negated feature; i.e., -F ^blah.  Set the
                         * action and go back around so that we can get the
                         * identifier.
                         */
                        act = Action::Reset;
                        continue;
                    }

                    if !c.is_ascii_alphabetic() {
                        bail!("feature names must start with a letter");
                    }

                    name.push(c);
                    st = State::Identifier;
                }
                State::Identifier => {
                    if c == '-' {
                        /*
                         * A hyphen is allowed anywhere except at the end of a
                         * feature name.  Sit on this one until we know which it
                         * is.
                         */
                        st = State::MaybeIdentifier;
                        continue;
                    }

                    if c.is_ascii_alphanumeric() || c == '_' {
                        name.push(c);
                        continue;
                    }

                    if act == Action::Reset && (c == '+' || c == '=') {
                        bail!("negated features cannot have a value");
                    }

                    assert_eq!(act, Action::Set);

                    st = match c {
                        '+' => {
                            act = Action::Add;
                            State::Equals
                        }
                        '~' => {
                            act = Action::Subtract;
                            State::Equals
                        }
                        '=' => {
                            act = Action::Set;
                            State::Value
                        }
                        other => {
                            bail!("expected [+|-]= not {other:?}");
                        }
                    };
                }
                State::MaybeIdentifier => {
                    if c == '=' {
                        act = Action::Subtract;
                        st = State::Value;
                        continue;
                    }

                    /*
                     * Release the hyphen!
                     */
                    name.push('-');

                    if c.is_ascii_alphanumeric() || c == '_' {
                        name.push(c);
                        st = State::Identifier;
                        continue;
                    }

                    bail!("feature names cannot end with a hyphen");
                }
                State::Equals => {
                    if c != '=' {
                        bail!("expected = not {c:?}");
                    }

                    assert!(act == Action::Add || act == Action::Subtract);
                    st = State::Value;
                }
                State::Value => {
                    if c.is_ascii() && !c.is_ascii_control() {
                        value.push(c);
                    } else {
                        bail!("feature values must be printable ASCII");
                    }
                }
            }
        }

        /*
         * Make sure we end up in a valid terminal state.
         */
        match (st, act) {
            /*
             * This should be unreachable:
             */
            (State::Value, Action::Reset) => {
                bail!("invalid feature specification");
            }

            (State::MaybeIdentifier, _) => {
                bail!("feature names cannot end with a hyphen");
            }

            (State::Identifier, Action::Reset) => {
                assert!(!name.is_empty());
                assert!(value.is_empty());

                /*
                 * This is a negated feature; i.e., -F ^blah
                 */
                self.features.remove(&name);
            }

            (State::Identifier, Action::Set) => {
                assert!(!name.is_empty());
                assert!(value.is_empty());

                /*
                 * This is an enabled feature with no value; i.e., -F blah
                 * Just set it to "1", as if the user had passed: -F blah=1
                 */
                self.features.entry(name).or_default().insert("1".to_string());
            }

            (State::Value, Action::Add) => {
                assert!(!name.is_empty());
                assert!(!value.is_empty());

                /*
                 * Adding a feature value to an existing feature name turns it
                 * into a multi-valued feature.
                 */
                self.features.entry(name).or_default().insert(value);
            }

            (State::Value, Action::Subtract) => {
                assert!(!name.is_empty());
                assert!(!value.is_empty());

                /*
                 * Remove this specific feature value from the feature.
                 */
                let remove = if let Some(feat) = self.features.get_mut(&name) {
                    feat.remove(&value);
                    feat.is_empty()
                } else {
                    false
                };

                /*
                 * If it is the last feature value remaining for the feature
                 * name, remove the name as well.
                 */
                if remove {
                    self.features.remove(&name);
                }
            }

            (State::Value, Action::Set) => {
                assert!(!name.is_empty());
                if value.is_empty() {
                    bail!("expected a value after the =");
                }

                /*
                 * Setting a feature clears any existing values that may exist
                 * and makes the feature single-valued with the new value.
                 */
                self.features.remove(&name);
                self.features.entry(name).or_default().insert(value);
            }

            (State::Rest, _) | (State::Equals, _) | (State::Identifier, _) => {
                bail!("unexpected end of feature specification");
            }
        }

        Ok(self)
    }

    pub fn build(&mut self) -> Features {
        Features { features: self.features.clone() }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parser_empty() {
        let mut fp = FeaturesParser::default();
        let f = fp.build();
        assert!(f.features.is_empty());
        assert!(f.get_single("a").is_err());
        assert!(f.get_single("b").is_err());
        assert!(f.get_single("").is_err());
    }

    #[test]
    fn parser_single() {
        let mut fp = FeaturesParser::default();
        fp.specify("a").unwrap();
        let f = fp.build();
        assert_eq!(f.features.len(), 1);
        assert_eq!(f.get_single("a").unwrap(), "1");
        assert!(f.get_single("b").is_err());
        assert!(f.get_single("").is_err());
    }

    #[test]
    fn parser_single_with_value() {
        let mut fp = FeaturesParser::default();
        fp.specify("a=b").unwrap();
        let f = fp.build();
        assert_eq!(f.features.len(), 1);
        assert_eq!(f.get_single("a").unwrap(), "b");
        assert!(f.get_single("b").is_err());
    }

    #[test]
    fn parser_single_with_many_single_values() {
        let mut fp = FeaturesParser::default();
        fp.specify("a=b").unwrap();
        fp.specify("b=ccccccccccccccccc").unwrap();
        fp.specify("c=1").unwrap();
        fp.specify("d=eeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap();
        assert!(fp.specify("e=").is_err());
        let f = fp.build();
        assert_eq!(f.features.len(), 4);
        assert_eq!(f.get_single("a").unwrap(), "b");
        assert_eq!(f.get_single("b").unwrap(), "ccccccccccccccccc");
        assert_eq!(f.get_single("c").unwrap(), "1");
        assert_eq!(f.get_single("d").unwrap(), "eeeeeeeeeeeeeeeeeeeeeeeeeeee");
        assert!(f.get_single("e").is_err());
    }

    #[test]
    fn parser_single_with_replacement() {
        let mut fp = FeaturesParser::default();
        fp.specify("a=b").unwrap();
        fp.specify("a=c").unwrap();
        let f = fp.build();
        assert_eq!(f.features.len(), 1);
        assert_eq!(f.get_single("a").unwrap(), "c");
        assert!(f.get_single("b").is_err());
    }

    #[test]
    fn parser_single_with_negation() {
        let mut fp = FeaturesParser::default();
        fp.specify("^a").unwrap();
        fp.specify("a=b").unwrap();
        fp.specify("^a").unwrap();
        assert!(fp.specify("^a=x").is_err());
        let f = fp.build();
        assert!(f.features.is_empty());
        assert!(f.get_single("a").is_err());
        assert!(f.get_single("b").is_err());
    }

    #[test]
    fn parser_multi_but_one_value() {
        let mut fp = FeaturesParser::default();
        fp.specify("aaa+=a").unwrap();
        fp.specify("aaa+=b").unwrap();
        fp.specify("aaa+=c").unwrap();
        fp.specify("aaa+=c").unwrap();
        fp.specify("aaa-=a").unwrap();
        fp.specify("aaa-=b").unwrap();
        let f = fp.build();
        assert_eq!(f.features.len(), 1);
        assert_eq!(f.get_single("aaa").unwrap(), "c");
        assert!(f.get_single("b").is_err());

        let aaa: HashSet<String> = ["c".into()].into();
        assert_eq!(f.get_multi_maybe("aaa").unwrap(), &aaa);
    }

    #[test]
    fn parser_multi() {
        let mut fp = FeaturesParser::default();
        fp.specify("aaa+=a").unwrap();
        fp.specify("aaa+=b").unwrap();
        fp.specify("aaa+=c").unwrap();
        fp.specify("b").unwrap();
        fp.specify("cc=3").unwrap();
        fp.specify("dd=pkg:/some/thing/or/other:3=fine").unwrap();
        fp.specify("a").unwrap();
        fp.specify("a+=2").unwrap();
        fp.specify("a+=3").unwrap();
        fp.specify("a-=3").unwrap();
        fp.specify("a-=2").unwrap();
        fp.specify("a-=1").unwrap();
        fp.specify("f+=2").unwrap();
        fp.specify("f+=3").unwrap();
        fp.specify("^f").unwrap();
        let f = fp.build();
        assert_eq!(f.features.len(), 4);
        assert!(f.get_single("aaa").is_err());
        assert_eq!(f.get_single("b").unwrap(), "1");
        assert_eq!(f.get_single("cc").unwrap(), "3");
        assert_eq!(
            f.get_single("dd").unwrap(),
            "pkg:/some/thing/or/other:3=fine"
        );
        assert!(f.get_single("a").is_err());
        assert!(f.get_single("f").is_err());

        let aaa: HashSet<String> = ["a".into(), "b".into(), "c".into()].into();
        assert_eq!(f.get_multi_maybe("aaa").unwrap(), &aaa);

        assert!(f.get_multi_maybe("g").is_none());
        assert!(f.get_multi("g").is_err());
    }

    #[test]
    fn parser_no_trailing_hyphens() {
        let mut fp = FeaturesParser::default();
        assert!(fp.specify("my-beautiful-house=1").is_ok());
        assert!(fp.specify("my-beautiful-house+=0").is_ok());
        assert!(fp.specify("my-beautiful-house-=1").is_ok());
        assert!(fp.specify("my-beautiful-house-+=2").is_err());
        assert!(fp.specify("my-beautiful-house--=0").is_err());
        let f = fp.build();
        assert_eq!(f.features.len(), 1);
        assert_eq!(f.get_single("my-beautiful-house").unwrap(), "0");
    }
}
