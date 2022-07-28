/*
 * Copyright 2022 Oxide Computer Company
 */

use anyhow::{bail, Result};
use std::fmt::{Display, Write};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Package {
    name: String,
    publisher: Option<String>,
    version: Option<String>,
    date: Option<String>,
}

impl Display for Package {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pkg:/")?;
        if let Some(p) = &self.publisher {
            write!(f, "/{}/", p)?;
        }
        write!(f, "{}", self.name)?;
        if let Some(v) = &self.version {
            write!(f, "@{}", v)?;
            if let Some(d) = &self.date {
                write!(f, ":{}", d)?;
            }
        }
        Ok(())
    }
}

#[allow(dead_code)]
impl Package {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    pub fn publisher(&self) -> Option<&str> {
        self.publisher.as_deref()
    }

    pub fn date(&self) -> Option<&str> {
        self.date.as_deref()
    }

    pub fn new_bare(name: &str) -> Package {
        Package {
            name: name.to_string(),
            publisher: None,
            version: None,
            date: None,
        }
    }

    pub fn new_bare_version(name: &str, version: &str) -> Package {
        Package {
            name: name.to_string(),
            publisher: None,
            version: Some(version.to_string()),
            date: None,
        }
    }

    pub fn to_string_without_publisher(&self) -> String {
        let mut f = String::new();
        write!(f, "pkg:/{}", self.name).unwrap();
        if let Some(v) = &self.version {
            write!(f, "@{}", v).unwrap();
            if let Some(d) = &self.date {
                write!(f, ":{}", d).unwrap();
            }
        }
        f
    }

    /**
     * Parse an FMRI from a depend action.  Apparently partial package names are
     * assumed to be anchored at the publisher root.
     */
    pub fn parse_fmri(fmri: &str) -> Result<Package> {
        let (publisher, input) = if let Some(i) = fmri.strip_prefix("pkg:/") {
            /*
             * Check to see if we expect a publisher.
             */
            if let Some(i) = i.strip_prefix('/') {
                let mut p = String::new();
                let mut r = String::new();
                let mut inpub = true;
                for c in i.chars() {
                    if inpub {
                        if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                            p.push(c);
                        } else if c == '/' {
                            if p.is_empty() {
                                bail!("expected publisher in \"{}\"", fmri);
                            }
                            inpub = false;
                        } else {
                            bail!("expected \"{}\" in \"{}\"", c, fmri);
                        }
                    } else {
                        r.push(c);
                    }
                }
                (Some(p), r)
            } else {
                (None, i.into())
            }
        } else if let Some(i) = fmri.strip_prefix('/') {
            if i.starts_with('/') {
                bail!("unexpected publisher without pkg: in \"{}\"", fmri);
            }
            (None, i.into())
        } else {
            (None, fmri.into())
        };

        let t = input.split('@').collect::<Vec<_>>();
        let (name, version, date) = match t.as_slice() {
            [n] => (n.to_string(), None, None),
            [n, x] => {
                let t = x.split(':').collect::<Vec<_>>();
                match t.as_slice() {
                    [v] => (n.to_string(), Some(v.to_string()), None),
                    [v, d] => (
                        n.to_string(),
                        Some(v.to_string()),
                        Some(d.to_string()),
                    ),
                    _ => bail!("too much : in \"{}\"", fmri),
                }
            }
            _ => bail!("too much @ in \"{}\"", fmri),
        };

        Ok(Package {
            name,
            publisher,
            version,
            date,
        })
    }
}
