/*
 * Copyright 2021 Oxide Computer Company
 */

use anyhow::{Result, bail};
use std::collections::HashMap;
use std::fs;
use std::io;
use io::ErrorKind::NotFound;
use io::Read;

fn read_file(p: &str) -> Result<Option<String>> {
    let f = match fs::File::open(p) {
        Ok(f) => f,
        Err(e) => {
            match e.kind() {
                NotFound => return Ok(None),
                _ => bail!("open \"{}\": {}", p, e),
            };
        }
    };
    let mut r = io::BufReader::new(f);
    let mut out = String::new();
    r.read_to_string(&mut out)?;
    Ok(Some(out))
}

fn read_lines(p: &str) -> Result<Option<Vec<String>>> {
    Ok(read_file(p)?.map(|data| {
        data.lines().map(|a| a.trim().to_string()).collect()
    }))
}

#[derive(Debug, Clone)]
pub enum MountOptionValue {
    Present,
    Value(String),
}

#[derive(Debug, Clone)]
pub struct Mount {
    pub special: String,
    pub mount_point: String,
    pub fstype: String,
    pub options: HashMap<String, MountOptionValue>,
    pub time: u64,
}

/**
 * Read mnttab(4) and produce a list of mounts.  The result is a list instead of
 * a dictionary as there may be more than one mount entry for a particular mount
 * point.
 */
pub fn mounts() -> Result<Vec<Mount>> {
    let mnttab = read_lines("/etc/mnttab")?.unwrap();
    let rows: Vec<Vec<_>> = mnttab.iter()
        .map(|m| { m.split('\t').collect() })
        .collect();

    assert!(rows.len() >= 5);

    let mut out = Vec::new();
    for r in rows {
        let mut options = HashMap::new();

        for p in r[3].split(',').collect::<Vec<&str>>() {
            let terms = p.splitn(2, '=').collect::<Vec<&str>>();

            let v = match terms.len() {
                1 => MountOptionValue::Present,
                2 => MountOptionValue::Value(terms[1].to_string()),
                n => panic!("{} terms?!", n),
            };

            options.insert(terms[0].to_string(), v);
        }

        out.push(Mount {
            special: r[0].to_string(),
            mount_point: r[1].to_string(),
            fstype: r[2].to_string(),
            options,
            time: r[4].parse().expect("mnttab time value"),
        });
    }

    Ok(out)
}
