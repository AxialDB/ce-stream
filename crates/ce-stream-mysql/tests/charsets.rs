//! Each decoder against what MySQL's own `CONVERT` makes of every byte sequence.
//! The tables come from `scripts/charset-tables/dump-mysql-charsets.sh`; a
//! decoder is admitted only with no mismatch here.
//!
//! ```text
//! CE_STREAM_CHARSET_TABLES=/path/to/tables cargo test -p ce-stream-mysql --test charsets -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;

use ce_stream_mysql::charset::{admitted, decode};

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
#[ignore = "needs the dumped tables"]
fn decoders_against_mysql() {
    let Ok(dir) = std::env::var("CE_STREAM_CHARSET_TABLES") else {
        eprintln!("CE_STREAM_CHARSET_TABLES not set");
        return;
    };
    // charset -> (checked, undefined in MySQL, no decoder, different, first differences)
    let mut seen: BTreeMap<String, (u32, u32, u32, u32, Vec<String>)> = BTreeMap::new();
    for file in ["single.txt", "double.txt"] {
        let Ok(text) = std::fs::read_to_string(format!("{dir}/{file}")) else {
            continue;
        };
        for line in text.lines() {
            let mut fields = line.split('\t');
            let (Some(cs), Some(bytes), Some(utf8)) = (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            let entry = seen.entry(cs.to_string()).or_default();
            // A byte MySQL itself has no character for: not a value to match.
            if (utf8 == "3F" && bytes != "3F") || utf8 == "EFBFBD" || utf8 == "NULL" {
                entry.1 += 1;
                continue;
            }
            entry.0 += 1;
            let want = String::from_utf8(hex(utf8)).unwrap();
            match decode(cs, &hex(bytes)) {
                Some(got) if got == want => {}
                Some(got) => {
                    entry.3 += 1;
                    if entry.4.len() < 4 {
                        entry.4.push(format!("{bytes}: mysql {want:?} got {got:?}"));
                    }
                }
                None => {
                    entry.2 += 1;
                    if entry.4.len() < 4 {
                        entry.4.push(format!("{bytes}: mysql {want:?} got nothing"));
                    }
                }
            }
        }
    }
    let mut wrong = Vec::new();
    for (cs, (checked, undefined, none, different, samples)) in &seen {
        println!("{cs}: checked {checked}, undefined {undefined}, no decode {none}, different {different} {samples:?}");
        if admitted(cs) && (*none > 0 || *different > 0) {
            wrong.push(cs.clone());
        }
    }
    assert!(
        wrong.is_empty(),
        "admitted, and not what MySQL does: {wrong:?}"
    );
}
