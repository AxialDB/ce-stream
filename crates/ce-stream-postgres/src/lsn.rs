//! A position in the WAL.

use std::fmt;

use ce_stream_core::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

impl Lsn {
    /// The server's text form, `16/B374D848`.
    pub fn parse(text: &str) -> Result<Self> {
        let bad = || Error::Source(format!("bad LSN {text:?}"));
        let (high, low) = text.split_once('/').ok_or_else(bad)?;
        let high = u32::from_str_radix(high, 16).map_err(|_| bad())?;
        let low = u32::from_str_radix(low, 16).map_err(|_| bad())?;
        Ok(Self((u64::from(high) << 32) | u64::from(low)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_the_servers_form() {
        let lsn = Lsn::parse("16/B374D848").unwrap();
        assert_eq!(lsn.0, 0x16_B374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(Lsn::parse("0/0").unwrap(), Lsn(0));
        assert!(Lsn::parse("nope").is_err());
        assert!(Lsn::parse("1/zz").is_err());
    }

    #[test]
    fn orders_by_value_not_by_text() {
        assert!(Lsn::parse("1/0").unwrap() > Lsn::parse("0/FFFFFFFF").unwrap());
    }
}
