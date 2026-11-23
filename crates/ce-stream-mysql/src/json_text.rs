//! A binlog JSON value as the text MySQL itself gives for it, so that a row reads
//! the same from the binlog and from a `SELECT` (issue #19). Checked against
//! MySQL 9.7.2 on the lab; the doubles follow its `my_gcvt`.

use base64::Engine;
use mysql_binlog_connector_rust::column::column_type::ColumnType;
use mysql_binlog_connector_rust::column::json::json_binary::JsonBinary;
use mysql_binlog_connector_rust::column::json::json_formatter::JsonFormatter;

/// MySQL writes a double with no exponent while its decimal point lands within
/// this many digits of the first significant one (`DBL_DIG`).
const MAX_DIGITS_WITHOUT_EXPONENT: i32 = 15;

#[derive(Default)]
struct MysqlJsonText {
    out: String,
}

/// The JSON value's text, as `SELECT` shows it.
pub fn json_text(bytes: &[u8]) -> Result<String, String> {
    let mut text = MysqlJsonText::default();
    JsonBinary::parse(bytes, &mut text).map_err(|e| e.to_string())?;
    Ok(text.out)
}

/// A double as MySQL prints it in JSON.
fn double_text(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    // Rust's `{:e}` gives the shortest digits that read back the same, as
    // MySQL's dtoa does: `1.5e30`, `1e-7`.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("a double in {:e} has an exponent");
    let exponent: i32 = exponent.parse().expect("the exponent is a number");
    let decimal_point = exponent + 1;
    if decimal_point > MAX_DIGITS_WITHOUT_EXPONENT || decimal_point <= -MAX_DIGITS_WITHOUT_EXPONENT
    {
        return format!("{mantissa}e{exponent}");
    }
    let fixed = format!("{value}");
    if fixed.contains('.') {
        fixed
    } else {
        format!("{fixed}.0")
    }
}

impl MysqlJsonText {
    fn append_string(&mut self, text: &str) {
        for c in text.chars() {
            match c {
                '"' => self.out.push_str("\\\""),
                '\\' => self.out.push_str("\\\\"),
                '\u{08}' => self.out.push_str("\\b"),
                '\t' => self.out.push_str("\\t"),
                '\u{0C}' => self.out.push_str("\\f"),
                '\n' => self.out.push_str("\\n"),
                '\r' => self.out.push_str("\\r"),
                c if (c as u32) < 0x20 => {
                    self.out.push_str(&format!("\\u{:04x}", c as u32));
                }
                c => self.out.push(c),
            }
        }
    }

    fn append_date(&mut self, year: i32, month: i32, day: i32) {
        if year < 0 {
            self.out.push('-');
        }
        self.out
            .push_str(&format!("{:04}-{month:02}-{day:02}", year.abs()));
    }

    fn append_time(&mut self, hour: i32, min: i32, sec: i32, micro_seconds: i32) {
        self.out
            .push_str(&format!("{hour:02}:{min:02}:{sec:02}.{micro_seconds:06}"));
    }
}

impl JsonFormatter for MysqlJsonText {
    fn begin_object(&mut self, _num_elements: u32) {
        self.out.push('{');
    }

    fn begin_array(&mut self, _num_elements: u32) {
        self.out.push('[');
    }

    fn end_object(&mut self) {
        self.out.push('}');
    }

    fn end_array(&mut self) {
        self.out.push(']');
    }

    fn name(&mut self, name: &str) {
        self.out.push('"');
        self.append_string(name);
        self.out.push_str("\": ");
    }

    fn value_string(&mut self, value: &str) {
        self.out.push('"');
        self.append_string(value);
        self.out.push('"');
    }

    fn value_int(&mut self, value: i32) {
        self.out.push_str(&value.to_string());
    }

    fn value_long(&mut self, value: i64) {
        self.out.push_str(&value.to_string());
    }

    fn value_double(&mut self, value: f64) {
        self.out.push_str(&double_text(value));
    }

    fn value_big_int(&mut self, value: i128) {
        self.out.push_str(&value.to_string());
    }

    fn value_decimal(&mut self, value: &str) {
        self.out.push_str(value);
    }

    fn value_bool(&mut self, value: bool) {
        self.out.push_str(&value.to_string());
    }

    fn value_null(&mut self) {
        self.out.push_str("null");
    }

    fn value_year(&mut self, year: i32) {
        self.out.push_str(&year.to_string());
    }

    fn value_date(&mut self, year: i32, month: i32, day: i32) {
        self.out.push('"');
        self.append_date(year, month, day);
        self.out.push('"');
    }

    fn value_datetime(
        &mut self,
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        min: i32,
        sec: i32,
        micro_seconds: i32,
    ) {
        self.out.push('"');
        self.append_date(year, month, day);
        self.out.push(' ');
        self.append_time(hour, min, sec, micro_seconds);
        self.out.push('"');
    }

    fn value_time(&mut self, hour: i32, min: i32, sec: i32, micro_seconds: i32) {
        self.out.push('"');
        self.append_time(hour, min, sec, micro_seconds);
        self.out.push('"');
    }

    fn value_negative_time(&mut self, hour: i32, min: i32, sec: i32, micro_seconds: i32) {
        self.out.push_str("\"-");
        self.append_time(hour, min, sec, micro_seconds);
        self.out.push('"');
    }

    fn value_timestamp(&mut self, seconds_past_epoch: i64, micro_seconds: i32) {
        let at = chrono::DateTime::from_timestamp(seconds_past_epoch, 0)
            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();
        self.out.push_str(&format!("\"{at}.{micro_seconds:06}\""));
    }

    fn value_opaque(&mut self, column_type: &ColumnType, value: &[u8]) {
        self.out.push_str(&format!(
            "\"base64:type{}:{}\"",
            column_type.clone() as i32,
            base64::engine::general_purpose::STANDARD.encode(value)
        ));
    }

    fn next_entry(&mut self) {
        self.out.push_str(", ");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_read_as_mysql_prints_them() {
        for (value, text) in [
            (1.5, "1.5"),
            (-0.25, "-0.25"),
            (3.0, "3.0"),
            (0.0, "0.0"),
            (1e20, "1e20"),
            (1e30, "1e30"),
            (1.5e30, "1.5e30"),
            (1e-7, "0.0000001"),
        ] {
            assert_eq!(double_text(value), text);
        }
    }
}
