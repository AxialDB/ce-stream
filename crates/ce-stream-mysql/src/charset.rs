//! A column's bytes as text (issue #19). The binlog carries a column's own
//! bytes; its charset comes from the table map's metadata.
//!
//! A decoder is admitted only after it matched MySQL's own `CONVERT` on every
//! byte sequence (`tests/charsets.rs`, `scripts/charset-tables/`). A charset
//! with no admitted decoder is refused when a table enters the include list.

use encoding_rs::Encoding;

/// Collation id to charset, from `information_schema.COLLATIONS` on 9.7.2.
const COLLATIONS: &[(&str, &[u32])] = &[
    ("armscii8", &[32, 64]),
    ("ascii", &[11, 65]),
    ("big5", &[1, 84]),
    ("binary", &[63]),
    ("cp1250", &[26, 34, 44, 66, 99]),
    ("cp1251", &[14, 23, 50, 51, 52]),
    ("cp1256", &[57, 67]),
    ("cp1257", &[29, 58, 59]),
    ("cp850", &[4, 80]),
    ("cp852", &[40, 81]),
    ("cp866", &[36, 68]),
    ("cp932", &[95, 96]),
    ("dec8", &[3, 69]),
    ("eucjpms", &[97, 98]),
    ("euckr", &[19, 85]),
    ("gb18030", &[248, 249, 250]),
    ("gb2312", &[24, 86]),
    ("gbk", &[28, 87]),
    ("geostd8", &[92, 93]),
    ("greek", &[25, 70]),
    ("hebrew", &[16, 71]),
    ("hp8", &[6, 72]),
    ("keybcs2", &[37, 73]),
    ("koi8r", &[7, 74]),
    ("koi8u", &[22, 75]),
    ("latin1", &[5, 8, 15, 31, 47, 48, 49, 94]),
    ("latin2", &[2, 9, 21, 27, 77]),
    ("latin5", &[30, 78]),
    ("latin7", &[20, 41, 42, 79]),
    ("macce", &[38, 43]),
    ("macroman", &[39, 53]),
    ("sjis", &[13, 88]),
    ("swe7", &[10, 82]),
    ("tis620", &[18, 89]),
    (
        "ucs2",
        &[
            35, 90, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
            144, 145, 146, 147, 148, 149, 150, 151, 159,
        ],
    ),
    ("ujis", &[12, 91]),
    (
        "utf16",
        &[
            54, 55, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
            117, 118, 119, 120, 121, 122, 123, 124,
        ],
    ),
    ("utf16le", &[56, 62]),
    (
        "utf32",
        &[
            60, 61, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
            176, 177, 178, 179, 180, 181, 182, 183,
        ],
    ),
    (
        "utf8mb3",
        &[
            33, 76, 83, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206,
            207, 208, 209, 210, 211, 212, 213, 214, 215, 223,
        ],
    ),
    (
        "utf8mb4",
        &[
            45, 46, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
            240, 241, 242, 243, 244, 245, 246, 247, 255, 256, 257, 258, 259, 260, 261, 262, 263,
            264, 265, 266, 267, 268, 269, 270, 271, 273, 274, 275, 277, 278, 279, 280, 281, 282,
            283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 296, 297, 298, 300, 303,
            304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320,
            321, 322, 323,
        ],
    ),
];

/// The charset of a collation id. `None` for an id this table does not know,
/// which is a collation newer than the table: refused, never guessed.
pub fn charset_of_collation(id: u32) -> Option<&'static str> {
    COLLATIONS
        .iter()
        .find(|(_, ids)| ids.contains(&id))
        .map(|(name, _)| *name)
}

/// The decoder of a charset that matched MySQL's `CONVERT` on every sequence.
/// The others differ on some characters (`sjis`, `ujis`, `big5`, `gb2312`,
/// `gb18030`, `latin5`, `tis620`, `greek`, `hebrew`, `cp866`, `koi8u`), or have
/// no decoder here (`armscii8`, `cp850`, `cp852`, `dec8`, `geostd8`, `hp8`,
/// `keybcs2`, `macce`, `swe7`), or were not checked (`utf16`, `utf16le`, `ucs2`,
/// `utf32`).
/// The decoder of a charset that matched MySQL's `CONVERT` on every sequence.
/// The others differ on some characters (`sjis`, `ujis`, `eucjpms`, `big5`,
/// `gb2312`, `gb18030`, `latin5`, `tis620`, `greek`, `hebrew`, `cp866`,
/// `koi8u`), or have no decoder here (`armscii8`, `cp850`, `cp852`, `dec8`,
/// `geostd8`, `hp8`, `keybcs2`, `macce`, `swe7`), or were not checked (`utf16`,
/// `utf16le`, `ucs2`, `utf32`).
fn encoding_of(charset: &str) -> Option<&'static Encoding> {
    Some(match charset {
        "utf8mb4" | "utf8mb3" => encoding_rs::UTF_8,
        "latin1" => encoding_rs::WINDOWS_1252,
        "latin2" => encoding_rs::ISO_8859_2,
        "latin7" => encoding_rs::ISO_8859_13,
        "cp1250" => encoding_rs::WINDOWS_1250,
        "cp1251" => encoding_rs::WINDOWS_1251,
        "cp1256" => encoding_rs::WINDOWS_1256,
        "cp1257" => encoding_rs::WINDOWS_1257,
        "koi8r" => encoding_rs::KOI8_R,
        "macroman" => encoding_rs::MACINTOSH,
        "cp932" => encoding_rs::SHIFT_JIS,
        "euckr" => encoding_rs::EUC_KR,
        "gbk" => encoding_rs::GBK,
        _ => return None,
    })
}

/// Does this charset have a decoder that matches MySQL?
pub fn admitted(charset: &str) -> bool {
    charset == "ascii" || encoding_of(charset).is_some()
}

/// The text of a column's bytes in `charset`. `None` when the bytes are not
/// valid in it, or the charset is not admitted.
pub fn decode(charset: &str, bytes: &[u8]) -> Option<String> {
    if charset == "ascii" {
        return bytes
            .is_ascii()
            .then(|| String::from_utf8_lossy(bytes).into_owned());
    }
    let (text, malformed) = encoding_of(charset)?.decode_without_bom_handling(bytes);
    (!malformed).then(|| text.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_collation_names_its_charset() {
        assert_eq!(charset_of_collation(8), Some("latin1"));
        assert_eq!(charset_of_collation(255), Some("utf8mb4"));
        assert_eq!(charset_of_collation(63), Some("binary"));
        assert_eq!(charset_of_collation(9999), None);
    }

    #[test]
    fn a_column_is_decoded_by_its_charset() {
        assert_eq!(
            decode("latin1", &[b't', b'h', 0xe9]).as_deref(),
            Some("thé")
        );
        // These bytes are two characters in latin1 and one in UTF-8.
        assert_eq!(decode("latin1", &[0xc3, 0xa9]).as_deref(), Some("Ã©"));
        assert_eq!(decode("utf8mb4", &[0xc3, 0xa9]).as_deref(), Some("é"));
        assert_eq!(decode("utf8mb4", &[0xe9]), None);
        assert_eq!(decode("ascii", &[0xe9]), None);
        assert_eq!(decode("sjis", b"a"), None);
        assert!(admitted("cp1251") && admitted("ascii") && !admitted("sjis"));
    }
}
