//! [i18n] diff_data's messages, in locales/<lang>.yml - one file per language, every key in every file -
//! compiled in: English, Russian and, not reviewed, French, German, Simplified Chinese and Japanese. An error starts with its code, `DA-Ennn`, the same in
//! every language (documented at docs.dbinvent.com); a progress or a report line has none. The language is
//! [set_lang]'s, else `LANG`'s first two letters; one without its messages - none, `C.UTF-8` - is English.
//! As sge's (`sge::i18n`), whose conventions these are.

use std::sync::RwLock;

/// The languages every message is in
pub const LANGS: [&str; 6] = ["en", "ru", "fr", "de", "zh", "ja"];

/// set_lang's
static LANG: RwLock<Option<String>> = RwLock::new(None);

/// The language of the messages from here on: `en`, `ru`, `fr`, `de`, `zh`, `ja`, or a locale of one
/// (`ru_RU.UTF-8`). Wins over `LANG`. Process-wide, as sge's.
pub fn set_lang(lang: &str) {
    if let Ok(mut l) = LANG.write() {
        *l = Some(two_letters(lang));
    }
}

/// The language: set_lang's, else `LANG`'s first two letters
pub fn lang() -> String {
    if let Some(l) = LANG.read().ok().and_then(|l| l.clone()) {
        return l;
    }
    std::env::var("LANG").map(|l| two_letters(&l)).unwrap_or_default()
}

fn two_letters(lang: &str) -> String {
    lang.chars().take(2).collect::<String>().to_lowercase()
}

/// The message `key` (locales/<lang>.yml) in the language of [lang], each `%{name}` placeholder filled from `args`
pub fn message(key: &str, args: &[(&str, &str)]) -> String {
    message_in(&lang(), key, args)
}

/// The message `key` in language `lang`; English when there is none in it
pub fn message_in(lang: &str, key: &str, args: &[(&str, &str)]) -> String {
    let mut m = rust_i18n::t!(key, locale = lang).into_owned();
    for (name, value) in args {
        m = m.replace(&format!("%{{{}}}", name), value);
    }
    m
}

/// [i18n] The message `key` in the language of [lang], each `%{name}` placeholder filled from `name = value`
macro_rules! tr {
    ($key:expr $(, $name:ident = $value:expr)* $(,)?) => {
        $crate::i18n::message($key, &[$((stringify!($name), $value.to_string().as_str())),*])
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// locales/<lang>.yml, one per language, as key => language => message
    fn messages() -> BTreeMap<String, BTreeMap<String, String>> {
        let mut all: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        let mut files = Vec::new();
        for f in std::fs::read_dir(format!("{}/locales", env!("CARGO_MANIFEST_DIR"))).unwrap() {
            let f = f.unwrap().path();
            let lang = f.file_stem().unwrap().to_str().unwrap().to_string();
            files.push(lang.clone());
            let y: serde_yaml::Mapping = serde_yaml::from_str(&std::fs::read_to_string(&f).unwrap()).unwrap();
            for (k, v) in y {
                let k = k.as_str().unwrap().to_string();
                if k == "_version" {
                    assert_eq!(v.as_u64(), Some(1), "{}: a file per language", f.display());
                    continue;
                }
                let m = v.as_str().unwrap_or_else(|| panic!("{}: {} is not a text", f.display(), k)).to_string();
                assert!(all.entry(k.clone()).or_default().insert(lang.clone(), m).is_none(), "{} twice in {}", k, lang);
            }
        }
        files.sort();
        let mut langs: Vec<String> = LANGS.iter().map(|l| l.to_string()).collect();
        langs.sort();
        assert_eq!(files, langs, "a file per language");
        all
    }

    /// every message in every language, with the same placeholders; an error's code the same in every
    /// language, and no two errors with one code
    #[test]
    fn every_message_is_in_every_language() {
        let placeholders = |m: &str| {
            let mut p: Vec<String> = m.split("%{").skip(1).map(|r| r.split('}').next().unwrap().to_string()).collect();
            p.sort();
            p
        };
        let mut langs: Vec<&str> = LANGS.to_vec();
        langs.sort();
        let mut codes = BTreeMap::new();
        for (key, by_lang) in messages() {
            let en = &by_lang["en"];
            assert_eq!(by_lang.keys().map(String::as_str).collect::<Vec<_>>(), langs, "{}", key);
            let code = en.split(' ').next().filter(|c| c.starts_with("DA-E"));
            assert_eq!(code.is_some(), key.starts_with("error."), "{}: an error, and only an error, has a code", key);
            for (l, m) in &by_lang {
                assert_eq!(placeholders(m), placeholders(en), "{} {}", key, l);
                assert_eq!(m.split(' ').next().filter(|c| c.starts_with("DA-E")), code, "{} {}: the code", key, l);
            }
            if let Some(c) = code {
                assert!(c.len() == 7 && c[4..].chars().all(|d| d.is_ascii_digit()), "{}: DA-Ennn", c);
                assert!(codes.insert(c.to_string(), key.clone()).is_none(), "{} for {} and {}", c, codes[c], key);
            }
        }
        assert!(codes.len() >= 25, "{}", codes.len());
    }

    /// every key the code asks for is in locales/, and every message there asked for
    #[test]
    fn every_key_used_has_its_message_and_every_message_a_use() {
        let all = messages();
        let is_key = |s: &str| ["error.", "cli.", "progress.", "report."].iter().any(|p| s.starts_with(p) && s.len() > p.len());
        let mut used = BTreeSet::new();
        let src = format!("{}/src", env!("CARGO_MANIFEST_DIR"));
        for dir in [src.clone(), format!("{}/cluster", src)] {
            for f in std::fs::read_dir(&dir).unwrap() {
                let f = f.unwrap().path();
                if f.extension() != Some(std::ffi::OsStr::new("rs")) {
                    continue;
                }
                let text = std::fs::read_to_string(&f).unwrap();
                // every "key" literal: from each quote, the key characters up to the closing one
                for (i, _) in text.match_indices('"') {
                    let rest = &text[i + 1..];
                    let end = rest.find(|c: char| !(c.is_ascii_lowercase() || c == '_' || c == '.')).unwrap_or(0);
                    let literal = &rest[..end];
                    if rest[end..].starts_with('"') && is_key(literal) {
                        assert!(all.contains_key(literal), "{}: no message {}", f.display(), literal);
                        used.insert(literal.to_string());
                    }
                }
            }
        }
        let unused: Vec<&String> = all.keys().filter(|k| !used.contains(*k)).collect();
        assert!(unused.is_empty(), "messages nothing asks for: {:?}", unused);
    }

    #[test]
    fn a_message_is_in_the_language_asked_and_english_without_one() {
        let ru = message_in("ru", "error.table_not_found", &[("table", "t")]);
        assert!(ru.starts_with("DA-E005 ") && ru.ends_with(": t") && ru != message_in("en", "error.table_not_found", &[("table", "t")]));
        assert_eq!(message_in("xx", "error.cancelled", &[]), message_in("en", "error.cancelled", &[]));
        assert_eq!(two_letters("ru_RU.UTF-8"), "ru");
        assert_eq!(two_letters("C.UTF-8"), "c.");
        // English everywhere, as every other test expects: the language is the process's
        set_lang("EN_us");
        assert_eq!(lang(), "en");
        assert_eq!(message("error.cancelled", &[]), message_in("en", "error.cancelled", &[]));
    }
}
