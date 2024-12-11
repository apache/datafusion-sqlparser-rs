use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn read_keywords() -> Vec<(String, Option<String>)> {
    let path = Path::new("src").join("keywords.txt");
    if !path.is_file() {
        panic!("Missing src/keywords.txt");
    }

    let data = std::fs::read_to_string(path).expect("Error reading src/keywords.txt");

    data.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }

            let parts = line.split_ascii_whitespace().collect::<Vec<_>>();
            if parts.len() == 1 {
                Some((parts[0].to_string(), None))
            } else if parts.len() == 2 {
                Some((parts[0].to_string(), Some(parts[1].to_string())))
            } else {
                panic!("Invalid keyword: {}", line);
            }
        })
        .collect::<Vec<_>>()
}

fn write_keyword_enum<W>(file: &mut BufWriter<W>, keywords: &[(String, Option<String>)])
where
    W: ?Sized + Write,
{
    let header = &[
        "#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]\n",
        "#[cfg_attr(feature = \"serde\", derive(Serialize, Deserialize))]\n",
        "#[cfg_attr(feature = \"visitor\", derive(Visit, VisitMut))]\n",
        "#[allow(non_camel_case_types)]\n",
        "pub enum Keyword {\n",
        "    NoKeyword,\n",
    ];
    let header = header.join("");
    write!(file, "{}", header).unwrap();

    keywords.iter().for_each(|kw| {
        writeln!(file, "    {},", kw.0).unwrap();
    });

    writeln!(file, "}}\n").unwrap();
}

fn write_all_keywords<W>(file: &mut BufWriter<W>, keywords: &[(String, Option<String>)])
where
    W: ?Sized + Write,
{
    writeln!(file, "pub const ALL_KEYWORDS: &[&str] = &[").unwrap();
    keywords.iter().for_each(|kw| {
        if kw.1.is_some() {
            writeln!(file, "    \"{}\",", kw.1.as_ref().unwrap()).unwrap();
        } else {
            writeln!(file, "    \"{}\",", kw.0).unwrap();
        }
    });
    writeln!(file, "];\n").unwrap();
}

fn write_phf_map<W>(file: &mut BufWriter<W>, keywords: &[(String, Option<String>)])
where
    W: ?Sized + Write,
{
    let map = phf_codegen::Map::new();
    let map = keywords.iter().fold(map, |mut map, kw| {
        if kw.1.is_some() {
            map.entry(kw.1.as_ref().unwrap(), &format!("Keyword::{}", kw.0));
        } else {
            map.entry(&kw.0, &format!("Keyword::{}", kw.0));
        }
        map
    });

    write!(
        file,
        "static KEYWORD_MAP: phf::Map<&'static str, Keyword> = {}",
        map.build()
    )
    .unwrap();
    writeln!(file, ";").unwrap();
}

fn main() {
    let keywords = read_keywords();
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("keyword_gen.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    write_keyword_enum(&mut file, &keywords);
    write_all_keywords(&mut file, &keywords);
    write_phf_map(&mut file, &keywords);
}
