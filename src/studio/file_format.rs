use rbx_dom_weak::WeakDom;
use std::borrow::Cow;
use std::io::Write;
use std::path::Path;
use tracing::warn;

/// Binary Roblox files (.rbxl, .rbxm) start with this.
const BINARY_HEADER: &[u8] = b"<roblox!";

/// Reads a place or model file. Works with both binary (.rbxl, .rbxm) and
/// xml (.rbxlx, .rbxmx) files, the format is detected from the content.
pub fn read_dom(data: &[u8]) -> anyhow::Result<WeakDom> {
    if data.starts_with(BINARY_HEADER) {
        return rbx_binary::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to read binary file: {}", e));
    }

    rbx_xml::from_reader_default(&*replace_invalid_xml_bytes(data))
        .map_err(|e| anyhow::anyhow!("Failed to read xml file: {}", e))
}

/// Control characters that aren't allowed anywhere in xml. Studio can still save them
/// (for example a stray character in a script), but xml parsers reject the whole file.
fn is_invalid_xml_byte(byte: u8) -> bool {
    byte < 0x20 && !matches!(byte, b'\t' | b'\n' | b'\r')
}

/// Replaces control characters that xml doesn't allow with `\u{FFFD}`, so the file can be read.
///
/// These bytes are never part of a multi-byte UTF-8 character, so nothing else changes.
fn replace_invalid_xml_bytes(data: &[u8]) -> Cow<'_, [u8]> {
    let invalid = data.iter().filter(|&&b| is_invalid_xml_byte(b)).count();
    if invalid == 0 {
        return Cow::Borrowed(data);
    }

    warn!(
        "The file has {} control characters that xml doesn't allow, replacing them with U+FFFD",
        invalid
    );

    let mut cleaned = Vec::with_capacity(data.len() + invalid * 2);
    for &byte in data {
        if is_invalid_xml_byte(byte) {
            cleaned.extend_from_slice("\u{FFFD}".as_bytes());
        } else {
            cleaned.push(byte);
        }
    }
    Cow::Owned(cleaned)
}

/// Writes the children of the dom's root, as xml or binary.
pub fn write_dom<W: Write>(writer: W, dom: &WeakDom, xml: bool) -> anyhow::Result<()> {
    let root_children = dom.root().children();

    if xml {
        rbx_xml::to_writer_default(writer, dom, root_children)?;
    } else {
        rbx_binary::to_writer(writer, dom, root_children)?;
    }
    Ok(())
}

/// Returns true if the path is an xml place or model (.rbxlx or .rbxmx).
pub fn is_xml_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("rbxlx") || extension.eq_ignore_ascii_case("rbxmx")
        })
}

#[cfg(test)]
mod tests {
    use super::{is_xml_path, read_dom, write_dom};
    use rbx_dom_weak::{InstanceBuilder, WeakDom};
    use std::path::Path;

    #[test]
    fn round_trips_binary_and_xml() {
        let dom = WeakDom::new(
            InstanceBuilder::new("DataModel").with_child(InstanceBuilder::new("Folder")),
        );

        for xml in [false, true] {
            let mut data = Vec::new();
            write_dom(&mut data, &dom, xml).unwrap();
            let read = read_dom(&data).unwrap();
            assert_eq!(read.root().children().len(), 1);
        }
    }

    #[test]
    fn reads_xml_with_invalid_control_characters() {
        let dom = WeakDom::new(
            InstanceBuilder::new("DataModel").with_child(
                InstanceBuilder::new("Script")
                    .with_property("Source", rbx_types::Variant::String("-- a\u{10}b".into())),
            ),
        );
        let mut data = Vec::new();
        write_dom(&mut data, &dom, true).unwrap();
        let data = String::from_utf8(data).unwrap().replace("&#16;", "\u{10}");

        let read = read_dom(data.as_bytes()).unwrap();
        let script = read.get_by_ref(read.root().children()[0]).unwrap();
        assert_eq!(
            script.properties.get(&ustr::ustr("Source")),
            Some(&rbx_types::Variant::String("-- a\u{FFFD}b".into()))
        );
    }

    #[test]
    fn detects_xml_paths() {
        assert!(is_xml_path(Path::new("place.rbxlx")));
        assert!(is_xml_path(Path::new("model.RBXMX")));
        assert!(!is_xml_path(Path::new("place.rbxl")));
    }
}
