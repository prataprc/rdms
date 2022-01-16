use prettytable::{cell, row};

use rdms::zimf::{self, Zimf};

pub trait PrettyRow {
    fn to_format() -> prettytable::format::TableFormat;

    fn to_head() -> prettytable::Row;

    fn to_row(&self) -> prettytable::Row;
}

pub fn make_info_table(z: &Zimf) -> prettytable::Table {
    let mut table = prettytable::Table::new();
    table.set_titles(row![Fy => "Info", "Value"]);

    let entries = z.as_entries();
    let title_list = z.as_title_list();

    let n_redirects: usize = entries.iter().filter(|e| e.is_redirect()).map(|_| 1).sum();

    table.add_row(row!["file_loc", format!("{:?}", z.to_location())]);
    table.add_row(row!["entries_count", entries.len() - n_redirects]);
    table.add_row(row!["redirect_count", n_redirects]);
    table.add_row(row!["title_list_count", title_list.len()]);

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table
}

pub fn make_header_table(hdr: &zimf::Header) -> prettytable::Table {
    let uuid = uuid::Uuid::from_slice(&hdr.uuid).unwrap();

    let mut table = prettytable::Table::new();
    table.set_titles(row![Fy => "Field", "Value"]);

    table.add_row(row!["magic_number", hdr.magic_number]);
    table.add_row(row!["major_version", hdr.major_version]);
    table.add_row(row!["minor_version", hdr.minor_version]);
    table.add_row(row!["uuid", uuid]);
    table.add_row(row!["entry_count", hdr.entry_count]);
    table.add_row(row!["cluster_count", hdr.cluster_count]);
    table.add_row(row!["url_ptr_pos", hdr.url_ptr_pos]);
    table.add_row(row!["title_ptr_pos", hdr.title_ptr_pos]);
    table.add_row(row!["cluster_ptr_pos", hdr.cluster_ptr_pos]);
    table.add_row(row!["mime_list_pos", hdr.mime_list_pos]);
    table.add_row(row!["main_page", hdr.main_page]);
    table.add_row(row!["layout_page", hdr.layout_page]);
    table.add_row(row!["checksum_pos", hdr.checksum_pos]);

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table
}

pub fn make_mimes_table(z: &Zimf) -> prettytable::Table {
    let mut table = prettytable::Table::new();
    table.set_titles(row![Fy => "Mime", "num-files"]);

    let mimes = z.as_mimes();
    let entries = z.as_entries();

    let mut mimes_count = vec![0; mimes.len()];
    for entry in entries.iter() {
        let i = entry.mime_type as usize;
        if i < mimes_count.len() {
            mimes_count[i] += 1;
        }
    }

    for (i, mime) in mimes.iter().enumerate() {
        table.add_row(row![mime, mimes_count[i]]);
    }

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table
}

pub fn make_namespace_table(z: &Zimf) -> prettytable::Table {
    let mut table = prettytable::Table::new();
    table.set_titles(row![Fy => "Namespace", "num-files"]);

    let entries = z.as_entries();

    let mut nm_count = vec![0; 256];
    for entry in entries.iter() {
        nm_count[entry.namespace as usize] += 1;
    }

    for (nm, count) in nm_count.into_iter().enumerate() {
        if count > 0 {
            table.add_row(row![nm as u8 as char, count]);
        }
    }

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table
}

pub fn make_entry_table(entry: &zimf::Entry, z: &Zimf) -> prettytable::Table {
    let mut table = prettytable::Table::new();
    table.set_titles(row![Fy => "Field", "Value"]);

    let mimes = z.as_mimes();

    table.add_row(row!["url", entry.url]);
    table.add_row(row!["title", entry.title]);
    table.add_row(row!["mime_type", mimes[entry.mime_type as usize]]);
    table.add_row(row!["namespace", entry.namespace as char]);
    table.add_row(row!["revision", entry.revision]);
    table.add_row(row!["param", format!("{:?}", entry.param)]);

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table
}

pub struct Mime {
    typ: String,
    entries_count: usize,
}

impl PrettyRow for Mime {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        row![Fy => "Type", "Entries-of-this-type"]
    }

    fn to_row(&self) -> prettytable::Row {
        row![self.typ, self.entries_count]
    }
}
