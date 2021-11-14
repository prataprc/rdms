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

    let n_redirects: usize = z
        .entries
        .iter()
        .filter(|e| e.is_redirect())
        .map(|_| 1)
        .sum();

    table.add_row(row!["file_loc", format!("{:?}", z.loc)]);
    table.add_row(row!["entries_count", z.entries.len() - n_redirects]);
    table.add_row(row!["redirect_count", n_redirects]);
    table.add_row(row!["title_list_count", z.title_list.len()]);

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

    let mut mimes_count = vec![0; z.mimes.len()];
    for entry in z.entries.iter() {
        let i = entry.mime_type as usize;
        if i < mimes_count.len() {
            mimes_count[i] += 1;
        }
    }

    for (i, mime) in z.mimes.iter().enumerate() {
        table.add_row(row![mime, mimes_count[i]]);
    }

    table.set_format(*prettytable::format::consts::FORMAT_CLEAN);
    table
}

pub fn make_namespace_table(z: &Zimf) -> prettytable::Table {
    let mut table = prettytable::Table::new();
    table.set_titles(row![Fy => "Namespace", "num-files"]);

    let mut nm_count = vec![0; 256];
    for entry in z.entries.iter() {
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

    table.add_row(row!["url", entry.url]);
    table.add_row(row!["title", entry.title]);
    table.add_row(row!["mime_type", z.mimes[entry.mime_type as usize]]);
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

#[allow(dead_code)]
pub fn make_table<R>(rows: &[R]) -> prettytable::Table
where
    R: PrettyRow,
{
    let mut table = prettytable::Table::new();

    match rows.len() {
        0 => table,
        _ => {
            table.set_titles(R::to_head());
            rows.iter().for_each(|r| {
                table.add_row(r.to_row());
            });
            table.set_format(R::to_format());
            table
        }
    }
}
