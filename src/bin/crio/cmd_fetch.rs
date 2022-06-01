use serde::{Deserialize, Serialize};

use std::{env, ffi, fs, path, result, time};

use crate::types;
use rdms::{err_at, git, util, Error, Result};

macro_rules! unpack_primary_table {
    ($profile:ident, $file:expr, $txn:ident, $tbl:expr, $type:ty) => {{
        let file_loc: path::PathBuf =
            [crates_io_untar_dir($profile)?, $file.into()].iter().collect();

        let mut fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&file_loc))?;
        let mut rdr = csv::Reader::from_reader(&mut fd);
        let iter = rdr
            .deserialize()
            .map(|r: result::Result<$type, csv::Error>| err_at!(InvalidFormat, r));

        let n = {
            let start = time::Instant::now();

            let (mut n, mut x) = (0, 0);
            for (i, item) in iter.enumerate() {
                let item: $type = item?;
                let key = match item.to_key() {
                    Some(key) => key,
                    None => {
                        x += 1;
                        continue;
                    }
                };
                print!("{} {}\r", $tbl, i);
                let s = err_at!(FailConvert, serde_json::to_string_pretty(&item))?;
                $txn.insert(key, s)?;
                n += 1;
            }

            assert!(x == 0);

            println!(
                "{:25?} <- unpacked {} records, took {:?}",
                $tbl,
                n,
                start.elapsed()
            );

            n
        };
        n
    }};
}

macro_rules! unpack_secondary_table {
    ($profile:ident, $tbl:expr, $src:expr, $dst:expr, $txn:ident, $type:ty) => {{
        let start = time::Instant::now();
        let untar_dir = crates_io_untar_dir($profile)?;

        let src_loc: path::PathBuf =
            [untar_dir, "data".into(), $src.into()].iter().collect();
        let key: path::PathBuf = [$tbl, $dst].iter().collect();

        let mut fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&src_loc))?;
        let mut rdr = csv::Reader::from_reader(&mut fd);
        let iter = rdr
            .deserialize()
            .map(|r: result::Result<$type, csv::Error>| err_at!(InvalidFormat, r));

        let mut items = vec![];
        for (i, item) in iter.enumerate() {
            items.push(item?);
            print!("{} {}\r", $tbl, i);
        }
        let s = err_at!(FailConvert, serde_json::to_string_pretty(&items))?;

        $txn.insert(key.clone(), s.as_bytes())?;

        println!("copied {:?} -> {:?}, took {:?}", src_loc, key, start.elapsed());
    }};
}

pub struct Opt {
    nohttp: bool,
    nountar: bool,
    nocopy: bool,
    git_root: Option<ffi::OsString>,
    profile: ffi::OsString,
}

impl From<crate::SubCommand> for Opt {
    fn from(sub_cmd: crate::SubCommand) -> Self {
        match sub_cmd {
            crate::SubCommand::Fetch { nohttp, nountar, nocopy, git_root, profile } => {
                Opt { nohttp, nountar, nocopy, git_root, profile }
            }
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct Profile {
    temp_dir: Option<String>,
    dump_url: url::Url,
    git_index_dir: Option<String>,
    git_analytics_dir: Option<String>,
    git: rdms::git::Config,
}

pub fn handle(opts: Opt) -> Result<()> {
    let mut profile: Profile = util::files::load_toml(&opts.profile)?;
    profile.git.loc_repo = opts
        .git_root
        .clone()
        .map(|s| s.to_str().unwrap().to_string())
        .unwrap_or_else(|| profile.git.loc_repo.clone());

    let crates_io_dump_loc = match opts.nohttp {
        true => crates_io_dump_loc(&profile),
        false => {
            remove_temp_dir(&profile)?;
            get_latest_db_dump(&profile)?
        }
    };
    match opts.nountar {
        true => (),
        false => untar(crates_io_dump_loc)?,
    };
    match opts.nocopy {
        true => (),
        false => {
            let metadata = crates_io_metadata(&opts, &profile)?;
            unpack_csv_tables(&opts, &profile, &metadata)?;
        }
    };

    Ok(())
}

fn untar(loc: path::PathBuf) -> Result<()> {
    use flate2::read::GzDecoder;
    use tar::Archive;

    let start = time::Instant::now();

    let fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&loc))?;
    match loc.extension().map(|x| x.to_str()) {
        Some(Some("gz")) => err_at!(
            IOError,
            Archive::new(GzDecoder::new(fd)).unpack(loc.parent().unwrap())
        )?,
        Some(Some("tar")) => {
            err_at!(IOError, Archive::new(fd).unpack(loc.parent().unwrap()))?
        }
        Some(_) | None => err_at!(Fatal, msg: "invalid tar dump: {:?}", loc)?,
    };

    println!("untar into {:?} ... ok ({:?})", loc.parent().unwrap(), start.elapsed());

    Ok(())
}

fn get_latest_db_dump(profile: &Profile) -> Result<path::PathBuf> {
    use std::io::{Read, Write};

    let start = time::Instant::now();

    let crates_io_dump_loc = crates_io_dump_loc(profile);
    let mut fd = err_at!(
        IOError,
        fs::OpenOptions::new().create(true).write(true).open(&crates_io_dump_loc)
    )?;

    let mut reader =
        err_at!(IOError, ureq::get(profile.dump_url.as_str()).call())?.into_reader();

    let (mut buf, mut m) = (vec![0; 1024 * 1024], 0);
    loop {
        match err_at!(IOError, reader.read(&mut buf))? {
            0 => break,
            n => {
                m += err_at!(IOError, fd.write(&buf[..n]))?;
            }
        }
    }

    println!(
        "fetched {} bytes into {:?} ... ok ({:?})",
        m,
        crates_io_dump_loc,
        start.elapsed()
    );

    Ok(crates_io_dump_loc)
}

fn remove_temp_dir(profile: &Profile) -> Result<()> {
    let temp_dir: path::PathBuf = [
        profile.temp_dir.as_ref().map(|x| x.into()).unwrap_or_else(env::temp_dir),
        crate::TEMP_DIR_CRIO.into(),
    ]
    .iter()
    .collect();

    fs::remove_dir_all(&temp_dir).ok();
    Ok(())
}

fn crates_io_dump_loc(profile: &Profile) -> path::PathBuf {
    let temp_dir: path::PathBuf = [
        profile.temp_dir.as_ref().map(|x| x.into()).unwrap_or_else(env::temp_dir),
        crate::TEMP_DIR_CRIO.into(),
    ]
    .iter()
    .collect();

    fs::create_dir_all(&temp_dir).ok();

    let dump_fname = path::Path::new(profile.dump_url.path()).file_name().unwrap();
    [temp_dir, dump_fname.into()].iter().collect()
}

fn crates_io_untar_dir(profile: &Profile) -> Result<path::PathBuf> {
    let parent: path::PathBuf = crates_io_dump_loc(profile).parent().unwrap().into();
    for entry in err_at!(IOError, fs::read_dir(&parent))? {
        let entry = err_at!(IOError, entry)?;
        let ok = err_at!(IOError, entry.metadata())?.is_dir();
        match entry.file_name().to_str() {
            Some(name) if ok && name.starts_with("20") => {
                return Ok([parent, entry.file_name().into()].iter().collect())
            }
            _ => (),
        }
    }

    err_at!(Fatal, msg: "missing valid untar-ed directory in {:?}", parent)
}

fn crates_io_metadata(_opts: &Opt, profile: &Profile) -> Result<Metadata> {
    use std::str::from_utf8;

    let src_loc: path::PathBuf =
        [crates_io_untar_dir(profile)?, "metadata.json".into()].iter().collect();
    let dst_loc: path::PathBuf =
        [profile.git.loc_repo.clone(), "metadata.json".into()].iter().collect();

    let data = err_at!(IOError, fs::read(&src_loc))?;
    err_at!(IOError, fs::write(&dst_loc, &data), "{:?}", dst_loc)?;

    println!("copied {:?} -> {:?} ... ok", src_loc, dst_loc);

    let s = err_at!(FailConvert, from_utf8(&data))?;
    err_at!(FailConvert, serde_json::from_str(s))
}

fn unpack_csv_tables(_opts: &Opt, profile: &Profile, metadata: &Metadata) -> Result<()> {
    let mut index = git::Index::open(profile.git.clone())?;

    let txn = {
        let mut txn = index.transaction()?;

        // TODO: depedencies.csv and teams.csv to be upacked.

        // Primary tables
        unpack_crates_csv(profile, &mut txn)?;
        unpack_categories_csv(profile, &mut txn)?;
        unpack_users_csv(profile, &mut txn)?;
        unpack_keywords_csv(profile, &mut txn)?;

        // Secondary tables
        unpack_secondary_csv(profile, &mut txn)?;

        txn
    };

    {
        print!("commiting transaction, ");
        let start = time::Instant::now();
        let message = err_at!(FailConvert, serde_json::to_string_pretty(&metadata))?;
        txn.commit(&message)?;
        println!("took {:?}", start.elapsed());
    }

    {
        print!("checking out, ");
        let start = time::Instant::now();
        let mut cb = git2::build::CheckoutBuilder::new();
        cb.recreate_missing(true);
        index.checkout_head(Some(&mut cb))?;
        println!("took {:?}", start.elapsed());
    }

    Ok(())
}

fn unpack_crates_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_primary_table!(
        profile,
        "data/crates.csv",
        txn,
        types::CRATE_TABLE,
        types::Crate
    );

    Ok(n)
}

fn unpack_categories_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_primary_table!(
        profile,
        "data/categories.csv",
        txn,
        types::CATEGORY_TABLE,
        types::Category
    );

    Ok(n)
}

fn unpack_users_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_primary_table!(
        profile,
        "data/users.csv",
        txn,
        types::USER_TABLE,
        types::User
    );

    Ok(n)
}

fn unpack_keywords_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_primary_table!(
        profile,
        "data/keywords.csv",
        txn,
        types::KEYWORDS_TABLE,
        types::Keyword
    );

    Ok(n)
}

fn unpack_secondary_csv(profile: &Profile, txn: &mut git::Txn) -> Result<()> {
    unpack_secondary_table!(
        profile,
        "table:versions",
        "versions.csv",
        "versions.json",
        txn,
        types::Version
    );
    unpack_secondary_table!(
        profile,
        "table:badges",
        "badges.csv",
        "badges.json",
        txn,
        types::Badge
    );
    unpack_secondary_table!(
        profile,
        "table:metadata",
        "metadata.csv",
        "metadata.json",
        txn,
        types::Metadata
    );
    unpack_secondary_table!(
        profile,
        "table:reserved_crate_names",
        "reserved_crate_names.csv",
        "reserved_crate_names.json",
        txn,
        types::ReservedCrateName
    );
    unpack_secondary_table!(
        profile,
        "table:version_downloads",
        "version_downloads.csv",
        "version_downloads.json",
        txn,
        types::VersionDownloads
    );
    unpack_secondary_table!(
        profile,
        "table:crate_owners",
        "crate_owners.csv",
        "crate_owners.json",
        txn,
        types::CrateOwners
    );
    unpack_secondary_table!(
        profile,
        "table:crates_categories",
        "crates_categories.csv",
        "crates_categories.json",
        txn,
        types::CrateCategories
    );
    unpack_secondary_table!(
        profile,
        "table:crates_keywords",
        "crates_keywords.csv",
        "crates_keywords.json",
        txn,
        types::CrateKeywords
    );

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    timestamp: String,
    crates_io_commit: String,
}
