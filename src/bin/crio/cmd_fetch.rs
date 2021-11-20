use serde::Deserialize;

use std::{env, ffi, fs, path, result, time};

use crate::types;
use rdms::{err_at, git, util, Error, Result};

macro_rules! unpack_table {
    ($profile:ident, $file:expr, $txn:ident, $tbl:expr, $type:ty) => {{
        let file_loc: path::PathBuf = [crates_io_untar_dir($profile)?, $file.into()]
            .iter()
            .collect();

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
            crate::SubCommand::Fetch {
                nohttp,
                nountar,
                nocopy,
                git_root,
                profile,
            } => Opt {
                nohttp,
                nountar,
                nocopy,
                git_root,
                profile,
            },
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
            crates_io_metadata(&opts, &profile)?;
            unpack_csv_tables(&opts, &profile)?;
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

    println!(
        "untar into {:?} ... ok ({:?})",
        loc.parent().unwrap(),
        start.elapsed()
    );

    Ok(())
}

fn get_latest_db_dump(profile: &Profile) -> Result<path::PathBuf> {
    use std::io::{Read, Write};

    let start = time::Instant::now();

    let crates_io_dump_loc = crates_io_dump_loc(profile);
    let mut fd = err_at!(
        IOError,
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&crates_io_dump_loc)
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
        profile
            .temp_dir
            .as_ref()
            .map(|x| x.into())
            .unwrap_or_else(env::temp_dir),
        crate::TEMP_DIR_CRIO.into(),
    ]
    .iter()
    .collect();

    fs::remove_dir_all(&temp_dir).ok();
    Ok(())
}

fn crates_io_dump_loc(profile: &Profile) -> path::PathBuf {
    let temp_dir: path::PathBuf = [
        profile
            .temp_dir
            .as_ref()
            .map(|x| x.into())
            .unwrap_or_else(env::temp_dir),
        crate::TEMP_DIR_CRIO.into(),
    ]
    .iter()
    .collect();

    fs::create_dir_all(&temp_dir).ok();

    let dump_fname = path::Path::new(profile.dump_url.path())
        .file_name()
        .unwrap();
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

fn crates_io_metadata(_opts: &Opt, profile: &Profile) -> Result<()> {
    let src_loc: path::PathBuf = [crates_io_untar_dir(profile)?, "metadata.json".into()]
        .iter()
        .collect();
    let dst_loc: path::PathBuf = [profile.git.loc_repo.clone(), "metadata.json".into()]
        .iter()
        .collect();

    let data = err_at!(IOError, fs::read(&src_loc))?;
    err_at!(IOError, fs::write(&dst_loc, &data))?;

    println!("copied {:?} -> {:?} ... ok", src_loc, dst_loc);

    Ok(())
}

fn unpack_csv_tables(_opts: &Opt, profile: &Profile) -> Result<()> {
    let mut index = git::Index::open(profile.git.clone())?;

    {
        let dir_date = crates_io_untar_dir(profile)?;
        let mut txn = index.transaction()?;

        // TODO: depedencies.csv and teams.csv to be upacked.
        //unpack_crates_csv(profile, &mut txn)?;
        //unpack_badges_csv(profile, &mut txn)?;
        //unpack_categories_csv(profile, &mut txn)?;
        //unpack_users_csv(profile, &mut txn)?;
        //unpack_keywords_csv(profile, &mut txn)?;
        //unpack_metadata_csv(profile, &mut txn)?;
        //unpack_reserved_crate_names_csv(profile, &mut txn)?;
        //unpack_versions_csv(profile, &mut txn)?;
        unpack_version_downloads_csv(profile, &mut txn)?;
        unpack_crate_owners_csv(profile, &mut txn)?;
        unpack_crate_categories_csv(profile, &mut txn)?;
        unpack_crate_keywords_csv(profile, &mut txn)?;

        txn.commit(dir_date.file_name().unwrap().to_str())?;
    }

    print!("checking out ... ");
    let mut cb = git2::build::CheckoutBuilder::new();
    cb.recreate_missing(true);
    index.checkout_head(Some(&mut cb))?;
    println!("ok");

    Ok(())
}

fn unpack_crates_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/crates.csv",
        txn,
        types::CRATE_TABLE,
        types::Crate
    );

    Ok(n)
}

fn unpack_badges_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/badges.csv",
        txn,
        types::BADGE_TABLE,
        types::Badge
    );

    Ok(n)
}

fn unpack_categories_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/categories.csv",
        txn,
        types::CATEGORY_TABLE,
        types::Category
    );

    Ok(n)
}

fn unpack_users_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/users.csv",
        txn,
        types::USER_TABLE,
        types::User
    );

    Ok(n)
}

fn unpack_keywords_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/keywords.csv",
        txn,
        types::KEYWORDS_TABLE,
        types::Keyword
    );

    Ok(n)
}

fn unpack_metadata_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/metadata.csv",
        txn,
        "metadata",
        types::Metadata
    );

    Ok(n)
}

fn unpack_reserved_crate_names_csv(
    profile: &Profile,
    txn: &mut git::Txn,
) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/reserved_crate_names.csv",
        txn,
        types::RESERVED_CRATE_NAME_TABLE,
        types::ReservedCrateName
    );

    Ok(n)
}

fn unpack_versions_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/versions.csv",
        txn,
        types::VERSION_TABLE,
        types::Version
    );

    Ok(n)
}

fn unpack_version_downloads_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/version_downloads.csv",
        txn,
        types::VERSION_DOWNLOADS_TABLE,
        types::VersionDownloads
    );

    Ok(n)
}

fn unpack_crate_owners_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/crate_owners.csv",
        txn,
        types::CRATE_OWNERS_TABLE,
        types::CrateOwners
    );

    Ok(n)
}

fn unpack_crate_categories_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/crates_categories.csv",
        txn,
        types::CRATE_CATEGORIES_TABLE,
        types::CrateCategories
    );

    Ok(n)
}

fn unpack_crate_keywords_csv(profile: &Profile, txn: &mut git::Txn) -> Result<usize> {
    let n = unpack_table!(
        profile,
        "data/crates_keywords.csv",
        txn,
        types::CRATE_KEYWORDS_TABLE,
        types::CrateKeywords
    );

    Ok(n)
}
