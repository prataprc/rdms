use serde::Deserialize;

use std::{ffi, path};

use rdms::{err_at, util, Error, Result};

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
            crates_io_data(&opts, &profile)?;
        }
    };

    //let mut fd = fs::OpenOptions::new()
    //    .read(true)
    //    .open("/media/prataprc/hdd1.4tb/crates-io/2021-11-09-020028/data/crates.csv")
    //    .unwrap();
    //let mut rdr = csv::Reader::from_reader(&mut fd);
    //for (i, result) in rdr.deserialize().enumerate() {
    //    // Notice that we need to provide a type hint for automatic deserialization.
    //    let record: Crate = result.unwrap();
    //    println!("{:?}", record);
    //    if i > 2 {
    //        return;
    //    }
    //}

    Ok(())
}

fn untar(loc: path::PathBuf) -> Result<()> {
    use flate2::read::GzDecoder;
    use tar::Archive;

    use std::{fs, time};

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
    use std::{fs, time};

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
    use std::{env, fs};

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

    err_at!(IOError, fs::remove_dir_all(&temp_dir))
}

fn crates_io_dump_loc(profile: &Profile) -> path::PathBuf {
    use std::{env, fs};

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
    use std::fs;

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
    use std::fs;

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

fn crates_io_data(_opts: &Opt, profile: &Profile) -> Result<()> {
    use std::fs;

    let data_dir: path::PathBuf = [profile.git.loc_repo.clone(), "data".into()]
        .iter()
        .collect();
    fs::create_dir_all(&data_dir).ok();

    let primary_files = [
        "data/categories.csv",
        "data/crates_categories.csv",
        "data/crates.csv",
        "data/crates_keywords.csv",
        "data/dependencies.csv",
        "data/keywords.csv",
        "data/metadata.csv",
        "data/reserved_crate_names.csv",
        "data/teams.csv",
        "data/users.csv",
        "data/version_downloads.csv",
        "data/versions.csv",
    ];

    for file in primary_files.iter() {
        let src_loc: path::PathBuf = [crates_io_untar_dir(profile)?, file.into()]
            .iter()
            .collect();
        let dst_loc: path::PathBuf = [profile.git.loc_repo.clone(), file.to_string()]
            .iter()
            .collect();

        let data = err_at!(IOError, fs::read(&src_loc))?;
        err_at!(IOError, fs::write(&dst_loc, &data))?;

        println!("copied {:?} -> {:?} ... ok", src_loc, dst_loc);
    }

    Ok(())
}

//let mut fd = fs::OpenOptions::new()
//    .read(true)
//    .open("/media/prataprc/hdd1.4tb/crates-io/2021-11-09-020028/data/crates.csv")
//    .unwrap();
//let mut rdr = csv::Reader::from_reader(&mut fd);
//for (i, result) in rdr.deserialize().enumerate() {
//    // Notice that we need to provide a type hint for automatic deserialization.
//    let record: Crate = result.unwrap();
//    println!("{:?}", record);
//    if i > 2 {
//        return;
//    }
//}
