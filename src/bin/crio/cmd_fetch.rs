use serde::Deserialize;

use std::{ffi, path};

use rdms::{err_at, util, Error, Result};

pub struct Opt {
    nohttp: bool,
    nountar: bool,
    git_root: Option<ffi::OsString>,
    profile: ffi::OsString,
}

impl From<crate::SubCommand> for Opt {
    fn from(sub_cmd: crate::SubCommand) -> Self {
        match sub_cmd {
            crate::SubCommand::Fetch {
                nohttp,
                nountar,
                git_root,
                profile,
            } => Opt {
                nohttp,
                nountar,
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
    git_root: String,
    git_index_dir: String,
    git_analytics: String,
}

pub fn handle(opts: Opt) -> Result<()> {
    let profile: Profile = util::files::load_toml(&opts.profile)?;

    let crates_io_dump_loc = match opts.nohttp {
        true => crates_io_dump_loc(&profile),
        false => {
            remove_temp_dir(&profile)?;
            get_latest_db_dump(&profile)?
        }
    };
    match opts.nountar {
        true => (),
        false => untar(crates_io_dump_loc.clone())?,
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

    use std::fs;

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

    println!("untar into {:?} ... ok", loc.parent().unwrap());

    Ok(())
}

fn get_latest_db_dump(profile: &Profile) -> Result<path::PathBuf> {
    use std::fs;
    use std::io::{Read, Write};

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
    println!("fetched {} bytes into {:?} ... ok", m, crates_io_dump_loc);

    Ok(crates_io_dump_loc)
}

fn remove_temp_dir(profile: &Profile) -> Result<()> {
    use std::{env, fs};

    let temp_dir: path::PathBuf = [
        profile
            .temp_dir
            .as_ref()
            .map(|x| x.into())
            .unwrap_or(env::temp_dir()),
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
            .unwrap_or(env::temp_dir()),
        crate::TEMP_DIR_CRIO.into(),
    ]
    .iter()
    .collect();

    fs::create_dir_all(&temp_dir).ok();

    let dump_fname = path::Path::new(profile.dump_url.path())
        .file_name()
        .unwrap();
    [temp_dir.clone(), dump_fname.into()].iter().collect()
}

#[derive(Deserialize)]
struct Category {
    category: String,
    crates_cnt: String,
    created_at: String,
    description: String,
    id: String,
    path: String,
    slug: String,
}

#[derive(Debug, Deserialize)]
struct Crate {
    created_at: String,
    description: String,
    documentation: String,
    downloads: String,
    homepage: String,
    id: String,
    max_upload_size: String,
    name: String,
    readme: String,
    repository: String,
    updated_at: String,
}
