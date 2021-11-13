use std::ffi;

use rdms::{err_at, Error, Result};

use crate::SubCommand;

pub struct Opt {
    pub loc_repo: Option<ffi::OsString>,
    pub loc_db: Option<ffi::OsString>,
    pub hash_file: Option<ffi::OsString>,
}

impl From<crate::SubCommand> for Opt {
    fn from(subcmd: crate::SubCommand) -> Opt {
        match subcmd {
            SubCommand::Git {
                loc_repo,
                loc_db,
                hash_file,
            } => Opt {
                loc_repo,
                loc_db,
                hash_file,
            },
            _ => unreachable!(),
        }
    }
}

pub fn handle(opts: Opt) -> Result<()> {
    if let Some(hash_file) = opts.hash_file {
        return handle_hash_file(hash_file);
    }

    Ok(())
}

fn handle_hash_file(hash_file: ffi::OsString) -> Result<()> {
    use sha1::{Digest, Sha1};
    use std::fs;

    let git_oid = err_at!(
        FailGitapi,
        git2::Oid::hash_file(git2::ObjectType::Blob, &hash_file)
    )?;

    let mut hasher = Sha1::new();
    let data = {
        let payload = err_at!(IOError, fs::read(&hash_file))?;
        let mut header = format!("blob {}\0", payload.len()).as_bytes().to_vec();
        header.extend(&payload);
        header
    };
    hasher.update(&data);
    let our_oid = err_at!(FailGitapi, git2::Oid::from_bytes(&hasher.finalize()))?;

    println!("git-sha1: {}", git_oid.to_string());
    println!("our-sha1: {}", our_oid.to_string());

    Ok(())
}
