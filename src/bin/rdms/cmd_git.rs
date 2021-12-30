use std::ffi;

use rdms::{err_at, Error, Result};

use crate::SubCommand;

pub struct Opt {
    pub loc_repo: Option<ffi::OsString>,
    pub loc_db: Option<ffi::OsString>,
    pub sha1_file: Option<ffi::OsString>,
    pub sha1_text: Option<String>,
}

impl From<crate::SubCommand> for Opt {
    fn from(subcmd: crate::SubCommand) -> Opt {
        match subcmd {
            SubCommand::Git {
                loc_repo,
                loc_db,
                sha1_file,
                sha1_text,
            } => Opt {
                loc_repo,
                loc_db,
                sha1_file,
                sha1_text,
            },
            _ => unreachable!(),
        }
    }
}

pub fn handle(opts: Opt) -> Result<()> {
    if let Some(sha1_file) = opts.sha1_file {
        return handle_sha1_file(sha1_file);
    } else if let Some(sha1_text) = opts.sha1_text {
        return handle_sha1_text(sha1_text);
    }

    Ok(())
}

fn handle_sha1_file(sha1_file: ffi::OsString) -> Result<()> {
    use sha1::{Digest, Sha1};
    use std::fs;

    let git_oid = err_at!(
        FailGitapi,
        git2::Oid::hash_file(git2::ObjectType::Blob, &sha1_file)
    )?;

    let mut hasher = Sha1::new();
    let data = {
        let payload = err_at!(IOError, fs::read(&sha1_file))?;
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

fn handle_sha1_text(sha1_text: String) -> Result<()> {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    let data = {
        let payload = sha1_text.as_bytes();
        let mut header = format!("blob {}\0", payload.len()).as_bytes().to_vec();
        header.extend(payload);
        header
    };
    hasher.update(&data);
    let oid = err_at!(FailGitapi, git2::Oid::from_bytes(&hasher.finalize()))?;

    println!("our-sha1: {}", oid.to_string());

    Ok(())
}
