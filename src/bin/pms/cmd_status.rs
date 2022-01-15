use serde::Deserialize;

use std::{convert::TryFrom, ffi, fs, path};

use crate::{repo, SubCommand};

use rdms::{
    trie,
    util::{files, print},
    Error, Result,
};

#[derive(Clone)]
pub struct Opt {
    pub ignored: bool,
    pub force_color: bool,
    pub loc_toml: Option<path::PathBuf>,
    pub profile: Profile, // toml options
}

#[derive(Clone, Default)]
pub struct Profile {
    hot: Option<usize>,
    cold: Option<usize>,
    scan_dirs: Vec<path::PathBuf>,
    exclude_dirs: Vec<path::PathBuf>,
}

#[derive(Clone, Deserialize)]
pub struct TomlProfile {
    hot: Option<usize>,  // in months
    cold: Option<usize>, // in months
    scan_dirs: Option<Vec<path::PathBuf>>,
    exclude_dirs: Option<Vec<path::PathBuf>>,
}

impl From<TomlProfile> for Profile {
    fn from(p: TomlProfile) -> Profile {
        Profile {
            hot: p.hot,
            cold: p.cold,
            scan_dirs: p.scan_dirs.unwrap_or_else(|| vec![]),
            exclude_dirs: p.exclude_dirs.unwrap_or_else(|| vec![]),
        }
    }
}

impl TryFrom<crate::SubCommand> for Opt {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Opt> {
        let opt = match subcmd {
            SubCommand::Status {
                loc,
                ignored,
                force_color,
                toml,
            } => {
                let loc_toml = files::find_config(toml, &["pms.toml", ".pms.toml"]);
                let mut profile = match loc_toml.as_ref() {
                    Some(loc_toml) => {
                        files::load_toml::<_, TomlProfile>(loc_toml)?.into()
                    }
                    None => Profile::default(),
                };

                loc.map(|loc| profile.scan_dirs.push(loc.into()));

                Opt {
                    ignored,
                    force_color,
                    loc_toml,
                    profile,
                }
            }
        };

        Ok(opt)
    }
}

#[derive(Clone)]
struct WalkState {
    scan_dir: path::PathBuf,
    opts: Opt,
    repos: Vec<repo::Repo>,
}

pub fn handle(opts: Opt) -> Result<()> {
    let walk_state = {
        let mut walk_state = WalkState {
            scan_dir: path::PathBuf::default(),
            opts: opts.clone(),
            repos: vec![],
        };
        for scan_dir in opts.profile.scan_dirs.iter() {
            walk_state.scan_dir = scan_dir.clone();
            let parent = path::Path::new(scan_dir).parent().unwrap();
            make_repo(&mut walk_state, &parent, &files::dir_entry(scan_dir)?)?;
            walk_state = files::walk(scan_dir, walk_state, check_dir_entry)?;
        }
        walk_state
    };

    let index =
        walk_state
            .repos
            .into_iter()
            .fold(trie::Trie::new(), |mut index, repo| {
                let comps: Vec<ffi::OsString> = path::PathBuf::from(&repo.to_loc())
                    .components()
                    .map(|c| c.as_os_str().to_os_string())
                    .collect();
                index.set(&comps, repo);
                index
            });

    let mut repos: Vec<repo::Repo> = index
        .walk(Vec::<repo::Repo>::default(), |repos, _, _, value, _, _| {
            value.map(|repo| repos.push(repo.clone()));
            Ok(trie::WalkRes::Ok)
        })?
        .into_iter()
        .filter(|r| !files::is_excluded(&r.to_loc(), &opts.profile.exclude_dirs))
        .collect();

    repos.sort_unstable_by_key(|r| r.to_last_commit_date());

    print::make_table(&mut repos).print_tty(opts.force_color);

    Ok(())
}

fn check_dir_entry(
    walk_state: &mut WalkState,
    parent: &path::Path,
    entry: &fs::DirEntry,
    _depth: usize,
    _breath: usize,
) -> Result<files::WalkRes> {
    if let Some(".git") = entry.file_name().to_str() {
        Ok(files::WalkRes::SkipDir)
    } else {
        make_repo(walk_state, parent, entry)?;
        Ok(files::WalkRes::Ok)
    }
}

fn make_repo(
    walk_state: &mut WalkState,
    parent: &path::Path,
    entry: &fs::DirEntry,
) -> Result<()> {
    match repo::Repo::from_entry(parent, entry).ok() {
        Some(mut repo) => {
            repo.set_ignored(walk_state.opts.ignored)
                .set_hot(walk_state.opts.profile.hot)
                .set_cold(walk_state.opts.profile.cold);
            walk_state.repos.push(repo);
        }
        None => (),
    };

    Ok(())
}
