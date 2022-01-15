pub fn make_diff_options(ignored: bool) -> git2::DiffOptions {
    let mut dopts = git2::DiffOptions::new();
    dopts
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(ignored)
        .recurse_ignored_dirs(ignored)
        .ignore_filemode(false)
        .ignore_submodules(false)
        .include_unreadable(true);
    dopts
}

pub fn get_reference_kind(refrn: &git2::Reference) -> String {
    refrn
        .kind()
        .map(|k| k.to_string())
        .unwrap_or_else(|| "".to_string())
}

pub fn get_reference_shorthand(refrn: &git2::Reference) -> String {
    refrn
        .shorthand()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string())
}
