use std::ffi;

/// Parse `args` (if args is None, [std::env::args_os] is used) and return list
/// of args before `sub-command` list of args after `sub-command`.
///
/// Return type: `(cmd-args, sub-cmd, subcmd-args)`
pub fn parse_os_args(
    args: Option<Vec<ffi::OsString>>,
) -> (Vec<ffi::OsString>, ffi::OsString, Vec<ffi::OsString>) {
    let args_os: Vec<ffi::OsString> = {
        args
            // while taking from std::env skip the first item, it is command-line
            .unwrap_or_else(|| std::env::args_os().skip(1).collect())
            .into_iter()
            // .map(|s| s.to_str().unwrap().to_string())
            .collect()
    };

    let mut iter = args_os.clone().into_iter().enumerate();

    let is_cmd_option = |arg: &ffi::OsString| -> bool {
        matches!(arg.to_str(), Some(arg) if arg.starts_with('-'))
    };

    loop {
        match iter.next() {
            None => break (args_os, ffi::OsString::new(), vec![]),
            Some((i, arg)) if !is_cmd_option(&arg) && i < (args_os.len() - 1) => {
                break (
                    args_os[..i].to_vec(),
                    args_os[i].clone(),
                    args_os[i..].to_vec(),
                )
            }
            Some((i, arg)) if !is_cmd_option(&arg) => {
                break (args_os[..i].to_vec(), args_os[i].clone(), vec![])
            }
            _ => (),
        }
    }
}
