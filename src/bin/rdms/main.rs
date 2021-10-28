use structopt::StructOpt;

mod cmd_perf;
mod perf_btree_map;
mod perf_llrb;
mod perf_lmdb;
mod perf_robt;
mod perf_wral;

/// Options for cmd
#[derive(Clone, StructOpt)]
pub struct Opt {
    //
}

fn main() {
    let args_os: Vec<String> = std::env::args_os()
        .map(|s| s.to_str().unwrap().to_string())
        .skip(1)
        .collect();
    let mut iter = args_os.clone().into_iter().enumerate();
    let (_args, cmd, cmd_args) = loop {
        match iter.next() {
            None => break (args_os.clone(), "".to_string(), vec![]),
            Some((i, arg)) if !arg.starts_with("-") && i < (args_os.len() - 1) => {
                break (
                    args_os[..i].to_vec(),
                    args_os[i].clone(),
                    args_os[i..].to_vec(),
                )
            }
            Some((i, arg)) if !arg.starts_with("-") => {
                break (args_os[..i].to_vec(), args_os[i].clone(), vec![])
            }
            _ => (),
        }
    };

    match cmd.as_str() {
        "perf" => cmd_perf::perf(cmd_args),
        "git" => cmd_git::handle(cmd_args),
        cmd => println!("invalid command {}", cmd),
    }
}
