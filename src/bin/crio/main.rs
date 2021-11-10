use rdms::util;

mod cmd_fetch;

fn main() {
    let (_args, cmd, cmd_args) = util::parse_os_args(None);

    let cmd = cmd.to_str().unwrap().to_string();

    let res = match cmd.as_str() {
        "ft" | "fetch" => cmd_fetch::handle(cmd_args),
        cmd => {
            println!("invalid command {}", cmd);
            Ok(())
        }
    };

    res.map_err(|e| println!("Error: {}", e));
}
