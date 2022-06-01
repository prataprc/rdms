use serde::Deserialize;

use std::path;

use rdms::util::files;

// TODO: implement glob-filtering for excluded_dirs and include_dirs

#[derive(Clone, Default)]
pub struct Config {
    pub hot: Option<usize>,
    pub cold: Option<usize>,
    pub scan: Scan,
}

#[derive(Clone, Default)]
pub struct Scan {
    pub scan_dirs: Vec<path::PathBuf>,
    pub exclude_dirs: Vec<path::PathBuf>,
}

#[derive(Clone, Deserialize)]
pub struct TomlConfig {
    hot: Option<usize>,  // in months
    cold: Option<usize>, // in months
    scan: Option<TomlScan>,
}

impl From<TomlConfig> for Config {
    fn from(cfg: TomlConfig) -> Config {
        Config {
            hot: cfg.hot,
            cold: cfg.cold,
            scan: cfg.scan.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct TomlScan {
    scan_dirs: Option<Vec<path::PathBuf>>,
    exclude_dirs: Option<Vec<path::PathBuf>>,
}

impl From<Option<TomlScan>> for Scan {
    fn from(toml_scan: Option<TomlScan>) -> Scan {
        match toml_scan {
            Some(toml_scan) => {
                let scan_dirs = toml_scan
                    .scan_dirs
                    .unwrap_or_else(|| vec![])
                    .iter()
                    .filter_map(files::canonicalize)
                    .collect();

                Scan {
                    scan_dirs,
                    exclude_dirs: toml_scan.exclude_dirs.unwrap_or_else(|| vec![]),
                }
            }
            None => Scan { scan_dirs: vec![], exclude_dirs: vec![] },
        }
    }
}
