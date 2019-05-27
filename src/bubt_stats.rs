use std::{fmt, fmt::Display, str::FromStr};

use crate::bubt_config::Config;
use crate::error::BognError;
use crate::jsondata::{Json, Property};

#[derive(Clone, Default)]
pub struct Stats {
    pub name: String,
    pub zblocksize: usize,
    pub mblocksize: usize,
    pub vblocksize: usize,
    pub vlog_ok: bool,
    pub vlog_file: Option<String>,
    pub value_in_vlog: bool,

    pub n_count: u64,
    pub n_deleted: usize,
    pub seqno: u64,
    pub keymem: usize,
    pub valmem: usize,
    pub z_bytes: usize,
    pub v_bytes: usize,
    pub m_bytes: usize,
    pub padding: usize,
    pub n_abytes: usize,

    pub buildtime: u64,
    pub epoch: i128,
}

impl From<Config> for Stats {
    fn from(config: Config) -> Stats {
        Stats {
            name: config.name,
            zblocksize: config.z_blocksize,
            mblocksize: config.m_blocksize,
            vblocksize: config.v_blocksize,
            vlog_ok: config.vlog_ok,
            vlog_file: config.vlog_file,
            value_in_vlog: config.value_in_vlog,

            n_count: Default::default(),
            n_deleted: Default::default(),
            seqno: Default::default(),
            keymem: Default::default(),
            valmem: Default::default(),
            z_bytes: Default::default(),
            v_bytes: Default::default(),
            m_bytes: Default::default(),
            padding: Default::default(),
            n_abytes: Default::default(),

            buildtime: Default::default(),
            epoch: Default::default(),
        }
    }
}

impl FromStr for Stats {
    type Err = BognError;

    fn from_str(s: &str) -> Result<Stats, BognError> {
        let js: Json = s.parse()?;
        Ok(Stats {
            name: js.get("/name")?.string().unwrap(),
            zblocksize: js.get("/zblocksize")?.integer().unwrap() as usize,
            mblocksize: js.get("/mblocksize")?.integer().unwrap() as usize,
            vblocksize: js.get("/vblocksize")?.integer().unwrap() as usize,
            vlog_ok: js.get("/vlog_ok")?.boolean().unwrap(),
            vlog_file: Some(js.get("/vlog_file")?.string().unwrap()),
            value_in_vlog: js.get("/value_in_vlog")?.boolean().unwrap(),

            n_count: js.get("/n_count")?.integer().unwrap() as u64,
            n_deleted: js.get("/n_deleted")?.integer().unwrap() as usize,
            seqno: js.get("/seqno")?.integer().unwrap() as u64,
            keymem: js.get("/keymem")?.integer().unwrap() as usize,
            valmem: js.get("/valmem")?.integer().unwrap() as usize,
            z_bytes: js.get("/z_bytes")?.integer().unwrap() as usize,
            v_bytes: js.get("/v_bytes")?.integer().unwrap() as usize,
            m_bytes: js.get("/m_bytes")?.integer().unwrap() as usize,
            padding: js.get("/padding")?.integer().unwrap() as usize,
            n_abytes: js.get("/n_abytes")?.integer().unwrap() as usize,

            buildtime: js.get("/buildtime")?.integer().unwrap() as u64,
            epoch: js.get("/epoch")?.integer().unwrap() as i128,
        })
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut js = Json::new::<Vec<Property>>(vec![]);
        js.set("/name", Json::new(self.name.clone()));
        js.set("/zblocksize", Json::new(self.zblocksize as i128));
        js.set("/mblocksize", Json::new(self.mblocksize as i128));
        js.set("/vblocksize", Json::new(self.vblocksize as i128));
        js.set("/vlog_ok", Json::new(self.vlog_ok));
        js.set(
            "/vlog_file",
            Json::new(self.vlog_file.clone().map_or("".to_string(), From::from)),
        );
        js.set("/value_in_vlog", Json::new(self.value_in_vlog));

        js.set("/n_count", Json::new(self.n_count as i128));
        js.set("/n_deleted", Json::new(self.n_deleted as i128));
        js.set("/seqno", Json::new(self.seqno as i128));
        js.set("/keymem", Json::new(self.keymem as i128));
        js.set("/valmem", Json::new(self.valmem as i128));
        js.set("/z_bytes", Json::new(self.z_bytes as i128));
        js.set("/v_bytes", Json::new(self.v_bytes as i128));
        js.set("/m_bytes", Json::new(self.m_bytes as i128));
        js.set("/padding", Json::new(self.padding as i128));
        js.set("/n_abytes", Json::new(self.n_abytes as i128));

        js.set("/buildtime", Json::new(self.buildtime as i128));
        js.set("/epoch", Json::new(self.epoch));

        write!(f, "{}", js.to_string())
    }
}
