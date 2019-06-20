use std::{convert::TryInto, fmt, fmt::Display, str::FromStr};

use crate::error::Error;
use crate::jsondata::{Json, Property};
use crate::robt_config::Config;

#[derive(Clone, Default)]
pub struct Stats {
    pub name: String,
    pub zblocksize: usize,
    pub mblocksize: usize,
    pub vblocksize: usize,
    pub delta_ok: bool,
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
            delta_ok: config.delta_ok,
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Stats, Error> {
        let js: Json = s.parse()?;
        let to_usize = |key: &str| -> Result<usize, Error> {
            let n: usize = js.get(key)?.integer().unwrap().try_into().unwrap();
            Ok(n)
        };
        let to_u64 = |key: &str| -> Result<u64, Error> {
            let n: u64 = js.get(key)?.integer().unwrap().try_into().unwrap();
            Ok(n)
        };
        let vlog_file = match js.get("/vlog_file")?.string().unwrap() {
            s if s.len() == 0 => None,
            s => Some(s),
        };

        Ok(Stats {
            // config fields.
            name: js.get("/name")?.string().unwrap(),
            zblocksize: to_usize("/zblocksize")?,
            mblocksize: to_usize("/mblocksize")?,
            vblocksize: to_usize("/vblocksize")?,
            delta_ok: js.get("/delta_ok")?.boolean().unwrap(),
            vlog_file,
            value_in_vlog: js.get("/value_in_vlog")?.boolean().unwrap(),
            // statitics fields.
            n_count: to_u64("/n_count")?,
            n_deleted: to_usize("/n_deleted")?,
            seqno: to_u64("/seqno")?,
            keymem: to_usize("/keymem")?,
            valmem: to_usize("/valmem")?,
            z_bytes: to_usize("/z_bytes")?,
            v_bytes: to_usize("/v_bytes")?,
            m_bytes: to_usize("/m_bytes")?,
            padding: to_usize("/padding")?,
            n_abytes: to_usize("/n_abytes")?,

            buildtime: to_u64("/buildtime")?,
            epoch: js.get("/epoch")?.integer().unwrap(),
        })
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut js = Json::new::<Vec<Property>>(vec![]);

        js.set("/name", Json::new(self.name.clone())).ok();
        js.set("/zblocksize", Json::new(self.zblocksize)).ok();
        js.set("/mblocksize", Json::new(self.mblocksize)).ok();
        js.set("/vblocksize", Json::new(self.vblocksize)).ok();
        js.set("/delta_ok", Json::new(self.delta_ok)).ok();
        let file = self.vlog_file.clone().map_or("".to_string(), From::from);
        js.set("/vlog_file", Json::new(file)).ok();
        js.set("/value_in_vlog", Json::new(self.value_in_vlog)).ok();

        js.set("/n_count", Json::new(self.n_count)).ok();
        js.set("/n_deleted", Json::new(self.n_deleted)).ok();
        js.set("/seqno", Json::new(self.seqno)).ok();
        js.set("/keymem", Json::new(self.keymem)).ok();
        js.set("/valmem", Json::new(self.valmem)).ok();
        js.set("/z_bytes", Json::new(self.z_bytes)).ok();
        js.set("/v_bytes", Json::new(self.v_bytes)).ok();
        js.set("/m_bytes", Json::new(self.m_bytes)).ok();
        js.set("/padding", Json::new(self.padding)).ok();
        js.set("/n_abytes", Json::new(self.n_abytes)).ok();

        js.set("/buildtime", Json::new(self.buildtime)).ok();
        js.set("/epoch", Json::new(self.epoch)).ok();

        write!(f, "{}", js.to_string())
    }
}
