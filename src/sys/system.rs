use crate::{Error, Result};

pub struct System {
    pub uname: Uname,
    pub boot_time: chrono::NaiveDateTime,
    pub num_cpu: usize,
    pub cpu_speed: usize, // in MHz
    pub disks: Vec<Disk>,
    pub process: Vec<Process>,
    pub networks: Vec<Network>,
}

#[derive(Clone, Debug)]
pub struct Uname {
    pub host_name: String,
    pub os_type: String,
    pub os_release: String,
}

impl Uname {
    pub fn new() -> Result<Uname> {
        let host_name = err_at!(IOError, sys_info::hostname())?;
        let os_type = err_at!(IOError, sys_info::os_type())?;
        let os_release = err_at!(IOError, sys_info::os_release())?;

        let val = Uname { host_name, os_type, os_release };

        Ok(val)
    }
}

pub struct Disk {
    pub name: String,
    pub total: usize,
    pub free: usize,
}

pub struct LoadAvg {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

pub struct MemInfo {
    pub total: usize,
    pub free: usize,
    pub avail: usize,
    pub buffers: usize,
    pub cached: usize,
    pub swap_total: usize,
    pub swap_free: usize,
}

pub struct Network {
    pub node_name: String,
}

pub struct Process;

#[cfg(test)]
#[path = "system_test.rs"]
mod system_test;
