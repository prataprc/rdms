pub struct System {
    pub uname: Uname,
    pub boot_time: chrono::NaiveDateTime,
    pub num_cpu: usize,
    pub cpu_speed: usize, // in MHz
    pub disks: Vec<Disk>,
    pub process: Vec<Process>,
}

pub struct Uname {
    pub host_name: String,
    pub node_name: String,
    pub os_type: OsType,
    pub os_release: String,
    pub os_version: String,
    pub machine: String,
}

pub struct Disk {
    pub name: String,
    pub total: usize,
    pub free: usize,
}

pub enum OsType {
    Linux,
    Darwin,
    Windows,
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

pub struct Process;
