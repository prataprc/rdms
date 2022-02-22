use std::convert::TryInto;
use std::time;

use rdms::{err_at, Error, Result};

pub struct Stats {
    tp_second: time::SystemTime,
    tp_current: u64,
    pub file_size: u64,
    pub sync_latencies: Vec<u64>,
    pub throughputs: Vec<u64>,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            tp_second: time::SystemTime::now(),
            tp_current: 0,
            sync_latencies: vec![],
            throughputs: vec![],
            file_size: Default::default(),
        }
    }

    pub fn click(&mut self, start: time::SystemTime, size: u64) -> Result<()> {
        if err_at!(Fatal, self.tp_second.elapsed())?.as_secs() == 1 {
            self.throughputs.push(self.tp_current);
            self.tp_second = time::SystemTime::now();
            self.tp_current = 0;
        } else {
            self.tp_current += size;
        }
        self.sync_latencies.push(
            err_at!(Fatal, start.elapsed())?
                .as_micros()
                .try_into()
                .unwrap(),
        );

        Ok(())
    }

    pub fn join(&mut self, other: Stats) {
        self.sync_latencies.extend_from_slice(&other.sync_latencies);
        self.throughputs.resize(other.throughputs.len(), 0);
        self.throughputs
            .iter_mut()
            .zip(other.throughputs.iter())
            .for_each(|(x, y)| *x += *y);
        self.file_size += other.file_size;
    }
}
