use std::{fmt, result};

use crate::db;

/// Statistic type, that captures minimum, maximum, average and percentile of
/// leaf-node depth in the LLRB tree.
#[derive(Clone)]
pub struct Depth {
    pub samples: usize,
    pub min: usize,
    pub max: usize,
    pub total: usize,
    pub depths: [u64; 256],
}

impl Depth {
    /// Record a sample, each sample specify the depth of single branch from root to
    /// leaf-node.
    pub fn sample(&mut self, depth: usize) {
        self.samples += 1;
        self.total += depth;
        self.min = usize::min(self.min, depth);
        self.max = usize::max(self.max, depth);
        self.depths[depth] += 1;
    }

    /// Return number of sample recorded
    pub fn to_samples(&self) -> usize {
        self.samples
    }

    /// Return minimum depth of leaf-node in LLRB tree.
    pub fn to_min(&self) -> usize {
        self.min
    }

    /// Return maximum depth of leaf-node in LLRB tree.
    pub fn to_max(&self) -> usize {
        self.max
    }

    /// Return the average depth of leaf-nodes in LLRB tree.
    pub fn to_mean(&self) -> usize {
        self.total / self.samples
    }

    /// Return depth as tuple of percentiles, each tuple provides
    /// (percentile, depth). Returned percentiles from 91 .. 99
    pub fn to_percentiles(&self) -> Vec<(u8, usize)> {
        let mut percentiles: Vec<(u8, usize)> = vec![];
        let (mut acc, mut prev_perc) = (0_u64, 90_u8);
        let iter = self.depths.iter().enumerate().filter(|(_, &item)| item > 0);
        for (depth, samples) in iter {
            acc += *samples;
            let perc = ((acc as f64 / (self.samples as f64)) * 100_f64) as u8;
            if perc > prev_perc {
                percentiles.push((perc, depth));
                prev_perc = perc;
            }
        }
        percentiles
    }

    // TODO: figure where this is needed
    //pub fn merge(self, other: Self) -> Self {
    //    let mut depths = Depth {
    //        samples: self.samples + other.samples,
    //        min: cmp::min(self.min, other.min),
    //        max: cmp::max(self.max, other.max),
    //        total: self.total + other.total,
    //        depths: [0; 256],
    //    };
    //    for i in 0..depths.depths.len() {
    //        depths.depths[i] = self.depths[i] + other.depths[i];
    //    }
    //    depths
    //}
}

impl fmt::Display for Depth {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let (m, n, x) = (self.to_min(), self.to_mean(), self.to_max());
        let props: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(perc, depth)| format!(r#""{}" = {}"#, perc, depth))
            .collect();
        let depth = props.join(", ");

        write!(
            f,
            concat!(
                "{{ samples={}, min={}, mean={}, max={}, ",
                "percentiles={{ {} }} }}"
            ),
            self.samples, m, n, x, depth
        )
    }
}

impl db::ToJson for Depth {
    fn to_json(&self) -> String {
        let props: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(d, n)| format!(r#""{}": {}"#, d, n))
            .collect();
        let strs = [
            format!(r#""samples": {}"#, self.to_samples()),
            format!(r#""min": {}"#, self.to_min()),
            format!(r#""mean": {}"#, self.to_mean()),
            format!(r#""max": {}"#, self.to_max()),
            format!(r#""percentiles": {{ {} }}"#, props.join(", ")),
        ];
        format!(r#"{{ {} }}"#, strs.join(", "))
    }
}

impl Default for Depth {
    fn default() -> Self {
        Depth {
            samples: 0,
            min: std::usize::MAX,
            max: std::usize::MIN,
            total: 0,
            depths: [0; 256],
        }
    }
}

#[cfg(test)]
#[path = "depth_test.rs"]
mod depth_test;
