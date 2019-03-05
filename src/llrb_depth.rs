/// Depth calculates minimum, maximum, average and percentile of leaf-node
/// depth in the LLRB tree.
#[derive(Clone)]
pub struct Depth {
    samples: usize,
    min: usize,
    max: usize,
    total: usize,
    depths: [usize; 256],
}

impl Depth {
    pub(crate) fn sample(&mut self, depth: usize) {
        self.samples += 1;
        self.total += depth;
        if self.min == 0 || self.min > depth {
            self.min = depth
        }
        if self.max == 0 || self.max < depth {
            self.max = depth
        }
        self.depths[depth as usize] += 1;
    }

    /// Return number of leaf-nodes sample for depth in LLRB tree.
    pub fn samples(&self) -> usize {
        self.samples
    }

    /// Return minimum depth of leaf-node in LLRB tree.
    pub fn min(&self) -> usize {
        self.min
    }

    /// Return the average depth of leaf-nodes in LLRB tree.
    pub fn mean(&self) -> usize {
        self.total / self.samples
    }

    /// Return maximum depth of leaf-node in LLRB tree.
    pub fn max(&self) -> usize {
        self.max
    }

    /// Return depth as tuple of percentiles, each tuple provides
    /// (percentile, depth).
    pub fn percentiles(&self) -> Vec<(u8, usize)> {
        let mut percentiles: Vec<(u8, usize)> = vec![];
        let (mut acc, mut prev_perc) = (0_f64, 89_u8);
        let iter = self.depths.iter().enumerate().filter(|(_, &item)| item > 0);
        for (depth, samples) in iter {
            acc += *samples as f64;
            let perc = ((acc / (self.samples as f64)) * 100_f64) as u8;
            if perc > prev_perc {
                percentiles.push((perc, depth));
                prev_perc = perc;
            }
        }
        percentiles
    }

    pub fn pretty_print(&self, prefix: &str) {
        let mean = self.mean();
        println!(
            "{}depth (min, max, avg): {:?}",
            prefix,
            (self.min, mean, self.max)
        );
        for (depth, n) in self.percentiles().into_iter() {
            if n > 0 {
                println!("{}  {} percentile = {}", prefix, depth, n);
            }
        }
    }

    pub fn json(&self) -> String {
        let ps: Vec<String> = self
            .percentiles()
            .into_iter()
            .map(|(d, n)| format!("{}: {}", d, n))
            .collect();
        let strs = [
            format!("min: {}", self.min),
            format!("mean: {}", self.mean()),
            format!("max: {}", self.max),
            format!("percentiles: {}", ps.join(", ")),
        ];
        ("{ ".to_string() + strs.join(", ").as_str() + " }").to_string()
    }
}

impl Default for Depth {
    fn default() -> Self {
        Depth {
            samples: 0,
            min: 0,
            max: 0,
            total: 0,
            depths: [0; 256],
        }
    }
}
