pub enum BognError {
    InvalidCAS,
    ConsecutiveReds,
    UnbalancedBlacks(String),
    SortError(String),
}
