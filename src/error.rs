#[derive(Debug, PartialEq)]
pub enum BognError {
    InvalidCAS,
    ConsecutiveReds,
    UnbalancedBlacks(String),
    SortError(String),
    DuplicateKey(String),
}
