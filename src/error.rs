/// BognError enumerates over all possible errors that this package
/// can return.
#[derive(Debug, PartialEq)]
pub enum BognError {
    InvalidCAS,
    ConsecutiveReds,
    UnbalancedBlacks(String),
    SortError(String),
    DuplicateKey(String),
}
