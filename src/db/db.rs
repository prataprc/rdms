//! Module define all things database related.

use std::{borrow::Borrow, fmt, hash::Hash, ops::Bound};

#[allow(unused_imports)]
use crate::data::{Diff, NoDiff};
use crate::{Error, LocalCborize};

#[cfg(test)]
#[path = "db_test.rs"]
mod db_test;
