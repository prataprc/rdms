List of types implementing Iterator
===================================

* `core::VersionIter`, iterate over older versions of an entry.

* `llrb_common::Iter`, returned by `Llrb::iter()`, `Mvcc::iter()` for
  full table iteration over Llrb/Mvcc index. Note that iteration will
  block all other operations in the index.
* `llrb_common::IterPWScan`, returned by `Llrb::pw_scan()` for full
  table iteration over Llrb/Mvcc index. Unlike `iter()` this won't
  lock the index for more than ~ 10ms.
* `llrb_common::Range`, returned by `Llrb::range()` and `Mvcc::range()`.
* `llrb_common::Reverse`, returned by `Llrb::reverse()` and `Mvcc::reverse()`.

* `lsm::YIter`, returned by `lsm::y_iter()` for lsm iteration used by
  multi-level indexes like `Dgm` and `WorkingSet`.
* `lsm::YIterVersions`, returned by `lsm::y_iter_versions()` for lsm iteration
  used by multi-level indexes like `Dgm` and `WorkingSet`.

* `robt::BuildScan`
* `robt::CommitScan`
* `robt::Iter`
* `robt::Range`
* `robt::Reverse`
* `robt::MZ`

* `scans::SkipScan`, useful in full-table scan using `pw_scan()` interface.
  Additionally, can be configured to filter entries within a key-range and/or
  `seqno` range. Used to implement CommitIterator for `Llrb` and `Mvcc`.
* `scans::FilterScans`, useful in full-table scan using one or more iterators.
  If more than one iterators are supplied Iterators are chained in stack order.
  Additionally, can be configured to filter entries within a `seqno` range.
* `scans::BitmappedScan`, useful to build a bitmap of all iterated keys.
* `scans::CompactScan`
* `scans::IterChain`
* `types::EmptyIter`

List of types implementing CommitIterator
=========================================

* `scans::CommitWrapper`
* `std::vec::IntoIter`
* `robt::Robt`
* `mvcc::Mvcc`
* `llrb::Llrb`
