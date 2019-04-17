Key Value store
===============

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

* [ ] In memory storage.
* [ ] Multi-version Concurrency Control for in memory storage.
* [ ] Fully packed immutable disk store.
* [ ] LSM based Multi-level storage on memory and disks.
* [ ] Bogn.
* [ ] ACID compliance.
* [ ] Memory optimised LLRB, Left Leaning Red black tree.
* [ ] Append only btree.

Open design decisions
=====================

Given Rust's threading model, is it more efficient to en-force, using atomic
primitives, that all write operations on MVCC index is deligated to single
thread or is it more efficient to serialize write operations from multiple
threads.

Is it enough to use ``Relaxed`` [memory-ordering][memory-ordering] for
AtomicPtr operations to manage MVCC Snapshots ?

[memory-ordering]: https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html
