Key Value store
===============

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

* [ ] CRUD support.
* [ ] ACID compliance.
* [ ] Index held only in memory, useful for caching data.
* [ ] Index held in memory, with as disk backup.
* [ ] Index held in disk.
* [ ] Index held in disk, with working set held in memory.
* [ ] Durability guarantee using Write Ahead Logging.
* [ ] LSM based Multi-level storage on memory and/or disks.
* [ ] Index can be compose using:
  * [ ] Type choice of key.
  * [ ] Type choice of value.
  * [ ] Type choice of memory data-structure. Type can be:
    * [ ] Left leaning red black tree.
    * [ ] Left leaning red black tree, with Multi-version-concurrency-control.
    * [ ] Skip list, with concurrent writers.
  * [ ] Type choice of disk data-structure.
    * [ ] Read only Btree.
    * [ ] Append only Btree.
* [ ] Centralised version control for index entries.
* [ ] Decentralised version control for index entries.
* [ ] Value, along with delta, can be stored in separate log files.

Open design decisions
=====================

Given Rust's threading model, is it more efficient to en-force, using atomic
primitives, that all write operations on MVCC index is deligated to single
thread or is it more efficient to serialize write operations from multiple
threads.

Is it enough to use ``Relaxed`` [memory-ordering][memory-ordering] for
AtomicPtr operations to manage MVCC Snapshots ?

[memory-ordering]: https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html
