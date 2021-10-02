//! Package implement an immutable read-only BTree index.
//!
//! Use [Builder] type to build a new index. And subsequently load the index using
//! the [Index] type. Index can be concurrently accessed by cloning the `Index`
//! instance. Note that a single Index instance cannot be shared among threads.
//! Once an index is built using the `Builder` type it is not possible to modify
//! them. While strict immutability might seem like an inconvenience, they have
//! certain advantages,
//!
//! * They are fully packed and hence less overhead and lesser tree depth.
//! * Easy and efficient caching of btree-blocks.
//! * Can be easily paired with immutable read-only bloom filters.
//! * Friendly for concurrent access.
//!
//! **Inventory of features**
//!
//! * Index can be parametrized over Key-type and Value-type.
//! * Uses [CBOR][cbor] for serialization and deserialization.
//! * Key and Value types can be made `robt` compliant by `derive(Cborize)`.
//! * Value can either be stored in leaf-node or in a separate log-file.
//! * Additionally, incoming iterator, to build index, can supply older
//!   versions for value using the [Diff] mechanics.
//! * Bloom filter can help optimize false lookups.
//! * API `get()` operation, with bloom-filter support.
//! * API `iter()` and `reverse()` operation for forward and reverse iteration.
//! * API `iter_version()` and `reverse_version()` operation similar to
//!   iter/reverse but also fetches older versions for a entry. Note that
//!   iter/reverse do not fetch the older versions.
//!
//! **Value-log file**
//!
//! Values and its deltas (older versions) can be stored in a separate log file. This
//! has following advantage,
//!
//! * Keep the leaf-node extremely compact and help better caching.
//! * Efficient when building multi-level index.
//! * Applications typically deal with older-versions as archives.
//!
//! While storing value in the value-log file is optional, deltas are always
//! stored in separate value-log file. Note that a separate value-log-file shall
//! be created only when [Config::value_in_vlog] option is true or [Config::delta_ok]
//! option is true.
//!
//! **Building an index**
//!
//! Unlike mutable data-structure, that support `set()`, `write()`,
//! `update()` etc.. `robt` indexes are built from pre-sorted iterators.
//! In a way each btree index can be seen as an immutable snapshot of
//! sorted `{key,value}` dataset.
//! Typical workflow is,
//!
//! ```ignore
//! use mkit::traits::BuildIndex;
//!
//! let config = Config::new("/opt/data/", "movies");
//! // use one or more set_ method to configure the btree parameters.
//! let builder = Build::initial(config, app_meta);
//! builder.from_iter(iter, mkit::nobitmap::NoBitmap);
//!
//! // Subsequently open an index as,
//! let reader1 = Index::open("/opt/data", "movies").expect("fail");
//! // create another concurrent reader
//! let reader2 = reader.clone();
//! let handle = thread::spawn(|| reader2);
//! ```
//!
//! Let us look at the steps one by one:
//!
//! * First create a configuration. More configurations available via the
//!  `set_` methods.
//! * By supplying `app_meta`, caller can also persist snapshot specific
//!   meta-data.
//! * After creating a builder, use `BuildIndex` trait's `from_iter()` to
//!   build a btree-index from an iterator. It is expected that iterated
//!   entries are pre-sorted.
//! * Caller can optionally pass a bitmap instance that shall be used
//!   for implementing a [bloom filter][bloom-filter].
//! * Bitmap type is parametrized via the `BuildIndex` trait. If
//!   probabilistic bitmap table is not required, pass `NoBitmap` value
//!   to `from_iter()` method.
//!
//! In the example above, we are using `initial()` constructor to create
//! a builder instance, it is also possible to incrementally build an
//! index via `incremental()` constructor. To understand the difference
//! we shall dig deeper into how data-set is indexed with `robt`.
//!
//! `robt` is a simple btree-index, made up of `root-node`,
//! `intermediate-node` (called m-block) and `leaf-node` (called z-block).
//! The entire dataset is maintained in the leaf node and the intermediate
//! nodes are constructed in bottoms-up fashion using the first-key in the
//! leaf-node, all the way up to the root-node. The shape and behavior of
//! root-node is exactly same as the `intermediate-node`.
//!
//! The dataset is made up of entries and each entries is made up of key,
//! value, seqno, a flag to denoted whether the node was deleted or upserted.
//! Reason for maintaining seqno, and deleted-flag is to support database
//! features like vector-timestamping, log-structured-merge etc..
//!
//! **Version control your values**, an additional feature with `robt`
//! index is that applications can version control their values. That is,
//! each entry, along with key, value, seqno, etc.. also maintains previous
//! version of the value along with its modification seqno. And instead of
//! persisting the entire value (older versions), their deltas as computed
//! in relation to its new-versions and persisted as deltas. This is
//! achieved using the [Diff] mechanics. Also note that `robt` itself
//! doesn't compute the version deltas, but it is treated as part of an
//! entry and persisted.
//!
//! Each entry in the index is defined as Entry<K, V, D> type and defined
//! in a common crate. Note that an index entry is parametrized over
//! key-type, value-type, and delta-type. Here delta-type `D` can be
//! `NoDiff` if application is not interested in preserving older-versions
//! or should be same as `<V as Diff>::D`. Refer to [Diff] mechanics for
//! more detail.
//!
//! Now coming back to the leaf-node, all entries are stored in the
//! leaf-node. And to facilitate archival of older versions `deltas`
//! are persisted in a separate value-log file. And optionally, to
//! facilitate incremental build, value can also be persisted in the
//! value-log file. When both values and deltas are persisted in a
//! separate value-log file, leaf nodes become very compact and ends
//! up suitable for caching, compaction, incremental-build, optimized
//! IOPS and delta-archival.
//!
//! **Reading from index**
//!
//! All read operations are done via [Index] type. Use the same arguments
//! passed to `initial()` or `incremental()` constructors to `open()` an
//! existing index for reading.
//!
//! _Cloning an index for concurrency_. Though applications can use the
//! `open()` call to create as many needed instance of an Index, the
//! recommended approach is to call `try_clone()` on Index. This will share
//! the underlying data-structure to avoid memory bloat across several
//! instance of same Index. Only meta-data is shared across index instance
//! (when it is cloned), every index instance will keep an open
//! file-descriptor for underlying file(s).
//!
//! **Simple Key-Value index**
//!
//! `robt` indexes are parametrized over key-type, value-type, delta-type,
//! and bitmap-type. `delta-type` implement the older versions of value-type
//! as delta-difference. `bitmap-type` implement bloom filter to optimize
//! away missing-lookups.
//!
//! In most cases, `delta-type` and `bitmap-type` are not needed. To build
//! and use simple `{key,value}` index [Builder] and [Index] type in the
//! crate-root can be used. To use fully parameterized variant, use
//! [db::Builder] and [db::Index] types.
//!
//! **Index Entry**
//!
//! For simple indexing, `key` and `value` are enough. But to implement
//! database-features like compaction, log-structured-merge we need to
//! preserve more information about each entry. While the internal shape of
//! entry is not exposed (for future compatibility), `robt` uses
//! [mkit::db::Entry] as the default index-entry.
//!
//! **Compaction**
//!
//! Compaction is the process of de-duplicating/removing entries
//! and/or older-versions from an index snapshots (aka instance). In `robt`
//! compaction operation consumes the `Index` instance and creates a new
//! a new `Index` instance with its entries compacted to the desired level
//! of `cutoff`. There are three types of compaction:
//!
//! _deduplication_
//!
//! This is basically applicable for snapshots that don't have to preserve
//! older versions or deleted entries.
//!
//! When same value-log file is used to incrementally build newer batch of
//! mutations older values gets duplicated. This requires a periodic clean up
//! of garbage values to reduce disk foot-print.
//!
//! This type of compaction is also applicable for index instances that
//! do not need distributed [LSM]. In such cases, the oldest snapshot
//! can compact away older versions of each entry and purge entries that are
//! marked deleted.
//!
//! _lsm-compaction_
//!
//! This is applicable for database index that store their index as multi-level
//! snapshots, similar to [leveldb][leveldb]. Each snapshot can be built as
//! `robt` [Index]. Most of the lsm-based-storage will have their root snapshot
//! as the oldest and only source of truth, but this is not possible for
//! distributed index that ends up with multiple truths across different nodes.
//! To facilitate such designs, in lsm mode, even the root level at any given
//! node, can retain older versions upto a specified `seqno`, that `seqno` is
//! computed through eventual consistency.
//!
//! Another use case of lsm-compaction is to maintain older versions of value.
//!
//! _tombstone-compaction_
//!
//! Tombstone compaction is similar to `lsm-compaction` with one main
//! difference. When application logic issue `tombstone-compaction` only
//! deleted entries that are older than specified seqno will be purged.
//!
//! [bloom-filter]: https://en.wikipedia.org/wiki/Bloom_filter
//! [cbor]: https://en.wikipedia.org/wiki/CBOR
//! [LSM]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
//! [leveldb]: https://en.wikipedia.org/wiki/LevelDB

mod build;
mod config;
mod entry;
mod files;
mod flush;
mod index;
mod marker;
mod reader;
mod scans;
mod vlog;

pub use config::{
    to_index_location, to_vlog_location, Config, Stats, FLUSH_QUEUE_SIZE, MBLOCKSIZE,
    VBLOCKSIZE, ZBLOCKSIZE,
};
pub use files::{IndexFileName, VlogFileName};
pub use index::{Builder, Index};

use entry::Entry;
use flush::Flusher;
use marker::ROOT_MARKER;
