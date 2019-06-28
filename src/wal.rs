// Takes care of, batching entries, serializing and appending them to disk,
// commiting the appended batch(es).

use std::fs;

use crate::core::Serialize;
use crate::error::Error;
use crate::type_empty::Empty;

const BatchMarker: &'static str = "vawval-treatment";

// <{name}-shard-{num}>/
// ..
// <{name}-shard-{num}>/
struct Wal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    name: String,
    seqno: u64,
    shards: Vec<Path>,
}

// <{name}-shard-{num}>/{name}-shard{num}-journal-{num}.log
//                      ..
//                      {name}-shard{num}-journal-{num}.log
struct Shard<K, V>
where
    K: Serialize,
    V: Serialize,
{
    num: usize,
    journals: Vec<Journal>,
}

// <{name}-shard-{num}>/{name}-shard{num}-journal-{num}.log
struct Journal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    num: usize,
    file: String,
    fd: fs::File,
    batches: Vec<Batch<K, V>>,
}

struct Batch<K, V> {
    entries: Vec<Entry<K, V>>,
    state: State,
}

struct State {
    // state: List of participating entities.
    config: Vec<String>,
    // state: term is current term for all entries in a batch.
    term: u64,
    // state: committed says index upto this index-seqno is
    // replicated and persisted in majority of participating nodes,
    // should always match with first-index of a previous batch.
    commited: u64,
    // state: persisted says index upto this index-seqno is persisted
    // in the snapshot, Should always match first-index of a commited
    // batch.
    persisted: u64,
    // state: votedfor is the leader's address in which this batch
    // was created.
    votedfor: String,
}
