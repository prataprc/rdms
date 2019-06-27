// Takes care of, batching entries, serializing and appending them to disk,
// commiting the appended batch(es).

use std::fs;

use crate::core::Serialize;
use crate::error::Error;
use crate::type_empty::Empty;

struct Journal<K, V>
where
    K: Serialize,
    V: Serialize,
{
    file: String,
    fd: fs::File,
    batches: Vec<Batch<K, V>>,
}

enum Batch<K, V>
where
    K: Serialize,
    V: Serialize,
{
    Refer {
        fpos: u64,
    },
    Data {
        entries: Vec<Entry<K, V>>,
        state: State,
    },
    Config {
        entries: Vec<Entry<K, V>>,
        state: State,
    },
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

struct Entry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    // Term in which the entry is created.
    term: u64,
    // Index seqno for this entry. This will be monotonically increasing
    // number without any break.
    index: u64,
    // Id of client applying this entry. To deal with false negatives.
    client_id: u64,
    // Client seqno monotonically increasing number. To deal with
    // false negatives.
    client_seqno: u64,
    // Operation on host data structure.
    op: Op<K, V>,
}

const BatchMarker: &'static str = "vawval-treatment";
