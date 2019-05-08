Symbols used to describe rest of this writeup.

M  : Memory index, whose life time equal the life time of bogn-index.
     Applicable only in "memory" and "backup" configuration.
Mw : Transient memory index, to handles all write operation. It is a
     ephimeral index that will become "Mf" index when it reaches a
     certian size or when "flush-time" elapses. Applicable only in
     "random-dgm" and "working-set-dgm" configuration.
Mf : Transient memory index, that was "Mw" in previous cycle. It is a
     read-only index that will be merged/flushed to disk and forgotten.
     Applicable only in "random-dgm" and "working-set-dgm" configuration.
Mc : Memory index, used to cache read operations from disk. It caches only
     the latest value for an entry. Applicable only in "working-set-dgm"
     configuration.
Dz : Last disk snapshot, typically the last level. Do not preserve deltas
     in "non-delta" mode.
D1 : First of the two disk snapshot used in compaction.
D2 : Second of the two disk snapshot used in compaction.
Dm : Target level for disk snapshot during compaction.
Da : A new disk snapshot, typically one level before the first "active-level".
D  : The "only-snapshot" on disk. Do not preserve deltas in "non-delta" mode.
Vm : Value in Memory.
Vd : Value in disk.
V' : Value refering to disk.
Δ  : Delta, when prefixed with M or D index, means the index shall carry
     delta values.

{Da', D1', D2', Dz'} -> Next version of same level.

DGM or dgm = Disk-Greater-than-Memory. This happens when total data-set
cannot fit into a single memory index.

Configurations
==============

broad sketch: "memory", "backup", "random-dgm", "working-set-dgm"

"memory" configuration
    "no-lsm", no-disk, delta/non-delta mode.

    {M}

    {MΔ}

"backup" configuration
    "lsm", single-level disk, delta/non-delta mode.

    {M, D}
        MΔ + D -> D                 "backup-cycle"
        Delta is required to handle deleted operation.

    {MΔ, DΔ}
        MΔ + DΔ -> DΔ               "backup-cycle"

    This configuration shall have "Value-Delta-reference" to Disk.

"random-dgm" configuration
    "lsm", multi-level disk, delta/non-delta mode.

    {Mw,Mf, Da,D1,D2,Dz}
        Mf -> Da                    "flush-cycle"
        Mf + Da -> Dm               "incremental-compact-cycle"
        Mf + Da -> Da'              "incremental-flush-cycle"
        D1 + D2 -> Dm               "compact-file-cycle"
        D1 + D2 -> D2'              "compact-cycle

    {MwΔ,MfΔ, DaΔ,D1Δ,D2Δ,DzΔ}
        MfΔ -> DaΔ                  "flush-cycle"
        MfΔ + DaΔ -> DmΔ            "incremental-compact-cycle"
        MfΔ + DaΔ -> Da'Δ           "incremental-flush-cycle"
        D1Δ + D2Δ -> DmΔ            "compact-file-cycle"
        D1Δ + D2Δ -> D2'Δ           "compact-cycle"

    Dz won't have deltas if configured in non-delta mode. But all other
    disk snapshot shall have deltas.

    Memory snapshots shall always be persisted shall always be persisted
    to first disk snapshot.

"working-set-dgm" configuration
    "lsm", multi-level disk, delta/non-delta mode.

    {Mw,Mf,Mc Da,D1,D2,Dz}
        Mf + Mc -> Da               "flush-cycle"
        Mf + Mc + Da -> Dm          "incremental-compact-cycle"
        Mf + Mc + Da -> Da'         "incremental-flush-cycle"
        D1 + D2 -> Dm               "compact-file-cycle"
        D1 + D2 -> D2'              "compact-cycle"

    {MwΔ,MfΔ,McΔ, DaΔ,D1Δ,D2Δ,DzΔ}
        MfΔ + McΔ -> DaΔ            "flush-cycle"
        MfΔ + McΔ + DaΔ -> DmΔ      "incremental-compact-cycle"
        MfΔ + McΔ + DaΔ -> Da'Δ     "incremental-flush-cycle"
        D1Δ + D2Δ -> DmΔ            "compact-file-cycle"
        D1Δ + D2Δ -> D2'Δ           "compact-cycle"

    Dz won't have deltas if configured in non-delta mode. But all other
    disk snapshot shall have deltas.

    Memory snapshots shall be persisted to the last, and only, disk
    snapshot.

Any of the above configuration can be in delta mode or non-delta mode.
In the former case, older values are preserved a deltas (also called diff).
In the later case, older values are not preserved.

There can be upto 16 levels of disk snapshot, where Dz is the 16th
level and Da is the 1st level.

Access measurement
==================

Access measurement is used by evict algorithm. It is made up
of two parameters:

* "most-recently-used" is a 32 bit value that measures the time
  elapsed, in seconds, from the time the index was active in memory.

* "most-frequently-used" is a 32 bit value measured in milli-seconds
  as a moving average between two access.

Evict algorithm
===============

Applicable only in "backup" configuration, where for every node evicted,
latest disk snapshot will be touched to learn the file-position for the
entry. Once the node is evicted, a reference to on-disk entry as
unary tuple - {file, fpos} shall be maintained. This logic implies that
older snapshot files cannot be deleted, until all such references are
pruged. There will be background scanner that shall update the older
references to latest snapshot. After which, older snapshot can be
compacted away.

"Evict walk", shall be a write operation walking from root node to
leaf node of memory index, deleting all deltas on its path. Optionally
values shall be deleted on the same path, if memory pressure is
high. The former is called "delta-evict" and the later is called
"value-evict".

If memory pressure is > 98%
    For every write operation _two_ evict operation shall be inserted.
    Includes both "delta-evict" and "value-evict".
If memory pressure is > 95%
    For every write operation _one_ evict operation shall be inserted.
    Includes both "delta-evict" and "value-evict".
If memory pressure is > 90%
    Between two write operation _one_ evit operation shall be inserted.
    Includes only "delta-evict".

If node was last accessed before 24 hours before, then node shall
be subjected to "delta-evict"/"value-evict".

If node was last accessed before 1 hour, then "most-frequently-used"
moving average between access-time shall be used to evict.

If node as accessed within past 1 hour, then it shall be left as is.

In "backup" configuration, and "non-delta" mode, evict logic shall
pruge all deltas older than the disk-seqno. Remember, there is only
one disk snapshot (D).

Cache management
================

Caching is applicable only for "working-set-dgm" configuration. Where, read
operations from disk is expected to be in memory. Here disk reads are
indexed in a separate memory index called "Mc" index. And it shall be
merged with "Mf" index to persist the latest batch of writes.

Mc index will contain only the latest value for any entry. To fetch the
deltas, we have to hit the disk.

The side-effect of this design is that, there will be duplicate entries
on disk for the same seqno. So, read/iteration/merge operations on disk
index should take care of this.

Misc.
=====

* Delta variants, memory index, for different configurations:

Native : "memory", "backup", "random-dgm", "working-set-dgm"
Backup : "backup"

* Delta variants, disk index:

Native    : For APIs asking for previous versions of a value.
Reference : Constructed during lambda-merge, when re-using vlog file.

* Value variants, memory index, for different configurations:

Native    : "memory", "backup", "random-dgm", "working-set-dgm"
Backup    : "backup"

* Value variants, disk index:

Native    : "memory", "backup", "random-dgm", "working-set-dgm"
Reference : "working-set-dgm", while constructing lambda-merge
            between cached entry in the latest disksnapshot.
