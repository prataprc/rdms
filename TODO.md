* cargo stuff
  * spell checker rust documentation and other md files.
  * md-book
  * release management
    * configuration via features
    * platform binary generation
    * publishing onto crates.io
  * package dependencies
    * declared but unused dependencies
    * outdated dependencies, upgrades
  * licensing analysis
  * source code analysis
    * modulewise, imports.
    * list of types (type, struct, enum, const, static)
    * list of functions, traits, trait implementation
    * type methods, public and private.
    * featured gated source code items.

* README: create document links in a separate section, linking to docs/ directory.
* implement logging using log facade.
* benches: Fix benches/ and add it as part of perf.sh
* rdms-perf: add performance suite for dgm, robt, wral, shllrb, shrobt, croaring
* rdms-perf: add latency measurements.
* rdms-perf: plot graphs.
* rdms-test: migrate test-suites from ixtest

* rdms-perf for robt
  * try initial build with 1M, 10, 100M entries; with value as Binary(1K)
    * try with nobitmap, xor8 bitmaps.
  * try incremental build with 1M, 10, 100M entries; with value as Binary(1K)
    * try the incremental builds with and without compaction
  * measure concurrent read performance for 1, 2, 4, 8, 16 threads
    * try with and without lru cache.

* wral: journal-limit, adjust the algorithm to not to exceed the journal limit.

(a) review 5c71164f6d9e57ce60ed0030f1fa7dba7d5056b5
        fix errors before refactoring llrb out into ppom

rdms-clru:

Concurrent access to least-recently-used-cache need its backing datastructure
like a disk-btree to be immutable. Otherwise, we may have to deal with
synchronization problem in building the cache and evicting the entries.

Access-1:
    Get(cache) fail
        Get(disk-btree)
        Set(cache)

Access-2:
    Set(disk-btree)
    Set(cache)

Access-3:
    Remove(disk-btree)
    Remove(cache)

Access-4:
    Evict(cache)

There will synchronization issues when above listed access scenarios happen
concurrently on the disk-btree and the cache.
