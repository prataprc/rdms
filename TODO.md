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
