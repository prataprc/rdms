#! /usr/bin/env bash

exec > $1
exec 2>&1

set -o xtrace

PROF_DIR = src/bin/rdms/perf-profiles

# regular benchmark
# date; time cargo +nightly bench -- --nocapture || exit $?
# TODO: date; time cargo +stable bench -- --nocapture || exit $?

# invoke perf binary
date; time cargo +nightly run --release --bin rdms --features=rdms -- --profile $PROF_DIR/default-llrb.toml llrb
# invoke perf binary, with valgrid
date; valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes cargo +nightly run --release --bin rdms --features=rdms -- --profile $PROF_DIR/default-llrb.toml llrb
