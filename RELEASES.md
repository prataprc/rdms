0.1.0
=====

Code Review checklist
=====================

* [ ] Check and confirm dead-code.
* [ ] Check and confirm ignored test cases.
* [ ] Check for un-necessary trait constraints like Debug and Display.
* [ ] Review and check for un-necessary copy, and allocations.
* [ ] Review resize calls on `Vec`.
* [ ] Review (as ...) type casting, to panic on data loss.
* [ ] Reduce trait constraints for Type parameters on public APIs.
* [ ] Public APIs can be as generic as possible. Check whether there
      is a scope for `AsRef` or `Borrow` constraints.
* [ ] Review/Document error variants, capture useful information in error msg.
* [ ] Check for dangling links in rustdoc.
* [ ] 80-column width.
* [ ] Copyright and License notice.
* [ ] Make sure that generated artifact is debuggable. Like,
  * [ ] RUSTLFAGS=-g
* [ ] Verify panic!() macro, try to replace them with Err(Error).
* [ ] Verify unreachable!() macro, try to replace them with Err(Error).
* [ ] Avoid println!() macro in production code.
* [ ] Document rdms::error::Error type and all its variants.
* [ ] Review TODO comments in code.
* [ ] Check for unwrap() calls. It can be security breach if unwrap
  is called on values from external sources, outside the `rdms` library.
* [ ] check fo ok() calls on Result type. It can be security breach if
  errors are ignored.
* [ ] Trim trait constraits for exported types, exported functions and
  type/methods/functions defined in core.rs


* Avoid panic!() in inner-level functions. If at all it is used, use
them at the API level.

Release Checklist
=================

* Bump up the version:
  * __major__: backward incompatible API changes.
  * __minor__: backward compatible API Changes.
  * __patch__: bug fixes.
* Travis-CI integration.
* Cargo checklist
  * cargo +stable build; cargo +nightly build
  * cargo +stable doc
  * cargo +nightly clippy --all-targets --all-features
  * cargo +nightly test
  * cargo +nightly bench
  * cargo +nightly benchcmp <old> <new>
  * cargo fix --edition --all-targets
* Cargo test `ixtest` and `ixperf`
* Create a git-tag for the new version.
* Cargo publish the new version.
* Badges
  * Build passing, Travis continuous integration.
  * Code coverage, codecov and coveralls.
  * Crates badge
  * Downloads badge
  * License badge
  * Rust version badge.
  * Maintenance-related badges based on isitmaintained.com
  * Documentation
  * Gitpitch
* Targets
  * RHEL
  * SUSE
  * Debian
  * Centos
  * Ubuntu
  * Mac-OS
  * Windows
  * amazon-aws
  * Raspberry-pi
