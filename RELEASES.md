0.1.0
=====

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
