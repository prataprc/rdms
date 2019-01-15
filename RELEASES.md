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
  * rust-doc
  * gitpitch
  * build-passing
