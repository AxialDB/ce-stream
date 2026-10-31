//! The `ce-stream` binary is in `main.rs`. This checks that the embedding guide
//! compiles: this crate is the one that has every source as a dependency.

#[cfg(doctest)]
#[doc = include_str!("../../../docs/library.md")]
pub struct LibraryGuide;
