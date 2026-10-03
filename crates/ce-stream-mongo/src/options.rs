//! Capture options that are not in [`ce_stream_core::SourceConfig`].

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FullDocumentMode {
    /// `fullDocument: "required"`. The collection must have `changeStreamPreAndPostImages` enabled.
    #[default]
    Required,
    /// `fullDocument: "updateLookup"`. The post-image is a later read and can miss.
    UpdateLookup,
}
