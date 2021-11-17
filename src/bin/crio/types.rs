use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Crate {
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    description: String,
    documentation: String, // url
    downloads: usize,
    homepage: String, // url
    id: String,
    max_upload_size: Option<usize>,
    name: String,
    readme: String,
    repository: String, // url
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    updated_at: String,
}
