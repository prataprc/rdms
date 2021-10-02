use lazy_static::lazy_static;

lazy_static! {
    pub static ref ROOT_MARKER: Vec<u8> = {
        let marker = "அறம் செய விரும்பு";
        marker.as_bytes().to_vec()
    };
}
