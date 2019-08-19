lazy_static! {
    pub static ref BATCH_MARKER: Vec<u8> = {
        let marker = "செய்வன திருந்தச் செய்";
        marker.as_bytes().to_vec()
    };
}
