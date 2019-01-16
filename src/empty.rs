use crate::traits::Serialize;

#[derive(Clone, Default)] // TODO: implement serialize
pub struct Empty;

impl Serialize for Empty {
    fn encode(&self, buffer: &mut Vec<u8>) {
        return;
    }
    fn decode(&mut self, buffer: Vec<u8>) {
        return;
    }
}
