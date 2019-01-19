use crate::traits::Serialize;

#[derive(Clone, Default)] // TODO: implement serialize
pub struct Empty;

impl Serialize for Empty {
    fn encode(&self, _: &mut Vec<u8>) {
        return;
    }
    fn decode(&mut self, _: Vec<u8>) {
        return;
    }
}
