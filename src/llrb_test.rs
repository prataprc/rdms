use crate::llrb::Llrb;
use crate::empty::Empty;

#[test]
fn test_id() {
    let llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert_eq!(llrb.id(), "test-llrb".to_string());
}

#[test]
fn test_seqno() {
    let mut llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert_eq!(llrb.get_seqno(), 0);
    llrb.set_seqno(1234);
    assert_eq!(llrb.get_seqno(), 1234);
}

#[test]
fn test_count() {
    let llrb: Llrb<i32,Empty> = Llrb::new("test-llrb", false);
    assert_eq!(llrb.count(), 0);
}

#[test]
fn test_crud() {
}
