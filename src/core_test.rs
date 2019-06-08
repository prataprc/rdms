use crate::core::{Delta, Diff, Entry};
use crate::vlog;

#[test]
fn test_delta_new1() {
    let dlt = vlog::Delta::Native { delta: 10.diff(20) };
    let delta = Delta::new(dlt, 100, None);
    match delta.vlog_delta_vref() {
        vlog::Delta::Native{ dlt } => assert_eq!(dlt, 20),
        _ => unreachable!(),
    }
    assert_eq!(delta.into_diff(), 20);
    assert_eq!(delta.seqno(), 100);
    assert_eq!(delta.born_seqno(), 100);
    assert_eq!(delta.dead_seqno(), None);
    assert_eq!(delta.is_deleted(), false);

    let delta = Delta::new(dlt, 100, Some(200));
    assert_eq!(delta.seqno(), 200);
    assert_eq!(delta.dead_seqno(), 200);
    assert_eq!(delta.is_deleted(), true);
}

#[test]
fn test_delta_new2() {
    let delta = Delta::new_delta(10.diff(20) }, 100, None);
    match delta.vlog_delta_vref() {
        vlog::Delta::Native{ dlt } => assert_eq!(dlt, 20),
        _ => unreachable!(),
    }
    assert_eq!(delta.into_diff(), 20);
    assert_eq!(delta.seqno(), 100);
    assert_eq!(delta.born_seqno(), 100);
    assert_eq!(delta.dead_seqno(), None);
    assert_eq!(delta.is_deleted(), false);

    let delta = Delta::new_delta(10.diff(20) }, 100, Some(200));
    assert_eq!(delta.seqno(), 200);
    assert_eq!(delta.dead_seqno(), 200);
    assert_eq!(delta.is_deleted(), true);

    let delta = Delta::new_delta(10.diff(20) }, 20, None);
}
