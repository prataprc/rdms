use std::sync::Arc;

use crate::sync_writer::SyncWriter;

#[test]
fn test_sync_writer_single() {
    let mu = SyncWriter::new();
    {
        let _lock = mu.lock();
    }
    let _lock = mu.lock();
}

#[test]
#[should_panic]
fn test_sync_writer_concur() {
    let mu1 = Arc::new(SyncWriter::new());
    let mu2 = Arc::clone(&mu1);
    let _lock1 = mu1.lock();
    let _lock2 = mu2.lock();
}
