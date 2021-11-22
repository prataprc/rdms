use std::sync::{
    atomic::{AtomicPtr, Ordering::SeqCst},
    Arc,
};

mod access;
mod evictor;
// mod lru;

use access::Access;
use evictor::Evictor;
// pub use lru::Lru;

// wrap the value parameter.
pub struct Value<K, V> {
    value: Arc<V>,
    access: AtomicPtr<Access<K>>,
}

impl<K, V> Clone for Value<K, V>
where
    V: Clone,
{
    fn clone(&self) -> Self {
        Value {
            value: Arc::clone(&self.value),
            access: AtomicPtr::new(self.access.load(SeqCst)),
        }
    }
}

impl<K, V> Drop for Value<K, V> {
    fn drop(&mut self) {
        // mark this as deleted so that evictor will remove this at some point.
        let access = self.access.load(SeqCst);
        unsafe { access.as_ref().unwrap() }.delete();
    }
}
