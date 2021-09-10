//! Module `sync` implements synchronisation primitive to implement
//! complex index types.

// TODO: deprecate this module, after implementing Dgm and Backup

use std::{ffi, mem, sync::Arc};

pub struct SyncAccess<T> {
    value: T,
}

impl<T> SyncAccess<T> {
    pub fn new(value: T) -> SyncAccess<T> {
        SyncAccess { value }
    }
}

impl<U, T> AsRef<U> for SyncAccess<T>
where
    T: AsRef<U>,
{
    fn as_ref(&self) -> &U {
        self.value.as_ref()
    }
}

impl<U, T> AsMut<U> for SyncAccess<T>
where
    T: AsMut<U>,
{
    fn as_mut(&mut self) -> &mut U {
        self.value.as_mut()
    }
}

pub struct CCMu {
    inner: mem::MaybeUninit<Arc<mem::ManuallyDrop<Box<ffi::c_void>>>>,
}

impl CCMu {
    pub fn uninit() -> CCMu {
        CCMu {
            inner: mem::MaybeUninit::uninit(),
        }
    }

    pub fn init_with_ptr(value: Box<ffi::c_void>) -> CCMu {
        CCMu {
            inner: mem::MaybeUninit::new(Arc::new(mem::ManuallyDrop::new(value))),
        }
    }

    pub fn clone(mu: &CCMu) -> CCMu {
        CCMu {
            inner: mem::MaybeUninit::new(Arc::clone(unsafe {
                mu.inner.as_ptr().as_ref().unwrap()
            })),
        }
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(unsafe { self.inner.as_ptr().as_ref().unwrap() })
    }

    pub fn as_mut_ptr(&self) -> *mut ffi::c_void {
        let arc_ref = unsafe { self.inner.as_ptr().as_ref().unwrap() };
        let ptr: &ffi::c_void = arc_ref.as_ref().as_ref();
        ptr as *const ffi::c_void as *mut ffi::c_void
    }
}
