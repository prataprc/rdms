use std::{ffi, mem, sync::Arc};

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
            inner: mem::MaybeUninit::new(Arc::clone(unsafe { mu.inner.get_ref() })),
        }
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(unsafe { self.inner.get_ref() })
    }

    pub fn get_ptr(&self) -> *mut ffi::c_void {
        let arc_ref = unsafe { self.inner.get_ref() };
        let ptr: &ffi::c_void = arc_ref.as_ref().as_ref();
        ptr as *const ffi::c_void as *mut ffi::c_void
    }
}
