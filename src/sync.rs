use std::{ffi, mem, sync::Arc};

pub(crate) struct CCMu {
    inner: mem::MaybeUninit<Arc<mem::ManuallyDrop<Box<ffi::c_void>>>>,
}

impl CCMu {
    pub(crate) fn uninit() -> CCMu {
        CCMu {
            inner: mem::MaybeUninit::uninit(),
        }
    }

    pub(crate) fn init_with_ptr(value: Box<ffi::c_void>) -> CCMu {
        CCMu {
            inner: mem::MaybeUninit::new(Arc::new(mem::ManuallyDrop::new(value))),
        }
    }

    pub(crate) fn clone(mu: &CCMu) -> CCMu {
        let arc_ref = unsafe { mu.inner.get_ref() };
        CCMu {
            inner: mem::MaybeUninit::new(Arc::clone(arc_ref)),
        }
    }

    pub(crate) fn strong_count(&self) -> usize {
        Arc::strong_count(unsafe { self.inner.get_ref() })
    }

    pub(crate) fn get_ptr(&self) -> *mut ffi::c_void {
        let arc_ref = unsafe { self.inner.get_ref() };
        let ptr: &ffi::c_void = arc_ref.as_ref().as_ref();
        ptr as *const ffi::c_void as *mut ffi::c_void
    }
}
