use std::{
    ffi, mem,
    ops::DerefMut,
    sync::{self, Arc},
    thread, time,
};

type Ccmut = (bool, u32, Box<ffi::c_void>);

pub(crate) struct CCMu {
    inner: mem::MaybeUninit<Arc<sync::Mutex<Ccmut>>>,
}

impl CCMu {
    pub(crate) fn clone(mu: &CCMu) -> CCMu {
        let arc_ref = unsafe { mu.inner.get_ref() };
        CCMu {
            inner: mem::MaybeUninit::new(Arc::clone(arc_ref)),
        }
    }

    pub(crate) fn uninit() -> CCMu {
        CCMu {
            inner: mem::MaybeUninit::uninit(),
        }
    }

    pub(crate) fn init_with_ptr(ptr: Box<ffi::c_void>) -> CCMu {
        let val = Arc::new(sync::Mutex::new((false, 0, ptr)));
        CCMu {
            inner: mem::MaybeUninit::new(val),
        }
    }

    pub(crate) fn start_op(&self) -> (bool, *mut ffi::c_void) {
        let mu = unsafe { self.inner.get_ref() };
        match mu.lock().unwrap().deref_mut() {
            (true, _, ptr) => (false, ptr.as_mut() as *mut ffi::c_void),
            (false, n, ptr) => {
                *n = *n + 1;
                (true, ptr.as_mut() as *mut ffi::c_void)
            }
        }
    }

    pub(crate) fn fin_op(&self) {
        let mu = unsafe { self.inner.get_ref() };
        match mu.lock().unwrap().deref_mut() {
            (false, n, _) if *n > 0 => {
                *n = *n - 1;
            }
            _ => unreachable!(),
        }
    }
}

impl AsRef<sync::Mutex<Ccmut>> for CCMu {
    fn as_ref(&self) -> &sync::Mutex<Ccmut> {
        unsafe { self.inner.get_ref().as_ref() }
    }
}

impl Drop for CCMu {
    fn drop(&mut self) {
        loop {
            let mu = unsafe { self.inner.get_ref() };
            match mu.lock().unwrap().deref_mut() {
                (dropped, n, _) if *n == 0 => {
                    *dropped = true;
                    break;
                }
                (_, _, _) => thread::sleep(time::Duration::from_secs(1)),
            }
        }
    }
}
