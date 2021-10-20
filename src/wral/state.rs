use cbordata::{Cborize, FromCbor, IntoCbor};

#[allow(unused_imports)]
use crate::wral::Wal;
use crate::{wral, Result};

/// Callback trait for updating application state in relation to [Wal] type.
pub trait State: 'static + Clone + Sync + Send + IntoCbor + FromCbor {
    fn on_add_entry(&mut self, new_entry: &wral::Entry) -> Result<()>;
}

/// Default parameter, implementing [State] trait, for [Wal] type.
#[derive(Clone, Eq, PartialEq, Debug, Cborize)]
pub struct NoState;

impl NoState {
    const ID: u32 = 0x0;
}

impl State for NoState {
    fn on_add_entry(&mut self, _: &wral::Entry) -> Result<()> {
        Ok(())
    }
}
