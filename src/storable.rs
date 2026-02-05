use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

pub trait Storable: Serialize + DeserializeOwned + Debug + PartialEq {
    const LENGTH: u32 = 4;
    fn content_length(&self) -> u32;
    fn total_length(&self) -> u32;
}
