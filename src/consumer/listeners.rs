use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UpdateOffset {
    pub partition: u16,
    pub offset: usize,
}
