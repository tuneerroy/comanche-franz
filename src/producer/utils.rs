use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub fn hash_value_to_range(v: &String, range: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    let hash_result = hasher.finish();
    (hash_result as usize) % range
}
