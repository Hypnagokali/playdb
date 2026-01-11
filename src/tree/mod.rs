// Copied from from my algos_test repo
use std::u32;

mod store;
pub mod node;

pub fn read_u32_with_null(raw_value: u32) -> Option<u32> {
    if raw_value == u32::MAX {
        None
    } else {
        Some(raw_value)
    }
}

pub fn get_u32_be_bytes_from_option(value: &Option<u32>) -> Vec<u8> {
    value.unwrap_or(u32::MAX).to_be_bytes().to_vec()    
}