pub(crate) fn from_i32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

pub(crate) fn from_i64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}
