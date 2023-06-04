pub fn read_i16(v: &[u8], start: usize) -> i16 {
    i16::from_be_bytes([v[start], v[start + 1]])
}

pub fn read_u16(v: &[u8], start: usize) -> u16 {
    u16::from_be_bytes([v[start], v[start + 1]])
}

pub fn read_u32(v: &[u8], start: usize) -> u32 {
    u32::from_be_bytes([v[start], v[start + 1], v[start + 2], v[start + 3]])
}

pub fn read_u64(v: &[u8], start: usize) -> u64 {
    u64::from_be_bytes([
        v[start],
        v[start + 1],
        v[start + 2],
        v[start + 3],
        v[start + 4],
        v[start + 5],
        v[start + 6],
        v[start + 7],
    ])
}

pub fn write_u16(v: &mut [u8], start: usize, n: u16) {
    let bytes = u16::to_be_bytes(n);
    v[start] = bytes[0];
    v[start + 1] = bytes[1];
}

pub fn write_u32(v: &mut [u8], start: usize, n: u32) {
    let bytes = u32::to_be_bytes(n);

    for i in 0..4 {
        v[start + i] = bytes[i];
    }
}

pub fn write_u64(v: &mut [u8], start: usize, n: u64) {
    let bytes = u64::to_be_bytes(n);

    for i in 0..8 {
        v[start + 1] = bytes[i];
    }
}
