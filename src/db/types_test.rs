use super::*;

macro_rules! test_diff_basic_types {
    ($(($type:ident, $name:ident)),*) => (
        $(
            #[test]
            fn $name() {
                let ver1: $type = rand::random();
                let ver2: $type = rand::random();

                assert_eq!(ver2.diff(&ver1), ver1);
                assert_eq!(ver2.merge(&ver1), ver1);
            }
        )*
    );
}

test_diff_basic_types![
    (bool, test_diff_bool),
    (i8, test_diff_i8),
    (i16, test_diff_i16),
    (i32, test_diff_i32),
    (i64, test_diff_i64),
    (i128, test_diff_i128),
    (isize, test_diff_isize),
    (u8, test_diff_u8),
    (u16, test_diff_u16),
    (u32, test_diff_u32),
    (u64, test_diff_u64),
    (u128, test_diff_u128),
    (usize, test_diff_usize)
];

macro_rules! test_footprint_basic_types {
    ($(($type:ty, $name:ident, $size:expr)),*) => (
        $(
            #[test]
            fn $name() {
                let val: $type = Default::default();
                assert_eq!(val.footprint().unwrap(), $size, stringify!($name));
            }
        )*
    );
}

test_footprint_basic_types![
    (bool, test_footprint_bool, 1),
    (i8, test_footprint_i8, 1),
    (i16, test_footprint_i16, 2),
    (i32, test_footprint_i32, 4),
    (i64, test_footprint_i64, 8),
    (i128, test_footprint_i128, 16),
    (isize, test_footprint_isize, 8),
    (u8, test_footprint_u8, 1),
    (u16, test_footprint_u16, 2),
    (u32, test_footprint_u32, 4),
    (u64, test_footprint_u64, 8),
    (u128, test_footprint_u128, 16),
    (usize, test_footprint_usize, 8),
    (f32, test_footprint_f32, 4),
    (f64, test_footprint_f64, 8),
    (char, test_footprint_char, 4)
];
