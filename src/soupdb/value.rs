use std::fmt::{Debug, Formatter, Result};
use byteorder::{ByteOrder, LittleEndian};
use soupdb::ast::value_type::ValueType;

pub union Value {
    bool_value: bool,
    uint_value: u64,
    int_value: i64,
    float_value: f64,
}

impl Value {
    pub fn from_bytes(bytes: &[u8], value_type: &ValueType) -> Option<Value> {
        match value_type {
            &ValueType::Bool => Some(Value {uint_value: if bytes[0] != 0 {1} else {0}}),
            &ValueType::Uint => Some(Value {uint_value: LittleEndian::read_u64(&bytes)}),
            &ValueType::Int => Some(Value {int_value: LittleEndian::read_i64(&bytes)}),
            &ValueType::Float => Some(Value {float_value: LittleEndian::read_f64(&bytes)}),
            _ => None
        }
    }

    pub fn to_bytes(&self, mut bytes: &mut [u8], value_type: &ValueType) {
        unsafe {
            match value_type {
                &ValueType::Bool => bytes[0] = if self.bool_value {1} else {0},
                &ValueType::Uint => LittleEndian::write_u64(&mut bytes, self.uint_value),
                &ValueType::Int => LittleEndian::write_i64(&mut bytes, self.int_value),
                &ValueType::Float => LittleEndian::write_f64(&mut bytes, self.float_value),
                _ => ()
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, rhs: &Self) -> bool {
        unsafe {
            self.uint_value == rhs.uint_value
        }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter) -> Result {
        unsafe {
            write!(f, "0x{:x}", self.uint_value)
        }
    }
}

#[test]
fn test_value_from_bytes() {
    assert_eq!(Some(Value {bool_value: true}), Value::from_bytes(&[1], &ValueType::Bool));
    assert_eq!(Some(Value {bool_value: false}), Value::from_bytes(&[0], &ValueType::Bool));
    assert_eq!(Some(Value {int_value: 0}), Value::from_bytes(&[0, 0, 0, 0, 0, 0, 0, 0], &ValueType::Int));
    assert_eq!(Some(Value {int_value: 1}), Value::from_bytes(&[1, 0, 0, 0, 0, 0, 0, 0], &ValueType::Int));
    assert_eq!(Some(Value {int_value: -1}), Value::from_bytes(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], &ValueType::Int));
    assert_eq!(Some(Value {uint_value: 0}), Value::from_bytes(&[0, 0, 0, 0, 0, 0, 0, 0], &ValueType::Uint));
    assert_eq!(Some(Value {uint_value: 1}), Value::from_bytes(&[1, 0, 0, 0, 0, 0, 0, 0], &ValueType::Uint));
    assert_eq!(Some(Value {uint_value: 18446744073709551615}), Value::from_bytes(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], &ValueType::Uint));
    assert_eq!(Some(Value {float_value: 0.12345}), Value::from_bytes(&[0x7c, 0xf2, 0xb0, 0x50, 0x6b, 0x9a, 0xbf, 0x3f], &ValueType::Float));
}
