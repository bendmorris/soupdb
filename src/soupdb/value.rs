use std::fmt::{Debug, Formatter, Result};
use byteorder::{ByteOrder, LittleEndian};

const MAX_INLINE_STRING_LENGTH: u64 = 256;

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

#[derive(Debug, PartialEq, Clone)]
pub enum ValueType {
    // unknown type in expressions
    Unknown,
    // auto-increment uint ID
    AutoId,
    Bool,
    Uint,
    Int,
    Float,
    // sized string type: a size of zero indicates variable size, which will
    // be stored off page
    Str(u64),
    Nullable(Box<ValueType>),
    Vector(u64, Box<ValueType>),
}

impl ValueType {
    pub fn size_of(&self) -> u64 {
        match self {
            &ValueType::Unknown => panic!("invalid schema with unknown field type"),
            &ValueType::Bool => 1,
            &ValueType::Uint => 8,
            &ValueType::Int => 8,
            &ValueType::Float => 8,
            &ValueType::AutoId => 8,
            &ValueType::Str(0) => 16,
            &ValueType::Str(n) => if n > MAX_INLINE_STRING_LENGTH {MAX_INLINE_STRING_LENGTH} else {n},
            &ValueType::Nullable(ref v) => 1 + (*v).size_of(),
            &ValueType::Vector(n, ref v) => n * (*v).size_of(),
        }
    }

    pub fn to_ddl(&self) -> String {
        match self {
            &ValueType::Unknown => panic!("invalid schema with unknown field type"),
            &ValueType::AutoId => "autoid".to_string(),
            &ValueType::Bool => "bool".to_string(),
            &ValueType::Uint => "unsigned int".to_string(),
            &ValueType::Int => "int".to_string(),
            &ValueType::Float => "float".to_string(),
            &ValueType::Str(n) => if n > 0 {format!("str({})", n)} else {"str".to_string()},
            &ValueType::Nullable(ref v) => format!("nullable {}", (*v).to_ddl()),
            &ValueType::Vector(n, ref v) => format!("vector({}) {}", n, (*v).to_ddl()),
        }
    }
}

#[test]
fn test_value_size() {
    use soupdb::value::ValueType::{AutoId, Bool, Uint, Int, Str, Nullable, Vector};

    assert_eq!(8, AutoId.size_of());
    assert_eq!(1, Bool.size_of());
    assert_eq!(8, Uint.size_of());
    assert_eq!(8, Int.size_of());
    assert_eq!(16, Str(0).size_of());
    assert_eq!(27, Str(27).size_of());
    assert_eq!(28, Nullable(Box::new(Str(27))).size_of());
    assert_eq!(112, Vector(4, Box::new(Nullable(Box::new(Str(27))))).size_of());
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

#[test]
fn test_ddl() {
    assert_eq!("bool", ValueType::Bool.to_ddl());
    assert_eq!("int", ValueType::Int.to_ddl());
    assert_eq!("unsigned int", ValueType::Uint.to_ddl());
    assert_eq!("float", ValueType::Float.to_ddl());
    assert_eq!("str", ValueType::Str(0).to_ddl());
    assert_eq!("str(12)", ValueType::Str(12).to_ddl());
    assert_eq!("nullable bool", (ValueType::Nullable(Box::new(ValueType::Bool))).to_ddl());
    assert_eq!("nullable int", (ValueType::Nullable(Box::new(ValueType::Int))).to_ddl());
    assert_eq!("nullable str(189)", (ValueType::Nullable(Box::new(ValueType::Str(189)))).to_ddl());
    assert_eq!("vector(3) nullable bool", (ValueType::Vector(3, Box::new(ValueType::Nullable(Box::new(ValueType::Bool))))).to_ddl());
}
