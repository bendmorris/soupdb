use std::fmt::{Debug, Formatter, Result};
use byteorder::{ByteOrder, LittleEndian};

pub const MAX_INLINE_STRING_LENGTH: u64 = 256;

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
    /// Size required for a ValueType, in bytes.
    pub fn size_of(&self) -> u64 {
        match self {
            &ValueType::Unknown => panic!("invalid schema with unknown field type"),
            &ValueType::Bool => 1,
            // 64-bit numeric types
            &ValueType::Uint => 8,
            &ValueType::Int => 8,
            &ValueType::Float => 8,
            &ValueType::AutoId => 8,
            // off-page storage is a page ID (u64) + offset (u16)
            &ValueType::Str(0) => 10,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_size() {
        use self::ValueType::{AutoId, Bool, Uint, Int, Str, Nullable, Vector};

        assert_eq!(8, AutoId.size_of());
        assert_eq!(1, Bool.size_of());
        assert_eq!(8, Uint.size_of());
        assert_eq!(8, Int.size_of());
        assert_eq!(10, Str(0).size_of());
        assert_eq!(27, Str(27).size_of());
        assert_eq!(28, Nullable(Box::new(Str(27))).size_of());
        assert_eq!(112, Vector(4, Box::new(Nullable(Box::new(Str(27))))).size_of());
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
}
