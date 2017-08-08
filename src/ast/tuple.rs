use std::ops::Index;
use ::ast::value_type::ValueType;

#[derive(Debug)]
pub struct TupleEntry {
    pub name: String,
    pub value: ValueType,
}

impl TupleEntry {
    pub fn to_ddl(&self) -> String {
        format!("{} {}", self.name, self.value.to_ddl())
    }
}

#[derive(Debug)]
pub struct TupleDef(pub Vec<TupleEntry>);

impl TupleDef {
    pub fn size_of(&self) -> u64 {
        match self {
            &TupleDef(ref v) => {
                let mut size = 0;
                for entry in v {
                    size += entry.value.size_of();
                }
                size
            }
        }
    }

    pub fn to_ddl(&self) -> String {
        match self {
            &TupleDef(ref v) => {
                let s: Vec<String> = v.into_iter().map(|e: &TupleEntry| e.to_ddl()).collect();
                format!("({})", s.join(", "))
            }
        }
    }
}

impl Index<usize> for TupleDef {
    type Output = TupleEntry;
    fn index(&self, index: usize) -> &TupleEntry {
        match self {
            &TupleDef(ref v) => {
                &v[index]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_size() {
        use ::ast::value_type::ValueType::{Bool, Uint, Int, Vector};

        assert_eq!(25, TupleDef(vec![
            TupleEntry {name: "col_1".to_string(), value: Bool},
            TupleEntry {name: "col_2".to_string(), value: Uint},
            TupleEntry {name: "col_3".to_string(), value: Vector(2, Box::new(Int))},
        ]).size_of());
    }
}
