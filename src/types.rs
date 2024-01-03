use crate::fields::{FieldVal, IntField, StringField};

pub const STRING_SIZE: usize = 256;

// Only support Int and String types
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Type {
    IntType,
    StringType,
}

impl Type {
    // Get the size of the type in bytes
    pub fn get_len(&self) -> usize {
        match self {
            // 4 bytes ints
            Type::IntType => 4,
            // 4 bytes for length + STRING_SIZE bytes for string
            Type::StringType => STRING_SIZE + 4,
        }
    }

    // Parse bytes into a FieldVal
    pub fn parse(&self, bytes: &[u8]) -> Result<FieldVal, String> {
        match self {
            Type::IntType => {
                let mut int_bytes = [0; 4];
                int_bytes.copy_from_slice(&bytes[..4]);
                Ok(FieldVal::IntField(IntField::new(i32::from_be_bytes(
                    int_bytes,
                ))))
            }
            Type::StringType => {
                let mut len_bytes = [0; 4];
                len_bytes.copy_from_slice(&bytes[..4]);
                let len = u32::from_be_bytes(len_bytes);
                let string_bytes = bytes[4..len as usize + 4].to_vec();
                Ok(FieldVal::StringField(StringField::new(
                    String::from_utf8(string_bytes.to_vec()).unwrap(),
                    len,
                )))
            }
        }
    }
}
