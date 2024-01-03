use crate::types::{Type, STRING_SIZE};

// Wrapper for different types of fields
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FieldVal {
    IntField(IntField),
    StringField(StringField),
}

impl FieldVal {
    // Extracts the inner IntField
    pub fn into_int(self) -> Option<IntField> {
        match self {
            FieldVal::IntField(int_field) => Some(int_field),
            _ => None,
        }
    }
    // Extracts the inner StringField
    pub fn into_string(self) -> Option<StringField> {
        match self {
            FieldVal::StringField(string_field) => Some(string_field),
            _ => None,
        }
    }
}

// Trait for different types of fields
pub trait Field {
    // Get the type of the field
    fn get_type(&self) -> Type;
    // Serialize the field into bytes
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct IntField {
    value: i32,
}

impl IntField {
    pub fn new(value: i32) -> Self {
        IntField { value }
    }
    pub fn get_value(&self) -> i32 {
        self.value
    }
}

impl Field for IntField {
    fn get_type(&self) -> Type {
        Type::IntType
    }
    fn serialize(&self) -> Vec<u8> {
        self.value.to_be_bytes().to_vec()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StringField {
    value: String,
    len: u32,
}

impl StringField {
    pub fn new(value: String, len: u32) -> Self {
        StringField { value, len }
    }

    // - adam
    pub fn get_value(&self) -> String {
        self.value.clone()
    }
}

impl Field for StringField {
    fn get_type(&self) -> Type {
        Type::StringType
    }

    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![0; STRING_SIZE + 4];
        bytes[0..4].copy_from_slice(&self.len.to_be_bytes());
        // copy as many bytes as possible from string and pad with 0s
        let str_bytes = self.value.as_bytes();
        let copy_len = std::cmp::min(str_bytes.len(), STRING_SIZE);
        bytes[4..4 + copy_len].copy_from_slice(&str_bytes[..copy_len]);
        bytes
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_int_field() {
        let int_field = IntField::new(1);
        assert_eq!(int_field.get_type(), Type::IntType);
        assert_eq!(int_field.serialize(), vec![0, 0, 0, 1]);
    }

    #[test]
    fn test_string_field() {
        let string_field = StringField::new("hello".to_string(), 5);
        assert_eq!(string_field.get_type(), Type::StringType);
        let mut serialized = [0; STRING_SIZE + 4];
        serialized[3] = 5;
        serialized[4..9].copy_from_slice("hello".as_bytes());

        assert_eq!(string_field.serialize(), serialized);
    }
}
