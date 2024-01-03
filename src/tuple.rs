use std::fmt::{Display, Formatter};

use crate::fields::{Field, FieldVal};
use crate::heap_page::HeapPageId;
use crate::types::Type;

// Reference to a tuple on a page of a table
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct RecordId {
    // Define RecordId properties
    pid: HeapPageId,
    tuple_no: usize,
}

impl RecordId {
    pub fn new(pid: HeapPageId, tuple_no: usize) -> Self {
        RecordId { pid, tuple_no }
    }

    pub fn get_page_id(&self) -> HeapPageId {
        self.pid
    }

    pub fn get_tuple_no(&self) -> usize {
        self.tuple_no
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TupleDesc {
    types: Vec<Type>,
    fields: Vec<String>,
}

impl TupleDesc {
    pub fn new(types: Vec<Type>, fields: Vec<String>) -> Self {
        TupleDesc { types, fields }
    }

    pub fn combine(td1: &TupleDesc, td2: &TupleDesc) -> TupleDesc {
        // Merge two TupleDescs into one, with td1.numFields + td2.numFields
        let mut types = td1.types.clone();
        types.extend(td2.types.clone());
        let mut field_names = td1.fields.clone();
        field_names.extend(td2.fields.clone());
        TupleDesc::new(types, field_names)
    }

    pub fn get_num_fields(&self) -> usize {
        self.types.len()
    }

    // Returns the (possibly null) Field object with the given name.
    pub fn get_field_name(&self, i: usize) -> Option<&String> {
        self.fields.get(i)
    }

    // Returns the (possibly null) index of the field with a given name
    pub fn name_to_id(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|n| n == name)
    }

    // Returns the type of the ith field of this TupleDesc.
    pub fn get_field_type(&self, i: usize) -> Option<&Type> {
        self.types.get(i)
    }

    // Return the size (in bytes) of tuples corresponding to this TupleDesc.
    pub fn get_size(&self) -> usize {
        self.types.iter().fold(0, |acc, t| acc + t.get_len())
    }
}

// Describe the schema of a tuple/table
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Tuple {
    // Define Tuple properties
    fields: Vec<FieldVal>,
    td: TupleDesc,
    rid: RecordId,
}

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        for (i, field) in self.fields.iter().enumerate() {
            match field {
                FieldVal::IntField(int_field) => {
                    s.push_str(&format!("{}: {}", self.td.fields[i], int_field.get_value()))
                }
                FieldVal::StringField(string_field) => s.push_str(&format!(
                    "{}: {}",
                    self.td.fields[i],
                    string_field.get_value()
                )),
            }
            if i != self.fields.len() - 1 {
                s.push_str(", ");
            }
        }
        write!(f, "{{{}}}", s)
    }
}

impl Tuple {
    pub fn new(fields: Vec<FieldVal>, td: &TupleDesc) -> Self {
        Tuple {
            fields,
            td: td.clone(),
            rid: RecordId::new(HeapPageId::new(0, 0), 0),
        }
    }

    pub fn get_tuple_desc(&self) -> &TupleDesc {
        &self.td
    }

    pub fn get_record_id(&self) -> RecordId {
        self.rid
    }

    pub fn set_record_id(&mut self, rid: RecordId) {
        self.rid = rid;
    }

    pub fn get_field(&self, i: usize) -> Option<&FieldVal> {
        self.fields.get(i)
    }

    pub fn set_field(&mut self, i: usize, field: FieldVal) {
        self.fields[i] = field;
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];
        for field in self.fields.iter() {
            match field {
                FieldVal::IntField(int_field) => bytes.extend(int_field.serialize()),
                FieldVal::StringField(string_field) => bytes.extend(string_field.serialize()),
            }
        }
        bytes
    }

    pub fn deserialize(bytes: &[u8], td: &TupleDesc) -> Self {
        let mut offset = 0;
        let mut fields = vec![];
        for t in td.types.iter() {
            let field = t.parse(&bytes[offset..]).unwrap();
            offset += t.get_len();
            fields.push(field);
        }
        Tuple::new(fields, td)
    }

    pub fn get_fields(&self) -> Vec<FieldVal> {
        self.fields.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::{IntField, StringField};
    use crate::types::Type;

    #[test]
    fn test_tuple_desc_combine() {
        let td1 = TupleDesc::new(
            vec![Type::IntType, Type::StringType],
            vec!["int".to_string(), "string".to_string()],
        );
        let td2 = TupleDesc::new(
            vec![Type::IntType, Type::StringType],
            vec!["int".to_string(), "string".to_string()],
        );
        let td3 = TupleDesc::combine(&td1, &td2);
        assert_eq!(td3.get_num_fields(), 4);
        assert_eq!(td3.get_field_name(0), Some(&"int".to_string()));
        assert_eq!(td3.get_field_name(1), Some(&"string".to_string()));
        assert_eq!(td3.get_field_name(2), Some(&"int".to_string()));
        assert_eq!(td3.get_field_name(3), Some(&"string".to_string()));
    }

    #[test]
    fn test_tuple_desc_len() {
        let td = TupleDesc::new(
            vec![Type::IntType, Type::StringType],
            vec!["int".to_string(), "string".to_string()],
        );
        assert_eq!(td.get_size(), 264);
    }

    #[test]
    fn test_tuple_serialize_deserialize() {
        let td = TupleDesc::new(
            vec![Type::IntType, Type::StringType],
            vec!["int".to_string(), "string".to_string()],
        );
        let tuple = Tuple::new(
            vec![
                FieldVal::IntField(IntField::new(1)),
                FieldVal::StringField(StringField::new("hello".to_string(), 5)),
            ],
            &td,
        );
        let bytes = tuple.serialize();
        let tuple2 = Tuple::deserialize(&bytes, &td);
        assert_eq!(tuple, tuple2);
    }
}
