use bson::{Bson, Document};
use serde_json::{Map, Number, Value};

pub fn bson_to_json(value: Bson) -> Value {
    match value {
        Bson::Double(v) => Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Bson::String(v) => Value::String(v),
        Bson::Array(values) => Value::Array(values.into_iter().map(bson_to_json).collect()),
        Bson::Document(doc) => document_to_json(doc),
        Bson::Boolean(v) => Value::Bool(v),
        Bson::Null => Value::Null,
        Bson::Int32(v) => Value::Number(Number::from(v)),
        Bson::Int64(v) => Value::Number(Number::from(v)),
        Bson::ObjectId(v) => Value::String(v.to_hex()),
        Bson::DateTime(v) => Value::String(v.to_string()),
        Bson::Binary(v) => Value::String(format!("{:?}", v.bytes)),
        other => Value::String(other.to_string()),
    }
}

pub fn document_to_json(doc: Document) -> Value {
    let mut map = Map::new();
    for (key, value) in doc {
        map.insert(key, bson_to_json(value));
    }
    Value::Object(map)
}

pub fn json_to_bson(value: Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(v) => Bson::Boolean(v),
        Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                Bson::Int64(v)
            } else if let Some(v) = n.as_u64() {
                if v <= i64::MAX as u64 {
                    Bson::Int64(v as i64)
                } else {
                    Bson::Double(v as f64)
                }
            } else {
                Bson::Double(n.as_f64().unwrap_or_default())
            }
        }
        Value::String(v) => Bson::String(v),
        Value::Array(values) => Bson::Array(values.into_iter().map(json_to_bson).collect()),
        Value::Object(map) => {
            let mut doc = Document::new();
            for (key, value) in map {
                doc.insert(key, json_to_bson(value));
            }
            Bson::Document(doc)
        }
    }
}

pub fn json_to_document(value: Value) -> Document {
    match json_to_bson(value) {
        Bson::Document(doc) => doc,
        _ => Document::new(),
    }
}
