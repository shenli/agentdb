use bson::{Bson, Document};
use serde_json::{Map, Number, Value};

/// Convert a BSON value to a JSON value. Types without a JSON equivalent
/// (ObjectId, Timestamp, Decimal128, Binary, etc.) are stringified.
pub fn bson_to_json(b: Bson) -> Value {
    match b {
        Bson::Double(n) => Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Bson::String(s) => Value::String(s),
        Bson::Array(a) => Value::Array(a.into_iter().map(bson_to_json).collect()),
        Bson::Document(d) => Value::Object(doc_to_map(d)),
        Bson::Boolean(b) => Value::Bool(b),
        Bson::Null => Value::Null,
        Bson::Int32(i) => Value::Number(Number::from(i)),
        Bson::Int64(i) => Value::Number(Number::from(i)),
        Bson::ObjectId(oid) => Value::String(oid.to_hex()),
        Bson::DateTime(dt) => Value::String(dt.to_string()),
        Bson::Timestamp(ts) => Value::String(format!("{}:{}", ts.time, ts.increment)),
        Bson::Decimal128(d) => Value::String(d.to_string()),
        Bson::Binary(bin) => Value::String(format!("{:?}", bin.bytes)),
        Bson::Symbol(s) => Value::String(s),
        Bson::RegularExpression(r) => Value::String(format!("/{}/{}", r.pattern, r.options)),
        Bson::JavaScriptCode(s)
        | Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope { code: s, .. }) => {
            Value::String(s)
        }
        Bson::DbPointer(_) | Bson::Undefined | Bson::MaxKey | Bson::MinKey => Value::Null,
    }
}

pub fn doc_to_map(d: Document) -> Map<String, Value> {
    let mut out = Map::with_capacity(d.len());
    for (k, v) in d {
        out.insert(k, bson_to_json(v));
    }
    out
}

pub fn doc_to_json(d: Document) -> Value {
    Value::Object(doc_to_map(d))
}

/// Convert a JSON value to BSON. Strings remain strings (no ObjectId inference).
pub fn json_to_bson(v: Value) -> Bson {
    match v {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else {
                n.as_f64().map(Bson::Double).unwrap_or(Bson::Null)
            }
        }
        Value::String(s) => Bson::String(s),
        Value::Array(a) => Bson::Array(a.into_iter().map(json_to_bson).collect()),
        Value::Object(o) => {
            let mut d = Document::new();
            for (k, v) in o {
                d.insert(k, json_to_bson(v));
            }
            Bson::Document(d)
        }
    }
}

pub fn json_to_doc(v: Value) -> Document {
    match json_to_bson(v) {
        Bson::Document(d) => d,
        other => {
            let mut d = Document::new();
            d.insert("value", other);
            d
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde_json::json;

    #[test]
    fn json_primitive_roundtrip() {
        let v = json!({"s": "hi", "n": 42, "b": true, "f": 1.5, "nil": null});
        let d = json_to_doc(v.clone());
        let back = doc_to_json(d);
        assert_eq!(back, v);
    }

    #[test]
    fn nested_array_and_object_roundtrip() {
        let v = json!({"a": [1, 2, {"x": [true, null]}]});
        let d = json_to_doc(v.clone());
        let back = doc_to_json(d);
        assert_eq!(back, v);
    }

    #[test]
    fn object_id_becomes_hex_string() {
        let oid = bson::oid::ObjectId::new();
        let d = doc! { "_id": oid };
        let v = doc_to_json(d);
        assert_eq!(v["_id"].as_str().unwrap(), oid.to_hex());
    }

    #[test]
    fn int32_preserved_as_integer_in_json() {
        let d = doc! { "n": 7i32 };
        let v = doc_to_json(d);
        assert_eq!(v["n"].as_i64(), Some(7));
    }
}
