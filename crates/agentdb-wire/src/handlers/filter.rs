use bson::Document as BsonDoc;
use serde_json::Value;

use crate::bsonx::bson_to_json;
use crate::error::{WireError, WireResult};

#[derive(Debug, Clone)]
enum Condition {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
}

#[derive(Debug, Clone)]
struct FieldMatch {
    path: Vec<String>,
    cond: Condition,
}

#[derive(Debug, Clone)]
pub struct Filter {
    conjuncts: Vec<FieldMatch>,
}

impl Filter {
    pub fn parse(doc: &BsonDoc) -> WireResult<Self> {
        let mut conjuncts = Vec::new();
        for (k, v) in doc.iter() {
            if k.starts_with('$') {
                return Err(WireError::Protocol(format!(
                    "top-level operator {k} not supported in filter"
                )));
            }
            let path: Vec<String> = k.split('.').map(|s| s.to_string()).collect();
            match v {
                bson::Bson::Document(inner) => {
                    // operator doc: { $op: value, ... }
                    for (op, opval) in inner.iter() {
                        let cond = match op.as_str() {
                            "$eq" => Condition::Eq(bson_to_json(opval.clone())),
                            "$ne" => Condition::Ne(bson_to_json(opval.clone())),
                            "$gt" => Condition::Gt(bson_to_json(opval.clone())),
                            "$gte" => Condition::Gte(bson_to_json(opval.clone())),
                            "$lt" => Condition::Lt(bson_to_json(opval.clone())),
                            "$lte" => Condition::Lte(bson_to_json(opval.clone())),
                            other => {
                                return Err(WireError::Protocol(format!(
                                    "filter operator {other} not supported"
                                )))
                            }
                        };
                        conjuncts.push(FieldMatch {
                            path: path.clone(),
                            cond,
                        });
                    }
                }
                other => conjuncts.push(FieldMatch {
                    path,
                    cond: Condition::Eq(bson_to_json(other.clone())),
                }),
            }
        }
        Ok(Filter { conjuncts })
    }

    pub fn matches(&self, value: &Value) -> bool {
        self.conjuncts.iter().all(|fm| match walk(value, &fm.path) {
            Some(got) => eval(&fm.cond, got),
            None => matches!(fm.cond, Condition::Ne(_)), // missing field only matches $ne
        })
    }

    /// Returns the top-level equality value for a known field, if present.
    /// Used by the planner to detect session-scoped queries.
    pub fn top_level_eq(&self, field: &str) -> Option<&Value> {
        self.conjuncts.iter().find_map(|fm| {
            if fm.path.len() == 1 && fm.path[0] == field {
                match &fm.cond {
                    Condition::Eq(v) => Some(v),
                    _ => None,
                }
            } else {
                None
            }
        })
    }
}

fn walk<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cur = root;
    for segment in path {
        cur = cur.get(segment)?;
    }
    Some(cur)
}

fn eval(cond: &Condition, got: &Value) -> bool {
    match cond {
        Condition::Eq(v) => got == v,
        Condition::Ne(v) => got != v,
        Condition::Gt(v) => cmp(got, v)
            .map(|o| o == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        Condition::Gte(v) => cmp(got, v)
            .map(|o| o != std::cmp::Ordering::Less)
            .unwrap_or(false),
        Condition::Lt(v) => cmp(got, v)
            .map(|o| o == std::cmp::Ordering::Less)
            .unwrap_or(false),
        Condition::Lte(v) => cmp(got, v)
            .map(|o| o != std::cmp::Ordering::Greater)
            .unwrap_or(false),
    }
}

fn cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    let _ = Ordering::Equal; // silence unused import if needed
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let xf = x.as_f64()?;
            let yf = y.as_f64()?;
            xf.partial_cmp(&yf)
        }
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde_json::json;

    #[test]
    fn equality_matches_top_level() {
        let f = Filter::parse(&doc! { "session_id": "s-1" }).unwrap();
        assert!(f.matches(&json!({"session_id": "s-1", "seq": 1})));
        assert!(!f.matches(&json!({"session_id": "s-2"})));
    }

    #[test]
    fn nested_path_matches() {
        let f = Filter::parse(&doc! { "payload.kind": "message" }).unwrap();
        assert!(f.matches(&json!({"payload": {"kind": "message"}})));
        assert!(!f.matches(&json!({"payload": {"kind": "tool_call"}})));
    }

    #[test]
    fn gt_operator_matches_numbers() {
        let f = Filter::parse(&doc! { "seq": { "$gt": 3 } }).unwrap();
        assert!(f.matches(&json!({"seq": 4})));
        assert!(!f.matches(&json!({"seq": 3})));
        assert!(!f.matches(&json!({"seq": 2})));
    }

    #[test]
    fn conjunction_across_fields() {
        let f = Filter::parse(&doc! { "session_id": "s-1", "seq": { "$gte": 2 } }).unwrap();
        assert!(f.matches(&json!({"session_id": "s-1", "seq": 2})));
        assert!(f.matches(&json!({"session_id": "s-1", "seq": 9})));
        assert!(!f.matches(&json!({"session_id": "s-1", "seq": 1})));
        assert!(!f.matches(&json!({"session_id": "s-2", "seq": 9})));
    }

    #[test]
    fn unsupported_operator_errors_out() {
        let err = Filter::parse(&doc! { "field": { "$regex": ".*" } }).unwrap_err();
        assert!(err.to_string().contains("$regex"));
    }

    #[test]
    fn top_level_eq_returns_scoped_value() {
        let f = Filter::parse(&doc! { "session_id": "s-9", "seq": { "$gt": 1 } }).unwrap();
        assert_eq!(f.top_level_eq("session_id"), Some(&json!("s-9")));
        assert!(f.top_level_eq("seq").is_none()); // seq has operator, not plain eq
    }
}
