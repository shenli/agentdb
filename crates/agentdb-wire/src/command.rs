use std::sync::Arc;

use agentdb_core::runtime::{
    AppendRequest, ContextInclude, ContextRequest, FetchRequest, ForkSessionRequest, SearchRequest,
};
use agentdb_core::{
    Id, IdKind, IndexingPolicy, Lifecycle, NewAgent, NewRecord, NewSession, NewSkill, NewTenant,
    Runtime, Session,
};
use bson::{doc, Bson, Document};
use serde_json::{json, Value};

use crate::bsonx::{document_to_json, json_to_document};
use crate::error::{WireError, WireResult};

const COLLECTIONS: &[&str] = &["sessions", "events", "records", "artifacts", "skills"];
const DEFAULT_AGENT_ALIAS: &str = "mongo-default";

#[derive(Clone, Copy, Debug, Default)]
pub struct CommandOptions {
    pub auth_enabled: bool,
}

#[derive(Clone)]
pub struct CommandContext {
    runtime: Arc<Runtime>,
}

impl CommandContext {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }

    pub async fn handle(&self, request: Document) -> WireResult<Document> {
        self.handle_with_options(request, CommandOptions::default())
            .await
    }

    pub async fn handle_with_options(
        &self,
        request: Document,
        options: CommandOptions,
    ) -> WireResult<Document> {
        let command = command_name(&request)?;
        match command.as_str() {
            "hello" | "isMaster" | "ismaster" => Ok(hello_response(options.auth_enabled)),
            "ping" => Ok(doc! { "ok": 1.0 }),
            "buildInfo" | "buildinfo" => Ok(doc! {
                "version": env!("CARGO_PKG_VERSION"),
                "gitVersion": "",
                "modules": Bson::Array(vec![]),
                "allocator": "system",
                "javascriptEngine": "none",
                "agentdb": true,
                "ok": 1.0,
            }),
            "endSessions" => Ok(doc! { "ok": 1.0 }),
            "listDatabases" => self.list_databases().await,
            "listCollections" => self.list_collections(&request).await,
            "insert" => self.insert(&request).await,
            "find" => self.find(&request).await,
            "count" => self.count(&request).await,
            "aggregate" => self.aggregate(&request).await,
            "createIndexes" => Ok(doc! {
                "createdCollectionAutomatically": false,
                "numIndexesBefore": 1_i32,
                "numIndexesAfter": 1_i32,
                "ok": 1.0,
            }),
            "agentdbAppendAndFetch" => self.agentdb_append_and_fetch(&request).await,
            "agentdbFetchRecent" => self.agentdb_fetch_recent(&request).await,
            "agentdbFork" => self.agentdb_fork(&request).await,
            "agentdbReplay" => self.agentdb_replay(&request).await,
            "agentdbContext" => self.agentdb_context(&request).await,
            "agentdbTransitionLifecycle" => self.agentdb_transition_lifecycle(&request).await,
            "agentdbRestoreSession" => self.agentdb_restore_session(&request).await,
            other => Ok(command_error(other)),
        }
    }

    async fn list_databases(&self) -> WireResult<Document> {
        let mut dbs = Vec::new();
        for tenant in self.runtime.list_tenants().await {
            let name = if tenant.external_key.is_empty() {
                tenant.tenant_id.to_string()
            } else {
                tenant.external_key
            };
            dbs.push(Bson::Document(doc! {
                "name": name,
                "sizeOnDisk": 1_i64,
                "empty": false,
            }));
        }
        Ok(doc! {
            "databases": Bson::Array(dbs),
            "totalSize": 1_i64,
            "ok": 1.0,
        })
    }

    async fn list_collections(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let first_batch = COLLECTIONS
            .iter()
            .map(|name| {
                Bson::Document(doc! {
                    "name": *name,
                    "type": "collection",
                    "options": Bson::Document(Document::new()),
                    "info": Bson::Document(doc! { "readOnly": false }),
                    "idIndex": Bson::Document(doc! {
                        "v": 2_i32,
                        "key": Bson::Document(doc! { "_id": 1_i32 }),
                        "name": "_id_",
                    }),
                })
            })
            .collect::<Vec<_>>();
        Ok(cursor_response(db, "$cmd.listCollections", first_batch))
    }

    async fn insert(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let collection = request.get_str("insert").map_err(|_| {
            WireError::Protocol("insert command missing collection name".to_string())
        })?;
        let documents = request.get_array("documents").map_err(|_| {
            WireError::Protocol("insert command missing documents array".to_string())
        })?;
        let tenant = self.ensure_tenant(db).await?;
        let mut inserted = 0i32;

        for value in documents {
            let Bson::Document(document) = value else {
                continue;
            };
            match collection {
                "events" => {
                    self.insert_event(&tenant.tenant_id, document.clone())
                        .await?;
                    inserted += 1;
                }
                "sessions" => {
                    self.insert_session(&tenant.tenant_id, document.clone())
                        .await?;
                    inserted += 1;
                }
                "records" => {
                    self.insert_record(&tenant.tenant_id, document.clone())
                        .await?;
                    inserted += 1;
                }
                "skills" => {
                    self.insert_skill(&tenant.tenant_id, document.clone())
                        .await?;
                    inserted += 1;
                }
                other => return Ok(command_error(&format!("insert into {other}"))),
            }
        }

        Ok(doc! { "n": inserted, "ok": 1.0 })
    }

    async fn find(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let collection = request
            .get_str("find")
            .map_err(|_| WireError::Protocol("find command missing collection name".to_string()))?;
        let filter = request
            .get_document("filter")
            .cloned()
            .unwrap_or_else(|_| Document::new());
        let limit = limit_from_request(request);
        let tenant = self.ensure_tenant(db).await?;
        let first_batch = match collection {
            "sessions" => {
                self.find_sessions(&tenant.tenant_id, &filter, limit)
                    .await?
            }
            "events" => self.find_events(&tenant.tenant_id, &filter, limit).await?,
            "records" => self.find_records(&tenant.tenant_id, &filter, limit).await?,
            "artifacts" => {
                self.find_artifacts(&tenant.tenant_id, &filter, limit)
                    .await?
            }
            "skills" => self.find_skills(&tenant.tenant_id, &filter, limit).await?,
            other => return Ok(command_error(&format!("find on {other}"))),
        };
        Ok(cursor_response(db, collection, first_batch))
    }

    async fn count(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let collection = request.get_str("count").map_err(|_| {
            WireError::Protocol("count command missing collection name".to_string())
        })?;
        let filter = request
            .get_document("query")
            .or_else(|_| request.get_document("filter"))
            .cloned()
            .unwrap_or_else(|_| Document::new());
        if !COLLECTIONS.contains(&collection) {
            return Ok(command_error(&format!("count on {collection}")));
        }
        let tenant = self.ensure_tenant(db).await?;
        let n = self
            .find_collection_batch(&tenant.tenant_id, collection, &filter, usize::MAX)
            .await?
            .len() as i64;
        Ok(doc! { "n": n, "ok": 1.0 })
    }

    async fn aggregate(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let collection = request.get_str("aggregate").map_err(|_| {
            WireError::Protocol("aggregate command missing collection name".to_string())
        })?;
        if !COLLECTIONS.contains(&collection) {
            return Ok(command_error(&format!("aggregate on {collection}")));
        }
        let mut filter = Document::new();
        let mut count_field = None;
        let pipeline = request.get_array("pipeline").cloned().unwrap_or_default();
        for stage in pipeline {
            let Some(stage) = stage.as_document() else {
                continue;
            };
            if let Ok(match_doc) = stage.get_document("$match") {
                filter.extend(match_doc.clone());
            } else if let Ok(field) = stage.get_str("$count") {
                count_field = Some(field.to_string());
            }
        }
        let tenant = self.ensure_tenant(db).await?;
        let batch = self
            .find_collection_batch(&tenant.tenant_id, collection, &filter, usize::MAX)
            .await?;
        if let Some(field) = count_field {
            return Ok(cursor_response(
                db,
                collection,
                vec![Bson::Document(doc! { field: batch.len() as i64 })],
            ));
        }
        Ok(cursor_response(db, collection, batch))
    }

    async fn agentdb_append_and_fetch(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session = self.ensure_session(&tenant.tenant_id, request).await?;
        let kind = request.get_str("kind").unwrap_or("message").to_string();
        let idempotency_key = request.get_str("idempotency_key").ok().map(str::to_string);
        let payload = request
            .get("payload")
            .cloned()
            .map(crate::bsonx::bson_to_json)
            .unwrap_or(Value::Null);
        let fetch_last_n = request
            .get_i32("fetch_last_n")
            .map(|value| value.max(0) as usize)
            .or_else(|_| {
                request
                    .get_i64("fetch_last_n")
                    .map(|value| value.max(0) as usize)
            })
            .unwrap_or(20);
        let max_bytes = request
            .get_i32("max_bytes")
            .map(|value| value.max(0) as usize)
            .or_else(|_| {
                request
                    .get_i64("max_bytes")
                    .map(|value| value.max(0) as usize)
            })
            .unwrap_or(256 * 1024);
        let response = self
            .runtime
            .append_and_fetch(
                AppendRequest {
                    tenant_id: tenant.tenant_id,
                    agent_id: session.agent_id,
                    session_id: session.session_id,
                    kind,
                    payload,
                    artifact_refs: vec![],
                    source_refs: vec![],
                    idempotency_key,
                },
                fetch_last_n,
                max_bytes,
            )
            .await?;
        Ok(doc! {
            "event_id": response.event_id.to_string(),
            "seq": response.seq as i64,
            "recent": Bson::Array(response.recent.into_iter().map(|event| {
                Bson::Document(json_to_document(serde_json::to_value(event).unwrap_or(Value::Null)))
            }).collect()),
            "ok": 1.0,
        })
    }

    async fn agentdb_fork(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session_id = required_id(request, "session_id", IdKind::Session)?;
        let at_seq = request
            .get_i64("at_seq")
            .map(|value| value.max(0) as u64)
            .or_else(|_| request.get_i32("at_seq").map(|value| value.max(0) as u64))
            .unwrap_or(0);
        let alias = request.get_str("alias").ok().map(str::to_string);
        let session = self
            .runtime
            .fork_session(ForkSessionRequest {
                tenant_id: tenant.tenant_id,
                session_id,
                at_seq,
                alias,
            })
            .await?;
        Ok(doc! {
            "session": Bson::Document(json_to_document(serde_json::to_value(session).unwrap_or(Value::Null))),
            "ok": 1.0,
        })
    }

    async fn agentdb_fetch_recent(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session_id = required_id(request, "session_id", IdKind::Session)?;
        let limit = request
            .get_i32("limit")
            .map(|value| value.max(0) as usize)
            .or_else(|_| request.get_i64("limit").map(|value| value.max(0) as usize))
            .unwrap_or(20);
        let events = self
            .runtime
            .fetch_recent(tenant.tenant_id, session_id, limit)
            .await?;
        Ok(doc! {
            "events": Bson::Array(events.into_iter().map(|event| {
                Bson::Document(json_to_document(serde_json::to_value(event).unwrap_or(Value::Null)))
            }).collect()),
            "ok": 1.0,
        })
    }

    async fn agentdb_replay(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session_id = required_id(request, "session_id", IdKind::Session)?;
        let replay = self
            .runtime
            .replay_session(&tenant.tenant_id, &session_id)
            .await?;
        Ok(doc! {
            "replay": Bson::Document(json_to_document(serde_json::to_value(replay).unwrap_or(Value::Null))),
            "ok": 1.0,
        })
    }

    async fn agentdb_context(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session_id = required_id(request, "session_id", IdKind::Session)?;
        let max_events = request
            .get_i32("max_events")
            .map(|value| value.max(0) as usize)
            .or_else(|_| {
                request
                    .get_i64("max_events")
                    .map(|value| value.max(0) as usize)
            })
            .unwrap_or(20);
        let max_bytes = request
            .get_i32("max_bytes")
            .map(|value| value.max(0) as usize)
            .or_else(|_| {
                request
                    .get_i64("max_bytes")
                    .map(|value| value.max(0) as usize)
            })
            .unwrap_or(256 * 1024);
        let include = context_include_from_request(request);
        let response = self
            .runtime
            .build_context(ContextRequest {
                tenant_id: tenant.tenant_id,
                session_id,
                max_events,
                max_bytes,
                include,
                record_query: None,
            })
            .await?;
        Ok(doc! {
            "context": Bson::Document(json_to_document(serde_json::to_value(response).unwrap_or(Value::Null))),
            "ok": 1.0,
        })
    }

    async fn agentdb_transition_lifecycle(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session_id = required_id(request, "session_id", IdKind::Session)?;
        let target = parse_lifecycle(request.get_str("target").unwrap_or("warm"))?;
        let summary = self
            .runtime
            .transition_session_lifecycle(&tenant.tenant_id, &session_id, target)
            .await?;
        Ok(doc! {
            "storage": Bson::Document(json_to_document(serde_json::to_value(summary).unwrap_or(Value::Null))),
            "ok": 1.0,
        })
    }

    async fn agentdb_restore_session(&self, request: &Document) -> WireResult<Document> {
        let db = request.get_str("$db").unwrap_or("agentdb");
        let tenant = self.ensure_tenant(db).await?;
        let session_id = required_id(request, "session_id", IdKind::Session)?;
        let target = parse_lifecycle(request.get_str("target").unwrap_or("warm"))?;
        let summary = self
            .runtime
            .restore_session_from_archive(&tenant.tenant_id, &session_id, target)
            .await?;
        Ok(doc! {
            "storage": Bson::Document(json_to_document(serde_json::to_value(summary).unwrap_or(Value::Null))),
            "ok": 1.0,
        })
    }

    async fn ensure_tenant(&self, external_key: &str) -> WireResult<agentdb_core::Tenant> {
        if let Some(existing) = self
            .runtime
            .list_tenants()
            .await
            .into_iter()
            .find(|tenant| {
                tenant.external_key == external_key || tenant.tenant_id.as_str() == external_key
            })
        {
            return Ok(existing);
        }
        match self
            .runtime
            .create_tenant(NewTenant {
                external_key: external_key.to_string(),
                policy: json!({}),
            })
            .await
        {
            Ok(tenant) => Ok(tenant),
            Err(_) => self
                .runtime
                .list_tenants()
                .await
                .into_iter()
                .find(|tenant| tenant.external_key == external_key)
                .ok_or_else(|| WireError::Protocol(format!("tenant not found: {external_key}"))),
        }
    }

    async fn ensure_default_agent(&self, tenant_id: &Id) -> WireResult<agentdb_core::Agent> {
        if let Some(existing) = self
            .runtime
            .list_agents(tenant_id)
            .await?
            .into_iter()
            .find(|agent| agent.alias.as_deref() == Some(DEFAULT_AGENT_ALIAS))
        {
            return Ok(existing);
        }
        match self
            .runtime
            .create_agent(NewAgent {
                tenant_id: tenant_id.clone(),
                alias: Some(DEFAULT_AGENT_ALIAS.to_string()),
                config: json!({ "created_by": "mongodb_compat" }),
            })
            .await
        {
            Ok(agent) => Ok(agent),
            Err(_) => self
                .runtime
                .list_agents(tenant_id)
                .await?
                .into_iter()
                .find(|agent| agent.alias.as_deref() == Some(DEFAULT_AGENT_ALIAS))
                .ok_or_else(|| WireError::Protocol("default Mongo agent not found".to_string())),
        }
    }

    async fn ensure_session(&self, tenant_id: &Id, document: &Document) -> WireResult<Session> {
        if let Ok(raw) = document.get_str("session_id") {
            let session_id = Id::parse_kind(raw, IdKind::Session)
                .map_err(|e| WireError::Protocol(e.to_string()))?;
            return self
                .runtime
                .get_session(tenant_id, &session_id)
                .await
                .map_err(Into::into);
        }

        let alias = document
            .get_str("session_alias")
            .ok()
            .or_else(|| document.get_str("session").ok())
            .unwrap_or("mongo-session");
        if let Some(existing) = self
            .runtime
            .list_sessions(tenant_id)
            .await?
            .into_iter()
            .find(|session| session.alias.as_deref() == Some(alias))
        {
            return Ok(existing);
        }

        let agent = if let Ok(raw) = document.get_str("agent_id") {
            let agent_id = Id::parse_kind(raw, IdKind::Agent)
                .map_err(|e| WireError::Protocol(e.to_string()))?;
            self.runtime.get_agent(tenant_id, &agent_id).await?
        } else {
            self.ensure_default_agent(tenant_id).await?
        };
        self.runtime
            .create_session(NewSession {
                tenant_id: tenant_id.clone(),
                agent_id: agent.agent_id,
                alias: Some(alias.to_string()),
            })
            .await
            .map_err(Into::into)
    }

    async fn insert_session(&self, tenant_id: &Id, document: Document) -> WireResult<Session> {
        let agent = if let Ok(raw) = document.get_str("agent_id") {
            let agent_id = Id::parse_kind(raw, IdKind::Agent)
                .map_err(|e| WireError::Protocol(e.to_string()))?;
            self.runtime.get_agent(tenant_id, &agent_id).await?
        } else {
            self.ensure_default_agent(tenant_id).await?
        };
        let alias = document
            .get_str("alias")
            .ok()
            .or_else(|| document.get_str("session_alias").ok())
            .map(str::to_string);
        self.runtime
            .create_session(NewSession {
                tenant_id: tenant_id.clone(),
                agent_id: agent.agent_id,
                alias,
            })
            .await
            .map_err(Into::into)
    }

    async fn insert_event(&self, tenant_id: &Id, document: Document) -> WireResult<()> {
        let session = self.ensure_session(tenant_id, &document).await?;
        let kind = document.get_str("kind").unwrap_or("message").to_string();
        let idempotency_key = document.get_str("idempotency_key").ok().map(str::to_string);
        let payload = document
            .get("payload")
            .cloned()
            .map(crate::bsonx::bson_to_json)
            .unwrap_or_else(|| document_to_json(strip_agentdb_fields(document)));
        self.runtime
            .append(AppendRequest {
                tenant_id: tenant_id.clone(),
                agent_id: session.agent_id,
                session_id: session.session_id,
                kind,
                payload,
                artifact_refs: vec![],
                source_refs: vec![],
                idempotency_key,
            })
            .await?;
        Ok(())
    }

    async fn insert_record(&self, tenant_id: &Id, document: Document) -> WireResult<()> {
        let namespace =
            get_string_array(&document, "namespace").unwrap_or_else(|| vec!["default".into()]);
        let tags = get_string_array(&document, "tags").unwrap_or_default();
        let metadata = document
            .get_document("metadata")
            .cloned()
            .map(document_to_json)
            .unwrap_or(Value::Null);
        let text = document.get_str("text").ok().map(str::to_string);
        self.runtime
            .write_record(NewRecord {
                tenant_id: tenant_id.clone(),
                namespace,
                scope: Value::Null,
                text,
                metadata,
                tags,
                source: None,
                embedding: None,
                policy: IndexingPolicy::default(),
                expires_at_ms: None,
            })
            .await?;
        Ok(())
    }

    async fn insert_skill(&self, tenant_id: &Id, document: Document) -> WireResult<()> {
        let name = document
            .get_str("name")
            .unwrap_or("mongo-skill")
            .to_string();
        let version = document.get_str("version").unwrap_or("0.0.0").to_string();
        let manifest = document
            .get_document("manifest")
            .cloned()
            .map(document_to_json)
            .unwrap_or(Value::Null);
        let idempotency_key = document.get_str("idempotency_key").ok().map(str::to_string);
        self.runtime
            .register_skill(
                NewSkill {
                    tenant_id: tenant_id.clone(),
                    name,
                    version,
                    manifest,
                    artifact_refs: vec![],
                },
                idempotency_key,
            )
            .await?;
        Ok(())
    }

    async fn find_sessions(
        &self,
        tenant_id: &Id,
        filter: &Document,
        limit: usize,
    ) -> WireResult<Vec<Bson>> {
        let mut out = Vec::new();
        for session in self.runtime.list_sessions(tenant_id).await? {
            let doc = json_to_document(serde_json::to_value(session).unwrap_or(Value::Null));
            if matches_filter(&doc, filter) {
                out.push(Bson::Document(doc));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn find_events(
        &self,
        tenant_id: &Id,
        filter: &Document,
        limit: usize,
    ) -> WireResult<Vec<Bson>> {
        let mut out = Vec::new();
        let sessions = self.runtime.list_sessions(tenant_id).await?;
        for session in sessions {
            if let Ok(filter_session) = filter.get_str("session_id") {
                if filter_session != session.session_id.as_str() {
                    continue;
                }
            }
            let events = self
                .runtime
                .fetch_from_seq(FetchRequest {
                    tenant_id: tenant_id.clone(),
                    session_id: session.session_id,
                    from_seq: 0,
                    limit,
                })
                .await?;
            for event in events {
                let doc = json_to_document(serde_json::to_value(event).unwrap_or(Value::Null));
                if matches_filter(&doc, filter) {
                    out.push(Bson::Document(doc));
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn find_records(
        &self,
        tenant_id: &Id,
        filter: &Document,
        limit: usize,
    ) -> WireResult<Vec<Bson>> {
        let namespace =
            get_string_array(filter, "namespace").unwrap_or_else(|| vec!["default".into()]);
        let text = filter.get_str("$text").ok().map(str::to_string);
        let records = self
            .runtime
            .search_records(SearchRequest {
                tenant_id: tenant_id.clone(),
                namespace,
                sharding_key: filter.get_str("session_id").ok().map(str::to_string),
                text,
                embedding: None,
                tags: get_string_array(filter, "tags").unwrap_or_default(),
                source_filter: None,
                limit,
                min_visible_record_commit_seq: None,
            })
            .await?;
        Ok(records
            .into_iter()
            .map(|record| {
                Bson::Document(json_to_document(
                    serde_json::to_value(record).unwrap_or(Value::Null),
                ))
            })
            .collect())
    }

    async fn find_artifacts(
        &self,
        tenant_id: &Id,
        filter: &Document,
        limit: usize,
    ) -> WireResult<Vec<Bson>> {
        let mut out = Vec::new();
        for artifact in self.runtime.list_artifacts(tenant_id).await? {
            let doc = json_to_document(serde_json::to_value(artifact).unwrap_or(Value::Null));
            if matches_filter(&doc, filter) {
                out.push(Bson::Document(doc));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn find_skills(
        &self,
        tenant_id: &Id,
        filter: &Document,
        limit: usize,
    ) -> WireResult<Vec<Bson>> {
        let mut out = Vec::new();
        for skill in self.runtime.list_skills(tenant_id).await? {
            let doc = json_to_document(serde_json::to_value(skill).unwrap_or(Value::Null));
            if matches_filter(&doc, filter) {
                out.push(Bson::Document(doc));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn find_collection_batch(
        &self,
        tenant_id: &Id,
        collection: &str,
        filter: &Document,
        limit: usize,
    ) -> WireResult<Vec<Bson>> {
        match collection {
            "sessions" => self.find_sessions(tenant_id, filter, limit).await,
            "events" => self.find_events(tenant_id, filter, limit).await,
            "records" => self.find_records(tenant_id, filter, limit).await,
            "artifacts" => self.find_artifacts(tenant_id, filter, limit).await,
            "skills" => self.find_skills(tenant_id, filter, limit).await,
            other => Err(WireError::Protocol(format!(
                "unsupported collection: {other}"
            ))),
        }
    }
}

pub fn command_name(request: &Document) -> WireResult<String> {
    request
        .keys()
        .find(|key| !key.starts_with('$') && key.as_str() != "lsid")
        .cloned()
        .ok_or_else(|| WireError::Protocol("empty Mongo command".to_string()))
}

fn hello_response(auth_enabled: bool) -> Document {
    let mut response = doc! {
        "ok": 1.0,
        "isWritablePrimary": true,
        "ismaster": true,
        "helloOk": true,
        "maxWireVersion": 17_i32,
        "minWireVersion": 0_i32,
        "maxBsonObjectSize": 16_777_216_i32,
        "maxMessageSizeBytes": 48_000_000_i32,
        "maxWriteBatchSize": 100_000_i32,
        "logicalSessionTimeoutMinutes": 30_i32,
        "connectionId": 1_i32,
    };
    if auth_enabled {
        response.insert(
            "saslSupportedMechs",
            Bson::Array(vec![Bson::String("PLAIN".into())]),
        );
    }
    response
}

fn cursor_response(db: &str, collection: &str, first_batch: Vec<Bson>) -> Document {
    doc! {
        "cursor": Bson::Document(doc! {
            "id": 0_i64,
            "ns": format!("{db}.{collection}"),
            "firstBatch": Bson::Array(first_batch),
        }),
        "ok": 1.0,
    }
}

fn command_error(command: &str) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": format!("{command} not supported by AgentDB MongoDB compatibility subset"),
        "code": 59_i32,
        "codeName": "CommandNotFound",
    }
}

fn strip_agentdb_fields(mut document: Document) -> Document {
    for key in [
        "_id",
        "tenant_id",
        "agent_id",
        "session_id",
        "session",
        "session_alias",
        "kind",
        "idempotency_key",
    ] {
        document.remove(key);
    }
    document
}

fn get_string_array(doc: &Document, key: &str) -> Option<Vec<String>> {
    match doc.get(key)? {
        Bson::String(value) => Some(vec![value.clone()]),
        Bson::Array(values) => Some(
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

fn limit_from_request(doc: &Document) -> usize {
    let raw = match doc.get("limit") {
        Some(Bson::Int32(v)) => i64::from(*v),
        Some(Bson::Int64(v)) => *v,
        Some(Bson::Double(v)) => *v as i64,
        _ => 100,
    };
    if raw == 0 {
        100
    } else {
        raw.unsigned_abs() as usize
    }
}

fn required_id(doc: &Document, key: &str, kind: IdKind) -> WireResult<Id> {
    let raw = doc
        .get_str(key)
        .map_err(|_| WireError::Protocol(format!("{key} is required")))?;
    Id::parse_kind(raw, kind).map_err(|e| WireError::Protocol(e.to_string()))
}

fn parse_lifecycle(raw: &str) -> WireResult<Lifecycle> {
    match raw {
        "warm" => Ok(Lifecycle::Warm),
        "cold" => Ok(Lifecycle::Cold),
        "archive" => Ok(Lifecycle::Archive),
        "derivative_only" => Ok(Lifecycle::DerivativeOnly),
        other => Err(WireError::Protocol(format!(
            "unsupported lifecycle target: {other}"
        ))),
    }
}

fn context_include_from_request(doc: &Document) -> Vec<ContextInclude> {
    let Some(Bson::Array(values)) = doc.get("include") else {
        return vec![
            ContextInclude::RecentEvents,
            ContextInclude::Records,
            ContextInclude::Skills,
            ContextInclude::Policy,
        ];
    };
    let mut include = Vec::new();
    for value in values {
        match value.as_str() {
            Some("recent_events") | Some("events") => include.push(ContextInclude::RecentEvents),
            Some("records") => include.push(ContextInclude::Records),
            Some("skills") => include.push(ContextInclude::Skills),
            Some("policy") => include.push(ContextInclude::Policy),
            _ => {}
        }
    }
    if include.is_empty() {
        include.push(ContextInclude::RecentEvents);
    }
    include
}

fn matches_filter(doc: &Document, filter: &Document) -> bool {
    filter.iter().all(|(key, expected)| {
        if key.starts_with('$') {
            return true;
        }
        doc.get(key)
            .is_some_and(|actual| bson_values_equal(actual, expected))
    })
}

fn bson_values_equal(actual: &Bson, expected: &Bson) -> bool {
    match (actual, expected) {
        (Bson::String(a), Bson::String(b)) => a == b,
        (Bson::Int32(a), Bson::Int32(b)) => a == b,
        (Bson::Int64(a), Bson::Int64(b)) => a == b,
        (Bson::Int64(a), Bson::Int32(b)) => *a == i64::from(*b),
        (Bson::Int32(a), Bson::Int64(b)) => i64::from(*a) == *b,
        (Bson::Boolean(a), Bson::Boolean(b)) => a == b,
        _ => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unsupported_command_returns_mongo_error_document() {
        let ctx = CommandContext::new(Runtime::in_memory());

        let response = ctx
            .handle(doc! { "drop": "events", "$db": "tenant_a" })
            .await
            .unwrap();

        assert_eq!(response.get_f64("ok").unwrap(), 0.0);
        assert_eq!(response.get_i32("code").unwrap(), 59);
        assert!(response
            .get_str("errmsg")
            .unwrap()
            .contains("not supported"));
    }

    #[tokio::test]
    async fn insert_event_creates_tenant_session_and_find_reads_event() {
        let ctx = CommandContext::new(Runtime::in_memory());

        let inserted = ctx
            .handle(doc! {
                "insert": "events",
                "$db": "tenant_a",
                "documents": [{
                    "session_alias": "session-a",
                    "kind": "message",
                    "payload": { "content": "hello" }
                }]
            })
            .await
            .unwrap();
        assert_eq!(inserted.get_i32("n").unwrap(), 1);

        let response = ctx
            .handle(doc! {
                "find": "events",
                "$db": "tenant_a",
                "filter": { "kind": "message" },
                "limit": 0_i32,
            })
            .await
            .unwrap();
        let first_batch = response
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(first_batch.len(), 1);
        let event = first_batch[0].as_document().unwrap();
        assert_eq!(event.get_str("kind").unwrap(), "message");
        assert_eq!(
            event
                .get_document("payload")
                .unwrap()
                .get_str("content")
                .unwrap(),
            "hello"
        );
    }

    #[tokio::test]
    async fn insert_record_and_find_by_namespace() {
        let ctx = CommandContext::new(Runtime::in_memory());

        ctx.handle(doc! {
            "insert": "records",
            "$db": "tenant_a",
            "documents": [{
                "namespace": ["memory"],
                "text": "prefers short updates",
                "tags": ["preference"]
            }]
        })
        .await
        .unwrap();

        let response = ctx
            .handle(doc! {
                "find": "records",
                "$db": "tenant_a",
                "filter": { "namespace": ["memory"] },
            })
            .await
            .unwrap();
        let first_batch = response
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(first_batch.len(), 1);
        let record = first_batch[0].as_document().unwrap();
        assert_eq!(record.get_str("text").unwrap(), "prefers short updates");
        assert!(record.get_str("record_id").unwrap().starts_with("rec_"));
    }

    #[tokio::test]
    async fn custom_append_and_fetch_returns_recent_events() {
        let ctx = CommandContext::new(Runtime::in_memory());

        let response = ctx
            .handle(doc! {
                "agentdbAppendAndFetch": "events",
                "$db": "tenant_a",
                "session_alias": "session-a",
                "kind": "message",
                "payload": { "content": "hello" },
                "fetch_last_n": 10_i32,
            })
            .await
            .unwrap();

        assert_eq!(response.get_f64("ok").unwrap(), 1.0);
        assert!(response.get_str("event_id").unwrap().starts_with("evt_"));
        assert_eq!(response.get_i64("seq").unwrap(), 0);
        assert_eq!(response.get_array("recent").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn custom_fetch_recent_returns_tail_events() {
        let ctx = CommandContext::new(Runtime::in_memory());

        let first = ctx
            .handle(doc! {
                "agentdbAppendAndFetch": "events",
                "$db": "tenant_a",
                "session_alias": "session-a",
                "kind": "message",
                "payload": { "content": "one" },
                "fetch_last_n": 1_i32,
            })
            .await
            .unwrap();
        assert_eq!(first.get_i64("seq").unwrap(), 0);

        for content in ["two", "three"] {
            ctx.handle(doc! {
                "agentdbAppendAndFetch": "events",
                "$db": "tenant_a",
                "session_alias": "session-a",
                "kind": "message",
                "payload": { "content": content },
                "fetch_last_n": 1_i32,
            })
            .await
            .unwrap();
        }

        let session = ctx
            .handle(doc! {
                "find": "sessions",
                "$db": "tenant_a",
                "filter": { "alias": "session-a" },
            })
            .await
            .unwrap();
        let session_id = session
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()[0]
            .as_document()
            .unwrap()
            .get_str("session_id")
            .unwrap()
            .to_string();

        let response = ctx
            .handle(doc! {
                "agentdbFetchRecent": "events",
                "$db": "tenant_a",
                "session_id": session_id,
                "limit": 2_i32,
            })
            .await
            .unwrap();
        let events = response.get_array("events").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].as_document().unwrap().get_i64("seq").unwrap(), 1);
        assert_eq!(events[1].as_document().unwrap().get_i64("seq").unwrap(), 2);
    }

    #[tokio::test]
    async fn count_create_indexes_and_simple_aggregate_work() {
        let ctx = CommandContext::new(Runtime::in_memory());

        for content in ["one", "two"] {
            ctx.handle(doc! {
                "insert": "events",
                "$db": "tenant_a",
                "documents": [{
                    "session_alias": "session-a",
                    "kind": "message",
                    "payload": { "content": content }
                }]
            })
            .await
            .unwrap();
        }

        let indexes = ctx
            .handle(doc! {
                "createIndexes": "events",
                "$db": "tenant_a",
                "indexes": [{ "key": { "kind": 1_i32 }, "name": "kind_1" }]
            })
            .await
            .unwrap();
        assert_eq!(indexes.get_f64("ok").unwrap(), 1.0);

        let count = ctx
            .handle(doc! { "count": "events", "$db": "tenant_a", "query": { "kind": "message" } })
            .await
            .unwrap();
        assert_eq!(count.get_i64("n").unwrap(), 2);

        let aggregate = ctx
            .handle(doc! {
                "aggregate": "events",
                "$db": "tenant_a",
                "pipeline": [{ "$match": { "kind": "message" } }, { "$count": "n" }],
                "cursor": {}
            })
            .await
            .unwrap();
        let first_batch = aggregate
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(
            first_batch[0].as_document().unwrap().get_i64("n").unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn skills_and_artifacts_are_findable() {
        let ctx = CommandContext::new(Runtime::in_memory());
        let tenant = ctx.ensure_tenant("tenant_a").await.unwrap();

        ctx.handle(doc! {
            "insert": "skills",
            "$db": "tenant_a",
            "documents": [{
                "name": "summarize",
                "version": "1.0.0",
                "manifest": { "entrypoint": "summarize" }
            }]
        })
        .await
        .unwrap();
        let skills = ctx
            .handle(doc! { "find": "skills", "$db": "tenant_a", "filter": { "name": "summarize" } })
            .await
            .unwrap();
        let skill_batch = skills
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(skill_batch.len(), 1);
        assert_eq!(
            skill_batch[0]
                .as_document()
                .unwrap()
                .get_str("version")
                .unwrap(),
            "1.0.0"
        );

        let artifact = ctx
            .runtime
            .create_artifact(
                agentdb_core::NewArtifact {
                    tenant_id: tenant.tenant_id,
                    content_type: Some("text/plain".to_string()),
                    expected_sha256: None,
                },
                b"hello artifact".to_vec(),
            )
            .await
            .unwrap();
        let artifacts = ctx
            .handle(doc! {
                "find": "artifacts",
                "$db": "tenant_a",
                "filter": { "artifact_id": artifact.artifact_id.to_string() }
            })
            .await
            .unwrap();
        let artifact_batch = artifacts
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(artifact_batch.len(), 1);
        assert_eq!(
            artifact_batch[0]
                .as_document()
                .unwrap()
                .get_str("content_type")
                .unwrap(),
            "text/plain"
        );
    }

    #[tokio::test]
    async fn fork_replay_context_and_lifecycle_commands_work() {
        let ctx = CommandContext::new(Runtime::in_memory());

        for content in ["first", "second"] {
            ctx.handle(doc! {
                "agentdbAppendAndFetch": "events",
                "$db": "tenant_a",
                "session_alias": "session-a",
                "kind": "message",
                "payload": { "content": content },
                "fetch_last_n": 10_i32,
            })
            .await
            .unwrap();
        }
        let sessions = ctx
            .handle(
                doc! { "find": "sessions", "$db": "tenant_a", "filter": { "alias": "session-a" } },
            )
            .await
            .unwrap();
        let session_id = sessions
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()[0]
            .as_document()
            .unwrap()
            .get_str("session_id")
            .unwrap()
            .to_string();

        let fork = ctx
            .handle(doc! {
                "agentdbFork": "sessions",
                "$db": "tenant_a",
                "session_id": session_id.clone(),
                "at_seq": 0_i32,
                "alias": "fork-a",
            })
            .await
            .unwrap();
        let child_id = fork
            .get_document("session")
            .unwrap()
            .get_str("session_id")
            .unwrap()
            .to_string();

        let replay = ctx
            .handle(doc! { "agentdbReplay": "sessions", "$db": "tenant_a", "session_id": child_id.clone() })
            .await
            .unwrap();
        assert_eq!(
            replay
                .get_document("replay")
                .unwrap()
                .get_array("events")
                .unwrap()
                .len(),
            1
        );

        let context = ctx
            .handle(doc! {
                "agentdbContext": "sessions",
                "$db": "tenant_a",
                "session_id": session_id.clone(),
                "include": ["events", "policy"],
                "max_events": 10_i32,
            })
            .await
            .unwrap();
        assert_eq!(context.get_f64("ok").unwrap(), 1.0);
        assert_eq!(
            context
                .get_document("context")
                .unwrap()
                .get_array("events")
                .unwrap()
                .len(),
            2
        );

        let lifecycle = ctx
            .handle(doc! {
                "agentdbTransitionLifecycle": "sessions",
                "$db": "tenant_a",
                "session_id": session_id,
                "target": "warm",
            })
            .await
            .unwrap();
        assert_eq!(lifecycle.get_f64("ok").unwrap(), 1.0);
    }
}
