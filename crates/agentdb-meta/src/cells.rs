//! Logical isolation-domain metadata.
//!
//! A cell is a scheduling and routing boundary inside the shared metadata DB.
//! It is not a storage primitive and does not create a separate metadata
//! database.

use sqlx::AnyPool;

use crate::error::{MetaError, MetaResult};

pub const DEFAULT_CELL_ID: &str = "default";
pub const DEFAULT_DOMAIN_CLASS: &str = "standard";
pub const DEFAULT_DEPLOYMENT_PROFILE: &str = "single_az_cluster";
pub const DEFAULT_CAPACITY_CLASS: &str = "stable";
pub const DEFAULT_METADATA_ISOLATION_LEVEL: &str = "shared_logical";

type CellRow = (
    String,
    Option<String>,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i64,
    i64,
    i64,
);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Cell {
    pub cell_id: String,
    pub region: Option<String>,
    pub domain_class: String,
    pub deployment_profile: String,
    pub capacity_class: String,
    pub metadata_isolation_level: String,
    pub state: String,
    pub metadata_namespace: Option<String>,
    pub object_store_namespace: Option<String>,
    pub rollout_version: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl Cell {
    pub fn default_cell() -> Self {
        Self {
            cell_id: DEFAULT_CELL_ID.into(),
            region: None,
            domain_class: DEFAULT_DOMAIN_CLASS.into(),
            deployment_profile: DEFAULT_DEPLOYMENT_PROFILE.into(),
            capacity_class: DEFAULT_CAPACITY_CLASS.into(),
            metadata_isolation_level: DEFAULT_METADATA_ISOLATION_LEVEL.into(),
            state: "serving".into(),
            metadata_namespace: Some(DEFAULT_CELL_ID.into()),
            object_store_namespace: Some(DEFAULT_CELL_ID.into()),
            rollout_version: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    pool: &AnyPool,
    cell_id: &str,
    region: Option<&str>,
    domain_class: &str,
    deployment_profile: &str,
    capacity_class: &str,
    metadata_isolation_level: &str,
    state: &str,
    metadata_namespace: Option<&str>,
    object_store_namespace: Option<&str>,
    rollout_version: i64,
    now_ms: i64,
) -> MetaResult<()> {
    validate_cell_id(cell_id)?;
    validate_domain_class(domain_class)?;
    validate_deployment_profile(deployment_profile)?;
    validate_capacity_class(capacity_class)?;
    validate_metadata_isolation_level(metadata_isolation_level)?;
    validate_state(state)?;
    if rollout_version < 0 {
        return Err(MetaError::InvalidInput(
            "rollout_version must be non-negative".into(),
        ));
    }
    sqlx::query(
        "INSERT INTO cells
            (cell_id, region, domain_class, deployment_profile, capacity_class,
             metadata_isolation_level, state, metadata_namespace, object_store_namespace,
             rollout_version, created_at_ms, updated_at_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (cell_id) DO UPDATE SET
            region = excluded.region,
            domain_class = excluded.domain_class,
            deployment_profile = excluded.deployment_profile,
            capacity_class = excluded.capacity_class,
            metadata_isolation_level = excluded.metadata_isolation_level,
            state = excluded.state,
            metadata_namespace = excluded.metadata_namespace,
            object_store_namespace = excluded.object_store_namespace,
            rollout_version = excluded.rollout_version,
            updated_at_ms = excluded.updated_at_ms",
    )
    .bind(cell_id)
    .bind(region)
    .bind(domain_class)
    .bind(deployment_profile)
    .bind(capacity_class)
    .bind(metadata_isolation_level)
    .bind(state)
    .bind(metadata_namespace)
    .bind(object_store_namespace)
    .bind(rollout_version)
    .bind(now_ms)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get(pool: &AnyPool, cell_id: &str) -> MetaResult<Option<Cell>> {
    let row: Option<CellRow> = sqlx::query_as(
        "SELECT cell_id, region, domain_class, deployment_profile, capacity_class,
                metadata_isolation_level, state, metadata_namespace,
                object_store_namespace, rollout_version, created_at_ms, updated_at_ms
           FROM cells
          WHERE cell_id = $1",
    )
    .bind(cell_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(into_cell))
}

pub async fn list_all(pool: &AnyPool) -> MetaResult<Vec<Cell>> {
    let rows: Vec<CellRow> = sqlx::query_as(
        "SELECT cell_id, region, domain_class, deployment_profile, capacity_class,
                metadata_isolation_level, state, metadata_namespace,
                object_store_namespace, rollout_version, created_at_ms, updated_at_ms
           FROM cells
          ORDER BY cell_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(into_cell).collect())
}

pub fn validate_cell_id(cell_id: &str) -> MetaResult<()> {
    if cell_id.trim().is_empty() {
        return Err(MetaError::InvalidInput("cell_id must not be empty".into()));
    }
    Ok(())
}

pub fn validate_domain_class(domain_class: &str) -> MetaResult<()> {
    match domain_class {
        "economy" | "standard" | "enterprise" | "regulated" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid domain class `{other}`"
        ))),
    }
}

pub fn validate_deployment_profile(deployment_profile: &str) -> MetaResult<()> {
    match deployment_profile {
        "local" | "single_az_cluster" | "multi_az_standard" | "enterprise" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid deployment profile `{other}`"
        ))),
    }
}

pub fn validate_capacity_class(capacity_class: &str) -> MetaResult<()> {
    match capacity_class {
        "stable" | "mixed" | "preemptible" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid capacity class `{other}`"
        ))),
    }
}

pub fn validate_metadata_isolation_level(level: &str) -> MetaResult<()> {
    match level {
        "shared_logical" | "partitioned" | "dedicated" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid metadata isolation level `{other}`"
        ))),
    }
}

pub fn validate_state(state: &str) -> MetaResult<()> {
    match state {
        "serving" | "draining" | "maintenance" | "disabled" => Ok(()),
        other => Err(MetaError::InvalidInput(format!(
            "invalid cell state `{other}`"
        ))),
    }
}

pub fn tier_allowed_in_domain(tier: &str, domain_class: &str, capacity_class: &str) -> bool {
    match tier {
        "free" => true,
        "developer" => domain_class != "regulated" || capacity_class == "stable",
        "paid" | "enterprise" | "regulated" => {
            domain_class != "economy" && capacity_class != "preemptible"
        }
        _ => false,
    }
}

fn into_cell(
    (
        cell_id,
        region,
        domain_class,
        deployment_profile,
        capacity_class,
        metadata_isolation_level,
        state,
        metadata_namespace,
        object_store_namespace,
        rollout_version,
        created_at_ms,
        updated_at_ms,
    ): CellRow,
) -> Cell {
    Cell {
        cell_id,
        region,
        domain_class,
        deployment_profile,
        capacity_class,
        metadata_isolation_level,
        state,
        metadata_namespace,
        object_store_namespace,
        rollout_version,
        created_at_ms,
        updated_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> AnyPool {
        crate::test_pool_sqlite_memory().await
    }

    #[tokio::test]
    async fn migration_seeds_default_cell() {
        let p = pool().await;
        let cell = get(&p, DEFAULT_CELL_ID).await.unwrap().unwrap();
        assert_eq!(cell.domain_class, DEFAULT_DOMAIN_CLASS);
        assert_eq!(cell.deployment_profile, DEFAULT_DEPLOYMENT_PROFILE);
        assert_eq!(cell.capacity_class, DEFAULT_CAPACITY_CLASS);
        assert_eq!(
            cell.metadata_isolation_level,
            DEFAULT_METADATA_ISOLATION_LEVEL
        );
        assert_eq!(cell.state, "serving");
    }

    #[tokio::test]
    async fn upsert_and_list_cells() {
        let p = pool().await;
        upsert(
            &p,
            "cell-a",
            Some("us-west-2"),
            "economy",
            "single_az_cluster",
            "preemptible",
            "shared_logical",
            "serving",
            Some("meta/cell-a"),
            Some("objects/cell-a"),
            3,
            100,
        )
        .await
        .unwrap();

        let cell = get(&p, "cell-a").await.unwrap().unwrap();
        assert_eq!(cell.region.as_deref(), Some("us-west-2"));
        assert_eq!(cell.domain_class, "economy");
        assert_eq!(cell.capacity_class, "preemptible");
        assert_eq!(cell.rollout_version, 3);
        assert!(list_all(&p)
            .await
            .unwrap()
            .iter()
            .any(|c| c.cell_id == "cell-a"));
    }

    #[test]
    fn paid_tiers_do_not_auto_land_in_economy_or_preemptible_domains() {
        assert!(!tier_allowed_in_domain("paid", "economy", "stable"));
        assert!(!tier_allowed_in_domain(
            "enterprise",
            "standard",
            "preemptible"
        ));
        assert!(tier_allowed_in_domain("paid", "standard", "stable"));
        assert!(tier_allowed_in_domain("free", "economy", "preemptible"));
    }
}
