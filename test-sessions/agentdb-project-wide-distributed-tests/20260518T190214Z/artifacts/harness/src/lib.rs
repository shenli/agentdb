#[cfg(test)]
mod authored_core_s1 {
    include!("../../../../../../crates/agentdb-core/tests/auto/S1_contiguous_session_seq_and_append_fetch_under_high_concurrency.rs");
}

#[cfg(test)]
mod authored_core_s2 {
    include!("../../../../../../crates/agentdb-core/tests/auto/S2_idempotent_replay_after_lost_ack_restart_and_handoff.rs");
}

#[cfg(test)]
mod authored_core_s3 {
    include!("../../../../../../crates/agentdb-core/tests/auto/S3_acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries.rs");
}

#[cfg(test)]
mod authored_core_s4 {
    include!("../../../../../../crates/agentdb-core/tests/auto/S4_fork_and_tiered_session_history_remains_ordered_across_lifecycle_faults.rs");
}

#[cfg(test)]
mod authored_core_s6 {
    include!("../../../../../../crates/agentdb-core/tests/auto/S6_record_visibility_and_policy_gc_match_committed_model_under_lag.rs");
}

#[cfg(test)]
mod authored_api_s7 {
    include!("../../../../../../crates/agentdb-api/tests/auto/S7_export_and_watch_cursors_are_gap_free_and_sink_ack_safe.rs");
}

#[cfg(test)]
mod authored_api_s10 {
    include!("../../../../../../crates/agentdb-api/tests/auto/S10_routing_epoch_residency_and_readiness_reject_unsafe_cluster_states.rs");
}
