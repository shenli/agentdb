-- Cooperative handoff: leader writes the target node id here to
-- signal the current owner that a handoff is requested. Owner's
-- TabletReconciler picks up the signal, drains its in-process
-- box, then atomically transfers ownership via complete_handoff
-- (which clears this column and bumps epoch in one statement).
ALTER TABLE tablets ADD COLUMN handoff_target_node_id TEXT;
