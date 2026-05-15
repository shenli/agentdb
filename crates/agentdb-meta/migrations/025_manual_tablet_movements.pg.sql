-- M25 — manual tablet movement types.

ALTER TABLE tablet_movements
    DROP CONSTRAINT IF EXISTS tablet_movements_movement_type_check;

ALTER TABLE tablet_movements
    ADD CONSTRAINT tablet_movements_movement_type_check
    CHECK (movement_type IN ('split', 'merge', 'manual_split', 'manual_merge'));

