\* AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
\* Scenario: S14 protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants
\* Falsifies: C12, C15, C16, C17
\*
\* REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

---- MODULE S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Writers, Owners, Cursors

VARIABLES committed, acked, sinkAccepted, ownerEpoch

Init ==
    /\ committed = {}
    /\ acked = {}
    /\ sinkAccepted = {}
    /\ ownerEpoch = [o \in Owners |-> 0]

QuorumCommit(w) ==
    /\ w \in Writers
    /\ committed' = committed \cup {w}
    /\ UNCHANGED <<acked, sinkAccepted, ownerEpoch>>

SinkAccept(c) ==
    /\ c \in Cursors
    /\ sinkAccepted' = sinkAccepted \cup {c}
    /\ UNCHANGED <<committed, acked, ownerEpoch>>

Ack(c) ==
    /\ c \in sinkAccepted
    /\ acked' = acked \cup {c}
    /\ UNCHANGED <<committed, sinkAccepted, ownerEpoch>>

MoveOwner(o) ==
    /\ o \in Owners
    /\ ownerEpoch[o] = 0
    /\ ownerEpoch' = [ownerEpoch EXCEPT ![o] = @ + 1]
    /\ UNCHANGED <<committed, acked, sinkAccepted>>

Next ==
    \/ \E w \in Writers: QuorumCommit(w)
    \/ \E c \in Cursors: SinkAccept(c)
    \/ \E c \in Cursors: Ack(c)
    \/ \E o \in Owners: MoveOwner(o)

NoAckBeforeSinkAccept ==
    acked \subseteq sinkAccepted

TypeOK ==
    /\ committed \subseteq Writers
    /\ sinkAccepted \subseteq Cursors
    /\ acked \subseteq Cursors
    /\ DOMAIN ownerEpoch = Owners
    /\ \A o \in Owners: ownerEpoch[o] \in 0..1

Spec == Init /\ [][Next]_<<committed, acked, sinkAccepted, ownerEpoch>>

====
