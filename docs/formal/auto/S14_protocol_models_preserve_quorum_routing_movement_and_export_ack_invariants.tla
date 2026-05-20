\* AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
\* Scenario: S14 protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants
\* Falsifies: C12, C15, C16, C17
\*
\* REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

---- MODULE S14_protocol_models_preserve_quorum_routing_movement_and_export_ack_invariants ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Writers, Owners, Cursors, Tablets, Quorum, MaxEpoch

VARIABLES
    replicaAccepted,
    committed,
    applied,
    sinkAccepted,
    acked,
    owner,
    ownerEpoch,
    ownerHistory,
    movementInFlight,
    movedKeys,
    staleRetries

Vars ==
    << replicaAccepted,
       committed,
       applied,
       sinkAccepted,
       acked,
       owner,
       ownerEpoch,
       ownerHistory,
       movementInFlight,
       movedKeys,
       staleRetries >>

WriteReplicas(w) ==
    { o \in Owners : <<w, o>> \in replicaAccepted }

InitialOwnerHistory(o) ==
    { <<t, 0, o[t]>> : t \in Tablets }

Init ==
    /\ replicaAccepted = {}
    /\ committed = {}
    /\ applied = {}
    /\ sinkAccepted = {}
    /\ acked = {}
    /\ owner \in [Tablets -> Owners]
    /\ ownerEpoch = [t \in Tablets |-> 0]
    /\ ownerHistory = InitialOwnerHistory(owner)
    /\ movementInFlight = {}
    /\ movedKeys = [t \in Tablets |-> {}]
    /\ staleRetries = {}

AcceptReplica(w, o) ==
    /\ w \in Writers
    /\ o \in Owners
    /\ replicaAccepted' = replicaAccepted \cup {<<w, o>>}
    /\ UNCHANGED <<committed, applied, sinkAccepted, acked, owner, ownerEpoch,
                  ownerHistory, movementInFlight, movedKeys, staleRetries>>

Commit(w) ==
    /\ w \in Writers
    /\ Cardinality(WriteReplicas(w)) >= Quorum
    /\ committed' = committed \cup {w}
    /\ UNCHANGED <<replicaAccepted, applied, sinkAccepted, acked, owner, ownerEpoch,
                  ownerHistory, movementInFlight, movedKeys, staleRetries>>

ApplyAtOwnerEpoch(w, t) ==
    /\ w \in committed
    /\ t \in Tablets
    /\ t \notin movementInFlight
    /\ applied' = applied \cup {<<w, t, ownerEpoch[t]>>}
    /\ UNCHANGED <<replicaAccepted, committed, sinkAccepted, acked, owner, ownerEpoch,
                  ownerHistory, movementInFlight, movedKeys, staleRetries>>

SinkAccept(c) ==
    /\ c \in Cursors
    /\ sinkAccepted' = sinkAccepted \cup {c}
    /\ UNCHANGED <<replicaAccepted, committed, applied, acked, owner, ownerEpoch,
                  ownerHistory, movementInFlight, movedKeys, staleRetries>>

Ack(c) ==
    /\ c \in sinkAccepted
    /\ acked' = acked \cup {c}
    /\ UNCHANGED <<replicaAccepted, committed, applied, sinkAccepted, owner, ownerEpoch,
                  ownerHistory, movementInFlight, movedKeys, staleRetries>>

StartMovement(t) ==
    /\ t \in Tablets
    /\ t \notin movementInFlight
    /\ movementInFlight' = movementInFlight \cup {t}
    /\ UNCHANGED <<replicaAccepted, committed, applied, sinkAccepted, acked, owner,
                  ownerEpoch, ownerHistory, movedKeys, staleRetries>>

FinishMovement(t, newOwner) ==
    /\ t \in movementInFlight
    /\ newOwner \in Owners
    /\ ownerEpoch[t] < MaxEpoch
    /\ owner' = [owner EXCEPT ![t] = newOwner]
    /\ ownerEpoch' = [ownerEpoch EXCEPT ![t] = @ + 1]
    /\ ownerHistory' = ownerHistory \cup {<<t, ownerEpoch[t] + 1, newOwner>>}
    /\ movementInFlight' = movementInFlight \ {t}
    /\ movedKeys' = [movedKeys EXCEPT ![t] = committed]
    /\ UNCHANGED <<replicaAccepted, committed, applied, sinkAccepted, acked, staleRetries>>

RetryWithStaleEpoch(w, t, e) ==
    /\ w \in Writers
    /\ t \in Tablets
    /\ e \in 0..MaxEpoch
    /\ e < ownerEpoch[t]
    /\ staleRetries' = staleRetries \cup {<<w, t, e>>}
    /\ UNCHANGED <<replicaAccepted, committed, applied, sinkAccepted, acked, owner,
                  ownerEpoch, ownerHistory, movementInFlight, movedKeys>>

Next ==
    \/ \E w \in Writers, o \in Owners: AcceptReplica(w, o)
    \/ \E w \in Writers: Commit(w)
    \/ \E w \in Writers, t \in Tablets: ApplyAtOwnerEpoch(w, t)
    \/ \E c \in Cursors: SinkAccept(c)
    \/ \E c \in Cursors: Ack(c)
    \/ \E t \in Tablets: StartMovement(t)
    \/ \E t \in Tablets, o \in Owners: FinishMovement(t, o)
    \/ \E w \in Writers, t \in Tablets, e \in 0..MaxEpoch: RetryWithStaleEpoch(w, t, e)

TypeOK ==
    /\ replicaAccepted \subseteq (Writers \X Owners)
    /\ committed \subseteq Writers
    /\ applied \subseteq (Writers \X Tablets \X (0..MaxEpoch))
    /\ sinkAccepted \subseteq Cursors
    /\ acked \subseteq Cursors
    /\ owner \in [Tablets -> Owners]
    /\ ownerEpoch \in [Tablets -> 0..MaxEpoch]
    /\ ownerHistory \subseteq (Tablets \X (0..MaxEpoch) \X Owners)
    /\ movementInFlight \subseteq Tablets
    /\ movedKeys \in [Tablets -> SUBSET Writers]
    /\ staleRetries \subseteq (Writers \X Tablets \X (0..MaxEpoch))

NoAckBeforeSinkAccept ==
    acked \subseteq sinkAccepted

NoCommittedWriteWithoutQuorum ==
    \A w \in committed : Cardinality(WriteReplicas(w)) >= Quorum

NoApplyBeforeCommit ==
    \A rec \in applied : rec[1] \in committed

NoAppliedEpochExceedsCurrentEpoch ==
    \A rec \in applied : rec[3] <= ownerEpoch[rec[2]]

NoLostCommittedKeyDuringMovement ==
    \A t \in Tablets : movedKeys[t] \subseteq committed

NoTwoActiveOwnersForSameTabletEpoch ==
    \A t \in Tablets :
    \A e \in 0..MaxEpoch :
        Cardinality({o \in Owners : <<t, e, o>> \in ownerHistory}) <= 1

StaleRetriesStayBelowCurrentEpoch ==
    \A retry \in staleRetries : retry[3] < ownerEpoch[retry[2]]

Spec ==
    Init /\ [][Next]_Vars

====
