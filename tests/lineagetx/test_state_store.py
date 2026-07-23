from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from data_lineage_fix_agent.lineagetx.models import (
    AbortCleanupReceipt,
    ApprovalReceipt,
    ChangeIntent,
    CoordinationReceipt,
    CoordinationReceiptKind,
    MigrationStatus,
    Participant,
    ParticipantKind,
    ParticipantStatus,
)
from data_lineage_fix_agent.lineagetx.state import (
    AbortCleanupFailed,
    ApprovalGuardFailed,
    CommitGuardFailed,
    IllegalStateTransition,
    ParentMigrationTerminal,
    PreparationGuardFailed,
    PublicationReconciliationRequired,
    SQLiteStateStore,
    StateConflict,
    VerificationGuardFailed,
)


CREATED_AT = "2026-07-17T01:02:03.000000Z"


def _intent() -> ChangeIntent:
    return ChangeIntent.create(
        producer_repository="acme/producer",
        producer_pr_number=42,
        producer_base_sha="producer-base",
        producer_head_sha="producer-head",
        source_asset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)",
        old_field="customer_id",
        new_field="customer_key",
        contract_schema_fingerprint="sha256:contract-v2",
        created_at=CREATED_AT,
    )


def _participant(
    intent: ChangeIntent,
    kind: ParticipantKind,
    suffix: str,
) -> Participant:
    return Participant.create(
        migration_id=intent.migration_id,
        kind=kind,
        asset_urn=f"urn:li:dataset:(urn:li:dataPlatform:test,{suffix},PROD)",
        repository=f"acme/{suffix}",
        owner_urns=(f"urn:li:corpuser:{suffix}-owner",),
        files=(f"{suffix}/consumer.txt",),
        base_sha=f"{suffix}-base",
        created_at=CREATED_AT,
    )


def _receipts(
    migration_id: str,
    candidate_shas: tuple[str, ...] = ("a" * 40,),
) -> tuple[CoordinationReceipt, ...]:
    candidate_receipts = tuple(
        CoordinationReceipt(
            migration_id=migration_id,
            kind=CoordinationReceiptKind.CANDIDATE_COMMIT,
            reference=f"refs/heads/lineagetx/migration/candidate-{index}",
            commit_sha=sha,
            recorded_at=CREATED_AT,
        )
        for index, sha in enumerate(candidate_shas, start=1)
    )
    return candidate_receipts + (
        CoordinationReceipt(
            migration_id=migration_id,
            kind=CoordinationReceiptKind.COORDINATED_PR,
            reference="https://github.example/acme/coordinated/pull/9",
            recorded_at=CREATED_AT,
        ),
        CoordinationReceipt(
            migration_id=migration_id,
            kind=CoordinationReceiptKind.PRODUCER_GATE_RELEASED,
            reference="check-run:lineagetx/safe-to-contract",
            recorded_at=CREATED_AT,
        ),
    )


def _advance_participant_to_verified(
    store: SQLiteStateStore,
    participant: Participant,
) -> Participant:
    preparing = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.DISCOVERED,
        expected_version=0,
    )
    return store.transition_participant(
        participant.participant_id,
        ParticipantStatus.VERIFIED,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=preparing.version,
        candidate_commit_sha="b" * 40,
        evidence_links=("https://evidence.example/consumer-tests.json",),
    )


def test_state_machine_persists_full_approval_flow_and_recovers(
    tmp_path: Path,
) -> None:
    db = tmp_path / "lineagetx.sqlite3"
    store = SQLiteStateStore(db)
    intent = _intent()
    dbt = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    airflow = _participant(intent, ParticipantKind.AIRFLOW_MAPPING, "airflow")
    semantic = _participant(
        intent, ParticipantKind.SEMANTIC_APPROVAL, "semantic-consumer"
    )
    detected = store.create_migration(intent, (dbt, airflow, semantic))

    assert detected.status is MigrationStatus.DETECTED
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=detected.version,
    )
    _advance_participant_to_verified(store, dbt)
    _advance_participant_to_verified(store, airflow)
    semantic_preparing = store.transition_participant(
        semantic.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.DISCOVERED,
        expected_version=0,
    )
    semantic_waiting = store.transition_participant(
        semantic.participant_id,
        ParticipantStatus.NEEDS_APPROVAL,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=semantic_preparing.version,
    )
    waiting = store.transition_migration(
        intent.migration_id,
        MigrationStatus.NEEDS_APPROVAL,
        expected_status=MigrationStatus.PREPARING,
        expected_version=preparing.version,
    )

    with pytest.raises(PreparationGuardFailed, match="NEEDS_APPROVAL"):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARED,
            expected_status=MigrationStatus.NEEDS_APPROVAL,
            expected_version=waiting.version,
        )

    approval = ApprovalReceipt(
        migration_id=intent.migration_id,
        participant_id=semantic.participant_id,
        owner_urn=semantic.owner_urns[0],
        approved_mapping="customer_id -> customer_key",
        approved_at="2026-07-17T01:03:00.000000Z",
        evidence_url="https://evidence.example/approval.json",
    )
    store.record_approval(approval)
    semantic_retry = store.transition_participant(
        semantic.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.NEEDS_APPROVAL,
        expected_version=semantic_waiting.version,
    )
    store.transition_participant(
        semantic.participant_id,
        ParticipantStatus.VERIFIED,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=semantic_retry.version,
        candidate_commit_sha="c" * 40,
        evidence_links=("https://evidence.example/semantic-tests.json",),
    )
    prepared = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARED,
        expected_status=MigrationStatus.NEEDS_APPROVAL,
        expected_version=waiting.version,
    )
    committed = store.transition_migration(
        intent.migration_id,
        MigrationStatus.COMMITTED,
        expected_status=MigrationStatus.PREPARED,
        expected_version=prepared.version,
        coordination_receipts=_receipts(
            intent.migration_id,
            ("b" * 40, "c" * 40),
        ),
    )

    reopened = SQLiteStateStore(db)
    assert reopened.get_migration(intent.migration_id) == committed
    assert all(
        item.status is ParticipantStatus.VERIFIED
        for item in reopened.list_participants(intent.migration_id)
    )
    assert reopened.list_approvals(intent.migration_id) == [approval]
    events = reopened.list_events(intent.migration_id)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert events[0].event_type == "MIGRATION_DETECTED"
    assert events[-1].to_status is MigrationStatus.COMMITTED
    assert {
        item["kind"]
        for item in events[-1].payload["coordination_receipts"]
    } == {"CANDIDATE_COMMIT", "COORDINATED_PR", "PRODUCER_GATE_RELEASED"}


def test_illegal_transition_and_expected_state_guards_are_atomic(
    tmp_path: Path,
) -> None:
    store = SQLiteStateStore(tmp_path / "state.sqlite3")
    intent = _intent()
    store.create_migration(intent)

    with pytest.raises(IllegalStateTransition, match="cannot move"):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARED,
            expected_status=MigrationStatus.DETECTED,
            expected_version=0,
        )
    assert store.get_migration(intent.migration_id).version == 0
    assert len(store.list_events(intent.migration_id)) == 1

    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=0,
    )
    with pytest.raises(StateConflict, match="not DETECTED@0"):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARING,
            expected_status=MigrationStatus.DETECTED,
            expected_version=0,
        )
    assert store.get_migration(intent.migration_id) == preparing
    assert len(store.list_events(intent.migration_id)) == 2


@pytest.mark.parametrize(
    "blocking_status",
    [ParticipantStatus.NEEDS_APPROVAL, ParticipantStatus.FAILED],
)
def test_prepared_guard_fails_closed_for_unverified_participant(
    tmp_path: Path,
    blocking_status: ParticipantStatus,
) -> None:
    store = SQLiteStateStore(tmp_path / f"{blocking_status.value}.sqlite3")
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "blocked")
    store.create_migration(intent, (participant,))
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=0,
    )
    participant_preparing = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.DISCOVERED,
        expected_version=0,
    )
    store.transition_participant(
        participant.participant_id,
        blocking_status,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=participant_preparing.version,
    )

    with pytest.raises(PreparationGuardFailed, match=blocking_status.value):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARED,
            expected_status=MigrationStatus.PREPARING,
            expected_version=preparing.version,
        )


def test_committed_requires_external_pr_and_gate_receipts(tmp_path: Path) -> None:
    store = SQLiteStateStore(tmp_path / "commit.sqlite3")
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    store.create_migration(intent, (participant,))
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=0,
    )
    _advance_participant_to_verified(store, participant)
    prepared = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARED,
        expected_status=MigrationStatus.PREPARING,
        expected_version=preparing.version,
    )

    with pytest.raises(CommitGuardFailed, match="CANDIDATE_COMMIT"):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.COMMITTED,
            expected_status=MigrationStatus.PREPARED,
            expected_version=prepared.version,
        )
    without_candidate = _receipts(intent.migration_id)[1:]
    with pytest.raises(CommitGuardFailed, match="CANDIDATE_COMMIT"):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.COMMITTED,
            expected_status=MigrationStatus.PREPARED,
            expected_version=prepared.version,
            coordination_receipts=without_candidate,
        )
    assert store.get_migration(intent.migration_id) == prepared


def test_prepared_rejects_zero_participants(tmp_path: Path) -> None:
    store = SQLiteStateStore(tmp_path / "zero-participants.sqlite3")
    intent = _intent()
    store.create_migration(intent)
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=0,
    )

    with pytest.raises(PreparationGuardFailed, match="at least one"):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARED,
            expected_status=MigrationStatus.PREPARING,
            expected_version=preparing.version,
        )
    assert store.get_migration(intent.migration_id) == preparing


def test_owner_approval_is_bound_to_state_owner_and_exact_intent_mapping(
    tmp_path: Path,
) -> None:
    store = SQLiteStateStore(tmp_path / "approval-guard.sqlite3")
    intent = _intent()
    participant = _participant(
        intent, ParticipantKind.SEMANTIC_APPROVAL, "semantic"
    )
    store.create_migration(intent, (participant,))

    def receipt(*, owner: str, mapping: str) -> ApprovalReceipt:
        return ApprovalReceipt(
            migration_id=intent.migration_id,
            participant_id=participant.participant_id,
                owner_urn=owner,
                approved_mapping=mapping,
                approved_at=CREATED_AT,
                evidence_url="https://evidence.example/owner-approval.json",
        )

    correct = receipt(
        owner=participant.owner_urns[0],
        mapping="customer_id -> customer_key",
    )
    with pytest.raises(ApprovalGuardFailed, match="only accepted.*NEEDS_APPROVAL"):
        store.record_approval(correct)

    preparing = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.DISCOVERED,
        expected_version=0,
    )
    store.transition_participant(
        participant.participant_id,
        ParticipantStatus.NEEDS_APPROVAL,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=preparing.version,
    )
    with pytest.raises(ApprovalGuardFailed, match="is not a DataHub owner"):
        store.record_approval(
            receipt(
                owner="urn:li:corpuser:intruder",
                mapping="customer_id -> customer_key",
            )
        )
    with pytest.raises(ApprovalGuardFailed, match="exactly match"):
        store.record_approval(
            receipt(
                owner=participant.owner_urns[0],
                mapping="customer_id->customer_key",
            )
        )
    assert store.list_approvals(intent.migration_id) == []

    store.record_approval(correct)
    assert store.list_approvals(intent.migration_id) == [correct]


def test_two_store_instances_detect_stale_concurrent_version(tmp_path: Path) -> None:
    db = tmp_path / "concurrent.sqlite3"
    first_store = SQLiteStateStore(db)
    second_store = SQLiteStateStore(db)
    intent = _intent()
    stale_first = first_store.create_migration(intent)
    stale_second = second_store.get_migration(intent.migration_id)

    first_store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=stale_first.status,
        expected_version=stale_first.version,
    )
    with pytest.raises(StateConflict, match="DETECTED@0"):
        second_store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARING,
            expected_status=stale_second.status,
            expected_version=stale_second.version,
        )


def test_abort_records_candidate_cleanup_scope_only(tmp_path: Path) -> None:
    store = SQLiteStateStore(tmp_path / "abort.sqlite3")
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    store.create_migration(intent, (participant,))
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=0,
    )
    aborted = store.abort_migration(
        intent.migration_id,
        AbortCleanupReceipt(
            migration_id=intent.migration_id,
            worktrees_removed=("migration/candidate",),
            candidate_branches_deleted=("lineagetx/migration/consumer",),
            recorded_at=CREATED_AT,
        ),
        expected_status=MigrationStatus.PREPARING,
        expected_version=preparing.version,
    )

    assert aborted.status is MigrationStatus.ABORTED
    abort_event = next(
        event
        for event in store.list_events(intent.migration_id)
        if event.event_type == "MIGRATION_ABORTED"
    )
    cleanup = abort_event.payload["cleanup"]
    assert cleanup["scope"] == "unmerged_candidate_changes_only"
    assert cleanup["deployed_systems_rolled_back"] is False
    participant_after = store.get_participant(participant.participant_id)
    assert participant_after.status is ParticipantStatus.ABORTED
    assert participant_after.version == 1
    assert store.list_events(intent.migration_id)[-1].event_type == (
        "PARTICIPANT_ABORTED_WITH_MIGRATION"
    )
    with pytest.raises(IllegalStateTransition):
        store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARING,
            expected_status=MigrationStatus.ABORTED,
            expected_version=aborted.version,
        )


def test_gate_arm_durably_forbids_abort_cleanup_and_state_transition(
    tmp_path: Path,
) -> None:
    db = tmp_path / "publication-ordering.sqlite3"
    store = SQLiteStateStore(db)
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    detected = store.create_migration(intent, (participant,))
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=detected.version,
    )
    _advance_participant_to_verified(store, participant)
    prepared = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARED,
        expected_status=MigrationStatus.PREPARING,
        expected_version=preparing.version,
    )

    store.arm_publication_gate(
        intent.migration_id,
        expected_version=prepared.version,
    )
    # Idempotent retries do not append another arm event.
    store.arm_publication_gate(
        intent.migration_id,
        expected_version=prepared.version,
    )
    reopened = SQLiteStateStore(db)
    assert reopened.publication_gate_armed(intent.migration_id) is True
    assert len(
        [
            event
            for event in reopened.list_events(intent.migration_id)
            if event.event_type == "PRODUCER_GATE_RELEASE_ARMED"
        ]
    ) == 1
    with pytest.raises(PublicationReconciliationRequired, match="cleanup is forbidden"):
        reopened.reserve_abort_cleanup(
            intent.migration_id,
            expected_status=MigrationStatus.PREPARED,
            expected_version=prepared.version,
        )
    with pytest.raises(PublicationReconciliationRequired, match="cannot be recorded"):
        reopened.abort_migration(
            intent.migration_id,
            AbortCleanupReceipt(migration_id=intent.migration_id),
            expected_status=MigrationStatus.PREPARED,
            expected_version=prepared.version,
        )
    assert reopened.get_migration(intent.migration_id).status is MigrationStatus.PREPARED


def test_abort_reservation_wins_ordering_race_before_any_ref_cleanup(
    tmp_path: Path,
) -> None:
    store = SQLiteStateStore(tmp_path / "abort-reservation.sqlite3")
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    detected = store.create_migration(intent, (participant,))
    preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=detected.version,
    )
    _advance_participant_to_verified(store, participant)
    prepared = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARED,
        expected_status=MigrationStatus.PREPARING,
        expected_version=preparing.version,
    )
    store.reserve_abort_cleanup(
        intent.migration_id,
        expected_status=MigrationStatus.PREPARED,
        expected_version=prepared.version,
    )
    assert store.abort_cleanup_reserved(intent.migration_id) is True
    with pytest.raises(PublicationReconciliationRequired, match="cleanup was already reserved"):
        store.arm_publication_gate(
            intent.migration_id,
            expected_version=prepared.version,
        )


def test_verification_persists_candidate_sha_and_evidence(tmp_path: Path) -> None:
    store = SQLiteStateStore(tmp_path / "verification.sqlite3")
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    store.create_migration(intent, (participant,))
    preparing = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.DISCOVERED,
        expected_version=0,
    )

    with pytest.raises(VerificationGuardFailed, match="VERIFIED requires"):
        store.transition_participant(
            participant.participant_id,
            ParticipantStatus.VERIFIED,
            expected_status=ParticipantStatus.PREPARING,
            expected_version=preparing.version,
        )
    with pytest.raises(VerificationGuardFailed, match="40- or 64-character"):
        store.transition_participant(
            participant.participant_id,
            ParticipantStatus.VERIFIED,
            expected_status=ParticipantStatus.PREPARING,
            expected_version=preparing.version,
            candidate_commit_sha="not-a-sha",
        )

    verified = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.VERIFIED,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=preparing.version,
        candidate_commit_sha="d" * 64,
        evidence_links=("https://evidence.example/red-green.json",),
    )
    reopened = SQLiteStateStore(tmp_path / "verification.sqlite3")
    restored = reopened.get_participant(participant.participant_id)
    assert restored == verified
    assert restored.candidate_commit_sha == "d" * 64
    assert restored.evidence_links == ("https://evidence.example/red-green.json",)


def test_abort_rejects_cleanup_errors_without_state_changes(tmp_path: Path) -> None:
    store = SQLiteStateStore(tmp_path / "abort-errors.sqlite3")
    intent = _intent()
    participant = _participant(intent, ParticipantKind.DBT_SQL, "dbt")
    detected = store.create_migration(intent, (participant,))

    with pytest.raises(AbortCleanupFailed, match="cleanup has errors"):
        store.abort_migration(
            intent.migration_id,
            AbortCleanupReceipt(
                migration_id=intent.migration_id,
                cleanup_errors=("failed to remove worktree",),
                recorded_at=CREATED_AT,
            ),
            expected_status=MigrationStatus.DETECTED,
            expected_version=detected.version,
        )
    assert store.get_migration(intent.migration_id) == detected
    assert store.get_participant(participant.participant_id).status is (
        ParticipantStatus.DISCOVERED
    )
    assert len(store.list_events(intent.migration_id)) == 1


@pytest.mark.parametrize(
    "terminal_status",
    [MigrationStatus.COMMITTED, MigrationStatus.ABORTED],
)
def test_terminal_parent_forbids_participant_mutation_and_approval(
    tmp_path: Path,
    terminal_status: MigrationStatus,
) -> None:
    store = SQLiteStateStore(tmp_path / f"terminal-{terminal_status.value}.sqlite3")
    intent = _intent()
    participant = _participant(
        intent, ParticipantKind.SEMANTIC_APPROVAL, "semantic"
    )
    store.create_migration(intent, (participant,))
    migration_preparing = store.transition_migration(
        intent.migration_id,
        MigrationStatus.PREPARING,
        expected_status=MigrationStatus.DETECTED,
        expected_version=0,
    )
    participant_preparing = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.PREPARING,
        expected_status=ParticipantStatus.DISCOVERED,
        expected_version=0,
    )
    participant_waiting = store.transition_participant(
        participant.participant_id,
        ParticipantStatus.NEEDS_APPROVAL,
        expected_status=ParticipantStatus.PREPARING,
        expected_version=participant_preparing.version,
    )
    if terminal_status is MigrationStatus.ABORTED:
        terminal = store.abort_migration(
            intent.migration_id,
            AbortCleanupReceipt(migration_id=intent.migration_id),
            expected_status=MigrationStatus.PREPARING,
            expected_version=migration_preparing.version,
        )
    else:
        retry = store.transition_participant(
            participant.participant_id,
            ParticipantStatus.PREPARING,
            expected_status=ParticipantStatus.NEEDS_APPROVAL,
            expected_version=participant_waiting.version,
        )
        store.transition_participant(
            participant.participant_id,
            ParticipantStatus.VERIFIED,
            expected_status=ParticipantStatus.PREPARING,
            expected_version=retry.version,
            candidate_commit_sha="e" * 40,
        )
        prepared = store.transition_migration(
            intent.migration_id,
            MigrationStatus.PREPARED,
            expected_status=MigrationStatus.PREPARING,
            expected_version=migration_preparing.version,
        )
        terminal = store.transition_migration(
            intent.migration_id,
            MigrationStatus.COMMITTED,
            expected_status=MigrationStatus.PREPARED,
            expected_version=prepared.version,
                coordination_receipts=_receipts(intent.migration_id, ("e" * 40,)),
        )

    current_participant = store.get_participant(participant.participant_id)
    with pytest.raises(ParentMigrationTerminal, match=terminal.status.value):
        store.transition_participant(
            participant.participant_id,
            ParticipantStatus.PREPARING,
            expected_status=current_participant.status,
            expected_version=current_participant.version,
        )
    with pytest.raises(ParentMigrationTerminal, match=terminal.status.value):
        store.record_approval(
            ApprovalReceipt(
                migration_id=intent.migration_id,
                participant_id=participant.participant_id,
                    owner_urn=participant.owner_urns[0],
                    approved_mapping="customer_id -> customer_key",
                    approved_at=CREATED_AT,
                    evidence_url="https://evidence.example/owner-approval.json",
                )
        )


def test_event_rows_are_append_only_at_database_boundary(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite3"
    store = SQLiteStateStore(db)
    intent = _intent()
    store.create_migration(intent)

    connection = sqlite3.connect(db)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE events SET event_type = 'TAMPERED'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM events")
    finally:
        connection.close()

    assert store.list_events(intent.migration_id)[0].event_type == "MIGRATION_DETECTED"
