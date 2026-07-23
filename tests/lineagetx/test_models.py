from __future__ import annotations

import pytest

from data_lineage_fix_agent.lineagetx.models import (
    AbortCleanupReceipt,
    ChangeIntent,
    CoordinationReceipt,
    CoordinationReceiptKind,
    MigrationStatus,
    Participant,
    ParticipantKind,
    ParticipantStatus,
    RolloutPhase,
    can_transition,
    can_transition_participant,
)


CREATED_AT = "2026-07-17T01:02:03.000000Z"


def _intent(**overrides: object) -> ChangeIntent:
    values: dict[str, object] = {
        "producer_repository": "acme/producer",
        "producer_pr_number": 42,
        "producer_base_sha": "base-0123456789",
        "producer_head_sha": "head-9876543210",
        "source_asset_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)",
        "old_field": "customer_id",
        "new_field": "customer_key",
        "contract_schema_fingerprint": "sha256:contract-schema-v2",
        "created_at": CREATED_AT,
    }
    values.update(overrides)
    return ChangeIntent.create(**values)  # type: ignore[arg-type]


def test_change_intent_identity_and_json_are_stable() -> None:
    first = _intent(producer_pr_url="https://github.example/acme/producer/pull/42")
    second = _intent(
        created_at="2026-07-18T01:02:03.000000Z",
        producer_pr_url="https://mirror.example/pull/42",
    )

    assert first.migration_id == second.migration_id
    assert first.intent_sha256 == second.intent_sha256
    assert first.migration_id == f"ltx-{first.intent_sha256[:24]}"
    assert first.rollout_phase is RolloutPhase.CONTRACT
    assert ChangeIntent.from_json(first.to_json()) == first
    assert first.to_json() == ChangeIntent.from_dict(first.to_dict()).to_json()

    different_contract = _intent(contract_schema_fingerprint="sha256:contract-schema-v3")
    assert different_contract.migration_id != first.migration_id


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"old_field": "customer_key"}, "must differ"),
        ({"producer_head_sha": "base-0123456789"}, "must differ"),
        ({"contract_schema_fingerprint": ""}, "fingerprint"),
        ({"producer_pr_number": 0}, "positive"),
    ],
)
def test_change_intent_rejects_invalid_identity(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _intent(**overrides)


def test_top_level_transition_graph_has_terminal_outcomes() -> None:
    assert can_transition(MigrationStatus.DETECTED, MigrationStatus.PREPARING)
    assert can_transition(MigrationStatus.PREPARING, MigrationStatus.NEEDS_APPROVAL)
    assert can_transition(MigrationStatus.NEEDS_APPROVAL, MigrationStatus.PREPARED)
    assert can_transition(MigrationStatus.PREPARED, MigrationStatus.COMMITTED)
    assert can_transition(MigrationStatus.PREPARED, MigrationStatus.ABORTED)
    assert not can_transition(MigrationStatus.DETECTED, MigrationStatus.PREPARED)
    assert not can_transition(MigrationStatus.COMMITTED, MigrationStatus.ABORTED)
    assert not can_transition(MigrationStatus.ABORTED, MigrationStatus.PREPARING)


def test_participant_has_distinct_state_machine_and_stable_round_trip() -> None:
    intent = _intent()
    participant = Participant.create(
        migration_id=intent.migration_id,
        kind=ParticipantKind.AIRFLOW_MAPPING,
        asset_urn="urn:li:dataset:(urn:li:dataPlatform:airflow,export,PROD)",
        repository="acme/data-platform",
        owner_urns=("urn:li:corpuser:data-owner",),
        files=("dags/export.py", "config/export.json"),
        base_sha="airflow-base-sha",
        created_at=CREATED_AT,
    )

    assert participant.status is ParticipantStatus.DISCOVERED
    assert Participant.from_json(participant.to_json()) == participant
    assert can_transition_participant(
        ParticipantStatus.DISCOVERED, ParticipantStatus.PREPARING
    )
    assert can_transition_participant(
        ParticipantStatus.PREPARING, ParticipantStatus.NEEDS_APPROVAL
    )
    assert can_transition_participant(
        ParticipantStatus.PREPARING, ParticipantStatus.VERIFIED
    )
    assert can_transition_participant(
        ParticipantStatus.FAILED, ParticipantStatus.PREPARING
    )
    assert not can_transition_participant(
        ParticipantStatus.NEEDS_APPROVAL, ParticipantStatus.VERIFIED
    )
    assert not can_transition_participant(
        ParticipantStatus.COMMITTED, ParticipantStatus.PREPARING
    )


def test_abort_receipt_cannot_claim_deployed_rollback() -> None:
    receipt = AbortCleanupReceipt(
        migration_id=_intent().migration_id,
        worktrees_removed=("migration/worktree",),
        candidate_branches_deleted=("lineagetx/migration/repo",),
        recorded_at=CREATED_AT,
    )

    assert receipt.to_dict()["scope"] == "unmerged_candidate_changes_only"
    assert receipt.to_dict()["deployed_systems_rolled_back"] is False
    with pytest.raises(ValueError, match="deployed rollback"):
        AbortCleanupReceipt(
            migration_id=receipt.migration_id,
            deployed_systems_rolled_back=True,
        )
    with pytest.raises(ValueError, match="portable relative"):
        AbortCleanupReceipt(
            migration_id=receipt.migration_id,
            worktrees_removed=("/tmp/private-worktree",),
        )


def test_coordination_receipt_explicitly_does_not_mean_merge() -> None:
    receipt = CoordinationReceipt(
        migration_id=_intent().migration_id,
        kind=CoordinationReceiptKind.COORDINATED_PR,
        reference="https://github.example/acme/data-platform/pull/9",
        recorded_at=CREATED_AT,
    )
    assert receipt.to_dict()["merged"] is False

    with pytest.raises(ValueError, match="does not auto-merge"):
        CoordinationReceipt(
            migration_id=receipt.migration_id,
            kind=CoordinationReceiptKind.COORDINATED_PR,
            reference=receipt.reference,
            recorded_at=CREATED_AT,
            merged=True,
        )


@pytest.mark.parametrize("commit_sha", ["", "abc123", "g" * 40, "a" * 39, "b" * 65])
def test_candidate_commit_receipt_requires_full_hex_sha(commit_sha: str) -> None:
    with pytest.raises(ValueError, match="40- or 64-character hex SHA"):
        CoordinationReceipt(
            migration_id=_intent().migration_id,
            kind=CoordinationReceiptKind.CANDIDATE_COMMIT,
            reference="refs/heads/lineagetx/migration/data-platform",
            commit_sha=commit_sha,
            recorded_at=CREATED_AT,
        )

    valid = CoordinationReceipt(
        migration_id=_intent().migration_id,
        kind=CoordinationReceiptKind.CANDIDATE_COMMIT,
        reference="refs/heads/lineagetx/migration/data-platform",
        commit_sha="a" * 40,
        recorded_at=CREATED_AT,
    )
    assert valid.commit_sha == "a" * 40


def test_coordination_receipt_requires_nonempty_reference() -> None:
    with pytest.raises(ValueError, match="reference is required"):
        CoordinationReceipt(
            migration_id=_intent().migration_id,
            kind=CoordinationReceiptKind.PRODUCER_GATE_RELEASED,
            reference="  ",
            recorded_at=CREATED_AT,
        )


@pytest.mark.parametrize(
    "url",
    (
        "https://user@example.invalid/evidence",
        "https://example.invalid/evidence?token=private",
        "https://example.invalid/evidence#private-fragment",
    ),
)
def test_evidence_urls_reject_userinfo_query_and_fragment(url: str) -> None:
    with pytest.raises(ValueError, match="userinfo, query, or fragment"):
        CoordinationReceipt(
            migration_id=_intent().migration_id,
            kind=CoordinationReceiptKind.COORDINATED_PR,
            reference="https://github.com/acme/data-platform/pull/9",
            recorded_at=CREATED_AT,
            evidence_url=url,
        )


def test_coordination_receipt_round_trips_for_commit_reconciliation() -> None:
    receipt = CoordinationReceipt(
        migration_id=_intent().migration_id,
        kind=CoordinationReceiptKind.COORDINATED_PR,
        reference="https://github.com/acme/data-platform/pull/9",
        recorded_at=CREATED_AT,
        evidence_url="https://github.com/acme/data-platform/pull/9",
    )
    assert CoordinationReceipt.from_dict(receipt.to_dict()) == receipt
