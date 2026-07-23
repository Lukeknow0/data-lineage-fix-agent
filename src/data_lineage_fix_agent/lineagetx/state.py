from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .models import (
    AbortCleanupReceipt,
    ApprovalReceipt,
    ChangeIntent,
    CoordinationReceipt,
    CoordinationReceiptKind,
    MigrationRecord,
    MigrationStatus,
    Participant,
    ParticipantKind,
    ParticipantStatus,
    StateEvent,
    can_transition,
    can_transition_participant,
    canonical_json,
    is_commit_sha,
    utc_now,
)


class StateStoreError(RuntimeError):
    """Base class for recoverable LineageTX state-store errors."""


class MigrationNotFound(StateStoreError):
    pass


class MigrationAlreadyExists(StateStoreError):
    pass


class ParticipantNotFound(StateStoreError):
    pass


class StateConflict(StateStoreError):
    """The caller acted on a stale status or version."""


class IllegalStateTransition(StateStoreError):
    pass


class PreparationGuardFailed(StateStoreError):
    pass


class CommitGuardFailed(StateStoreError):
    pass


class ApprovalGuardFailed(StateStoreError):
    pass


class VerificationGuardFailed(StateStoreError):
    pass


class ParentMigrationTerminal(StateStoreError):
    pass


class AbortCleanupFailed(StateStoreError):
    pass


class PublicationReconciliationRequired(StateStoreError):
    """A Producer gate may be visible, so candidate cleanup is forbidden."""


_GATE_ARMED_EVENT = "PRODUCER_GATE_RELEASE_ARMED"
_ABORT_RESERVED_EVENT = "ABORT_CLEANUP_RESERVED"


def _status(value: MigrationStatus | str) -> MigrationStatus:
    if isinstance(value, MigrationStatus):
        return value
    return MigrationStatus(value)


def _participant_status(value: ParticipantStatus | str) -> ParticipantStatus:
    if isinstance(value, ParticipantStatus):
        return value
    return ParticipantStatus(value)


class SQLiteStateStore:
    """Durable, optimistic-locking state for one LineageTX coordinator.

    All mutations acquire a SQLite RESERVED lock with ``BEGIN IMMEDIATE`` and
    still require the caller's expected status and version. Events are written
    in the same transaction as their corresponding state mutation.
    """

    def __init__(self, path: str | Path, *, timeout: float = 5.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.timeout,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS migrations (
                    migration_id TEXT PRIMARY KEY,
                    intent_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK (version >= 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS participants (
                    participant_id TEXT PRIMARY KEY,
                    migration_id TEXT NOT NULL,
                    participant_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK (version >= 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (migration_id)
                        REFERENCES migrations(migration_id) ON DELETE RESTRICT
                );
                CREATE INDEX IF NOT EXISTS participants_by_migration
                    ON participants(migration_id, participant_id);

                CREATE TABLE IF NOT EXISTS approvals (
                    migration_id TEXT NOT NULL,
                    participant_id TEXT NOT NULL,
                    owner_urn TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    approved_at TEXT NOT NULL,
                    PRIMARY KEY (migration_id, participant_id, owner_urn),
                    FOREIGN KEY (migration_id)
                        REFERENCES migrations(migration_id) ON DELETE RESTRICT,
                    FOREIGN KEY (participant_id)
                        REFERENCES participants(participant_id) ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    migration_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    participant_id TEXT,
                    event_type TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK (version >= 0),
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (migration_id, sequence),
                    FOREIGN KEY (migration_id)
                        REFERENCES migrations(migration_id) ON DELETE RESTRICT
                );
                CREATE INDEX IF NOT EXISTS events_by_migration
                    ON events(migration_id, sequence);

                CREATE TRIGGER IF NOT EXISTS events_are_append_only_update
                BEFORE UPDATE ON events
                BEGIN
                    SELECT RAISE(ABORT, 'LineageTX events are append-only');
                END;

                CREATE TRIGGER IF NOT EXISTS events_are_append_only_delete
                BEFORE DELETE ON events
                BEGIN
                    SELECT RAISE(ABORT, 'LineageTX events are append-only');
                END;
                """
            )
        finally:
            connection.close()

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _append_event(
        self,
        connection: sqlite3.Connection,
        *,
        migration_id: str,
        participant_id: str = "",
        event_type: str,
        from_status: MigrationStatus | ParticipantStatus | None,
        to_status: MigrationStatus | ParticipantStatus,
        version: int,
        payload: Mapping[str, Any] | None,
        created_at: str,
    ) -> None:
        sequence = int(
            connection.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM events
                WHERE migration_id = ?
                """,
                (migration_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO events (
                migration_id, sequence, participant_id, event_type,
                from_status, to_status, version, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                migration_id,
                sequence,
                participant_id or None,
                event_type,
                from_status.value if from_status else None,
                to_status.value,
                version,
                canonical_json(payload or {}),
                created_at,
            ),
        )

    def create_migration(
        self,
        intent: ChangeIntent,
        participants: Sequence[Participant] = (),
    ) -> MigrationRecord:
        if any(item.migration_id != intent.migration_id for item in participants):
            raise ValueError("every participant must belong to the migration")
        participant_ids = [item.participant_id for item in participants]
        if len(participant_ids) != len(set(participant_ids)):
            raise ValueError("participant IDs must be unique")

        with self._write_transaction() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO migrations (
                        migration_id, intent_json, status, version,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, 0, ?, ?)
                    """,
                    (
                        intent.migration_id,
                        intent.to_json(),
                        MigrationStatus.DETECTED.value,
                        intent.created_at,
                        intent.created_at,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise MigrationAlreadyExists(intent.migration_id) from error

            for participant in participants:
                initial = Participant.from_dict(
                    {
                        **participant.to_dict(),
                        "status": ParticipantStatus.DISCOVERED.value,
                        "version": 0,
                    }
                )
                connection.execute(
                    """
                    INSERT INTO participants (
                        participant_id, migration_id, participant_json,
                        status, version, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        initial.participant_id,
                        initial.migration_id,
                        initial.to_json(),
                        initial.status.value,
                        initial.created_at,
                        initial.updated_at,
                    ),
                )

            self._append_event(
                connection,
                migration_id=intent.migration_id,
                event_type="MIGRATION_DETECTED",
                from_status=None,
                to_status=MigrationStatus.DETECTED,
                version=0,
                payload={"participant_ids": participant_ids},
                created_at=intent.created_at,
            )

        return MigrationRecord(
            intent=intent,
            status=MigrationStatus.DETECTED,
            version=0,
            created_at=intent.created_at,
            updated_at=intent.created_at,
        )

    @staticmethod
    def _migration_from_row(row: sqlite3.Row) -> MigrationRecord:
        return MigrationRecord(
            intent=ChangeIntent.from_json(str(row["intent_json"])),
            status=MigrationStatus(str(row["status"])),
            version=int(row["version"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def get_migration(self, migration_id: str) -> MigrationRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM migrations WHERE migration_id = ?",
                (migration_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise MigrationNotFound(migration_id)
        return self._migration_from_row(row)

    def _has_migration_event(self, migration_id: str, event_type: str) -> bool:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT 1 FROM events
                WHERE migration_id = ? AND event_type = ?
                LIMIT 1
                """,
                (migration_id, event_type),
            ).fetchone()
        finally:
            connection.close()
        return row is not None

    def publication_gate_armed(self, migration_id: str) -> bool:
        self.get_migration(migration_id)
        return self._has_migration_event(migration_id, _GATE_ARMED_EVENT)

    def abort_cleanup_reserved(self, migration_id: str) -> bool:
        self.get_migration(migration_id)
        return self._has_migration_event(migration_id, _ABORT_RESERVED_EVENT)

    def arm_publication_gate(
        self,
        migration_id: str,
        *,
        expected_version: int,
    ) -> None:
        """Durably forbid ABORT before the first external gate observation."""

        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT status, version FROM migrations WHERE migration_id = ?",
                (migration_id,),
            ).fetchone()
            if row is None:
                raise MigrationNotFound(migration_id)
            current = MigrationStatus(str(row["status"]))
            version = int(row["version"])
            if current is not MigrationStatus.PREPARED or version != expected_version:
                raise StateConflict(
                    f"migration {migration_id} is {current.value}@{version}, "
                    f"not PREPARED@{expected_version}"
                )
            abort_reserved = connection.execute(
                """
                SELECT 1 FROM events
                WHERE migration_id = ? AND event_type = ?
                LIMIT 1
                """,
                (migration_id, _ABORT_RESERVED_EVENT),
            ).fetchone()
            if abort_reserved is not None:
                raise PublicationReconciliationRequired(
                    "candidate cleanup was already reserved; Producer gate release "
                    "cannot start"
                )
            already_armed = connection.execute(
                """
                SELECT 1 FROM events
                WHERE migration_id = ? AND event_type = ?
                LIMIT 1
                """,
                (migration_id, _GATE_ARMED_EVENT),
            ).fetchone()
            if already_armed is not None:
                return
            self._append_event(
                connection,
                migration_id=migration_id,
                event_type=_GATE_ARMED_EVENT,
                from_status=current,
                to_status=current,
                version=version,
                payload={
                    "candidate_cleanup_permitted": False,
                    "recovery": "reconcile_publication",
                },
                created_at=utc_now(),
            )

    def reserve_abort_cleanup(
        self,
        migration_id: str,
        *,
        expected_status: MigrationStatus | str,
        expected_version: int,
    ) -> None:
        """Win the durable abort-vs-gate ordering race before deleting refs."""

        expected = _status(expected_status)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT status, version FROM migrations WHERE migration_id = ?",
                (migration_id,),
            ).fetchone()
            if row is None:
                raise MigrationNotFound(migration_id)
            current = MigrationStatus(str(row["status"]))
            version = int(row["version"])
            if current is not expected or version != expected_version:
                raise StateConflict(
                    f"migration {migration_id} is {current.value}@{version}, "
                    f"not {expected.value}@{expected_version}"
                )
            gate_armed = connection.execute(
                """
                SELECT 1 FROM events
                WHERE migration_id = ? AND event_type = ?
                LIMIT 1
                """,
                (migration_id, _GATE_ARMED_EVENT),
            ).fetchone()
            if gate_armed is not None:
                raise PublicationReconciliationRequired(
                    "Producer gate release was armed; ABORT cleanup is forbidden and "
                    "publication must be reconciled"
                )
            already_reserved = connection.execute(
                """
                SELECT 1 FROM events
                WHERE migration_id = ? AND event_type = ?
                LIMIT 1
                """,
                (migration_id, _ABORT_RESERVED_EVENT),
            ).fetchone()
            if already_reserved is not None:
                return
            self._append_event(
                connection,
                migration_id=migration_id,
                event_type=_ABORT_RESERVED_EVENT,
                from_status=current,
                to_status=current,
                version=version,
                payload={"scope": "unmerged_candidate_changes_only"},
                created_at=utc_now(),
            )

    def list_migrations(self) -> list[MigrationRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM migrations ORDER BY created_at, migration_id"
            ).fetchall()
        finally:
            connection.close()
        return [self._migration_from_row(row) for row in rows]

    def transition_migration(
        self,
        migration_id: str,
        target_status: MigrationStatus | str,
        *,
        expected_status: MigrationStatus | str,
        expected_version: int,
        event_type: str = "MIGRATION_STATE_CHANGED",
        payload: Mapping[str, Any] | None = None,
        coordination_receipts: Sequence[CoordinationReceipt] = (),
    ) -> MigrationRecord:
        target = _status(target_status)
        expected = _status(expected_status)
        if target is MigrationStatus.ABORTED:
            raise IllegalStateTransition(
                "ABORTED requires abort_migration with a cleanup receipt"
            )
        if any(item.migration_id != migration_id for item in coordination_receipts):
            raise ValueError("coordination receipt belongs to a different migration")
        event_payload = dict(payload or {})
        if coordination_receipts:
            event_payload["coordination_receipts"] = [
                item.to_dict() for item in coordination_receipts
            ]
        if target is MigrationStatus.COMMITTED:
            receipt_kinds = {item.kind for item in coordination_receipts}
            required = {
                CoordinationReceiptKind.CANDIDATE_COMMIT,
                CoordinationReceiptKind.COORDINATED_PR,
                CoordinationReceiptKind.PRODUCER_GATE_RELEASED,
            }
            missing = sorted(item.value for item in required - receipt_kinds)
            if missing:
                raise CommitGuardFailed(
                    "COMMITTED requires external coordination receipts: "
                    + ", ".join(missing)
                )
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM migrations WHERE migration_id = ?",
                (migration_id,),
            ).fetchone()
            if row is None:
                raise MigrationNotFound(migration_id)

            current = MigrationStatus(str(row["status"]))
            version = int(row["version"])
            if current is not expected or version != expected_version:
                raise StateConflict(
                    f"migration {migration_id} is {current.value}@{version}, "
                    f"not {expected.value}@{expected_version}"
                )
            if not can_transition(current, target):
                raise IllegalStateTransition(
                    f"migration {migration_id} cannot move from "
                    f"{current.value} to {target.value}"
                )

            if target is MigrationStatus.PREPARED:
                participant_rows = connection.execute(
                    """
                    SELECT participant_id, status, participant_json FROM participants
                    WHERE migration_id = ?
                    ORDER BY participant_id
                    """,
                    (migration_id,),
                ).fetchall()
                if not participant_rows:
                    raise PreparationGuardFailed(
                        "PREPARED requires at least one discovered participant"
                    )
                not_verified: list[str] = []
                for item in participant_rows:
                    participant_value = json.loads(str(item["participant_json"]))
                    candidate_sha = str(
                        participant_value.get("candidate_commit_sha", "")
                    )
                    if item["status"] != ParticipantStatus.VERIFIED.value:
                        not_verified.append(
                            f"{item['participant_id']}={item['status']}"
                        )
                    elif not is_commit_sha(candidate_sha):
                        not_verified.append(
                            f"{item['participant_id']}=VERIFIED_WITHOUT_CANDIDATE_COMMIT"
                        )
                if not_verified:
                    raise PreparationGuardFailed(
                        "PREPARED requires every participant VERIFIED; unresolved: "
                        + ", ".join(not_verified)
                    )
            elif target is MigrationStatus.COMMITTED:
                participant_rows = connection.execute(
                    """
                    SELECT participant_id, status, participant_json FROM participants
                    WHERE migration_id = ?
                    ORDER BY participant_id
                    """,
                    (migration_id,),
                ).fetchall()
                not_ready = [
                    f"{item['participant_id']}={item['status']}"
                    for item in participant_rows
                    if item["status"]
                    not in {
                        ParticipantStatus.VERIFIED.value,
                        ParticipantStatus.COMMITTED.value,
                    }
                ]
                if not_ready:
                    raise CommitGuardFailed(
                        "COMMITTED requires every participant still verified; "
                        "unresolved: " + ", ".join(not_ready)
                    )
                expected_candidate_shas = {
                    str(json.loads(str(item["participant_json"]))["candidate_commit_sha"])
                    for item in participant_rows
                }
                supplied_candidate_shas = {
                    item.commit_sha
                    for item in coordination_receipts
                    if item.kind is CoordinationReceiptKind.CANDIDATE_COMMIT
                }
                missing_candidate_shas = sorted(
                    expected_candidate_shas - supplied_candidate_shas
                )
                if missing_candidate_shas:
                    raise CommitGuardFailed(
                        "COMMITTED requires a candidate receipt for every verified "
                        "participant commit: " + ", ".join(missing_candidate_shas)
                    )

            updated_at = utc_now()
            next_version = version + 1
            cursor = connection.execute(
                """
                UPDATE migrations
                SET status = ?, version = ?, updated_at = ?
                WHERE migration_id = ? AND status = ? AND version = ?
                """,
                (
                    target.value,
                    next_version,
                    updated_at,
                    migration_id,
                    current.value,
                    version,
                ),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"migration {migration_id} changed concurrently")

            self._append_event(
                connection,
                migration_id=migration_id,
                event_type=event_type,
                from_status=current,
                to_status=target,
                version=next_version,
                payload=event_payload,
                created_at=updated_at,
            )
            record = MigrationRecord(
                intent=ChangeIntent.from_json(str(row["intent_json"])),
                status=target,
                version=next_version,
                created_at=str(row["created_at"]),
                updated_at=updated_at,
            )
        return record

    def abort_migration(
        self,
        migration_id: str,
        receipt: AbortCleanupReceipt,
        *,
        expected_status: MigrationStatus | str,
        expected_version: int,
    ) -> MigrationRecord:
        if receipt.migration_id != migration_id:
            raise ValueError("cleanup receipt belongs to a different migration")
        if receipt.cleanup_errors:
            raise AbortCleanupFailed(
                "ABORTED cannot be recorded while candidate cleanup has errors: "
                + "; ".join(receipt.cleanup_errors)
            )
        expected = _status(expected_status)
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM migrations WHERE migration_id = ?",
                (migration_id,),
            ).fetchone()
            if row is None:
                raise MigrationNotFound(migration_id)
            current = MigrationStatus(str(row["status"]))
            version = int(row["version"])
            if current is not expected or version != expected_version:
                raise StateConflict(
                    f"migration {migration_id} is {current.value}@{version}, "
                    f"not {expected.value}@{expected_version}"
                )
            gate_armed = connection.execute(
                """
                SELECT 1 FROM events
                WHERE migration_id = ? AND event_type = ?
                LIMIT 1
                """,
                (migration_id, _GATE_ARMED_EVENT),
            ).fetchone()
            if gate_armed is not None:
                raise PublicationReconciliationRequired(
                    "Producer gate release was armed; ABORTED cannot be recorded"
                )
            if not can_transition(current, MigrationStatus.ABORTED):
                raise IllegalStateTransition(
                    f"migration {migration_id} cannot move from "
                    f"{current.value} to ABORTED"
                )

            updated_at = utc_now()
            next_version = version + 1
            cursor = connection.execute(
                """
                UPDATE migrations
                SET status = ?, version = ?, updated_at = ?
                WHERE migration_id = ? AND status = ? AND version = ?
                """,
                (
                    MigrationStatus.ABORTED.value,
                    next_version,
                    updated_at,
                    migration_id,
                    current.value,
                    version,
                ),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"migration {migration_id} changed concurrently")
            self._append_event(
                connection,
                migration_id=migration_id,
                event_type="MIGRATION_ABORTED",
                from_status=current,
                to_status=MigrationStatus.ABORTED,
                version=next_version,
                payload={"cleanup": receipt.to_dict()},
                created_at=updated_at,
            )

            participant_rows = connection.execute(
                """
                SELECT * FROM participants
                WHERE migration_id = ?
                ORDER BY participant_id
                """,
                (migration_id,),
            ).fetchall()
            for participant_row in participant_rows:
                participant_status = ParticipantStatus(str(participant_row["status"]))
                if participant_status in {
                    ParticipantStatus.COMMITTED,
                    ParticipantStatus.ABORTED,
                }:
                    continue
                participant_version = int(participant_row["version"])
                participant_value = json.loads(
                    str(participant_row["participant_json"])
                )
                participant_value.update(
                    {
                        "status": ParticipantStatus.ABORTED.value,
                        "updated_at": updated_at,
                        "version": participant_version + 1,
                    }
                )
                participant_cursor = connection.execute(
                    """
                    UPDATE participants
                    SET participant_json = ?, status = ?, version = ?, updated_at = ?
                    WHERE participant_id = ? AND status = ? AND version = ?
                    """,
                    (
                        canonical_json(participant_value),
                        ParticipantStatus.ABORTED.value,
                        participant_version + 1,
                        updated_at,
                        str(participant_row["participant_id"]),
                        participant_status.value,
                        participant_version,
                    ),
                )
                if participant_cursor.rowcount != 1:
                    raise StateConflict(
                        f"participant {participant_row['participant_id']} "
                        "changed concurrently"
                    )
                self._append_event(
                    connection,
                    migration_id=migration_id,
                    participant_id=str(participant_row["participant_id"]),
                    event_type="PARTICIPANT_ABORTED_WITH_MIGRATION",
                    from_status=participant_status,
                    to_status=ParticipantStatus.ABORTED,
                    version=participant_version + 1,
                    payload={"cleanup_scope": "unmerged_candidate_changes_only"},
                    created_at=updated_at,
                )

            record = MigrationRecord(
                intent=ChangeIntent.from_json(str(row["intent_json"])),
                status=MigrationStatus.ABORTED,
                version=next_version,
                created_at=str(row["created_at"]),
                updated_at=updated_at,
            )
        return record

    @staticmethod
    def _participant_from_row(row: sqlite3.Row) -> Participant:
        value = json.loads(str(row["participant_json"]))
        value.update(
            {
                "status": str(row["status"]),
                "version": int(row["version"]),
                "updated_at": str(row["updated_at"]),
            }
        )
        return Participant.from_dict(value)

    def get_participant(self, participant_id: str) -> Participant:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM participants WHERE participant_id = ?",
                (participant_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise ParticipantNotFound(participant_id)
        return self._participant_from_row(row)

    def list_participants(self, migration_id: str) -> list[Participant]:
        # Distinguish an unknown migration from one with no consumers.
        self.get_migration(migration_id)
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM participants
                WHERE migration_id = ?
                ORDER BY participant_id
                """,
                (migration_id,),
            ).fetchall()
        finally:
            connection.close()
        return [self._participant_from_row(row) for row in rows]

    def transition_participant(
        self,
        participant_id: str,
        target_status: ParticipantStatus | str,
        *,
        expected_status: ParticipantStatus | str,
        expected_version: int,
        event_type: str = "PARTICIPANT_STATE_CHANGED",
        payload: Mapping[str, Any] | None = None,
        candidate_commit_sha: str | None = None,
        evidence_links: Sequence[str] | None = None,
    ) -> Participant:
        target = _participant_status(target_status)
        expected = _participant_status(expected_status)
        with self._write_transaction() as connection:
            row = connection.execute(
                """
                SELECT p.*, m.status AS migration_status
                FROM participants AS p
                JOIN migrations AS m ON m.migration_id = p.migration_id
                WHERE p.participant_id = ?
                """,
                (participant_id,),
            ).fetchone()
            if row is None:
                raise ParticipantNotFound(participant_id)
            migration_status = MigrationStatus(str(row["migration_status"]))
            if migration_status in {
                MigrationStatus.COMMITTED,
                MigrationStatus.ABORTED,
            }:
                raise ParentMigrationTerminal(
                    f"participant mutation is forbidden after parent migration "
                    f"{migration_status.value}"
                )

            current = ParticipantStatus(str(row["status"]))
            version = int(row["version"])
            if current is not expected or version != expected_version:
                raise StateConflict(
                    f"participant {participant_id} is {current.value}@{version}, "
                    f"not {expected.value}@{expected_version}"
                )
            if not can_transition_participant(current, target):
                raise IllegalStateTransition(
                    f"participant {participant_id} cannot move from "
                    f"{current.value} to {target.value}"
                )

            updated_at = utc_now()
            next_version = version + 1
            value = json.loads(str(row["participant_json"]))
            if candidate_commit_sha is not None:
                if candidate_commit_sha and not is_commit_sha(candidate_commit_sha):
                    raise VerificationGuardFailed(
                        "candidate_commit_sha must be a 40- or 64-character hex SHA"
                    )
                value["candidate_commit_sha"] = candidate_commit_sha
            if evidence_links is not None:
                value["evidence_links"] = [str(item) for item in evidence_links]
            if target is ParticipantStatus.VERIFIED and not is_commit_sha(
                str(value.get("candidate_commit_sha", ""))
            ):
                raise VerificationGuardFailed(
                    "VERIFIED requires a persisted 40- or 64-character candidate "
                    "commit SHA"
                )
            value.update(
                {
                    "status": target.value,
                    "updated_at": updated_at,
                    "version": next_version,
                }
            )
            cursor = connection.execute(
                """
                UPDATE participants
                SET participant_json = ?, status = ?, version = ?, updated_at = ?
                WHERE participant_id = ? AND status = ? AND version = ?
                """,
                (
                    canonical_json(value),
                    target.value,
                    next_version,
                    updated_at,
                    participant_id,
                    current.value,
                    version,
                ),
            )
            if cursor.rowcount != 1:
                raise StateConflict(f"participant {participant_id} changed concurrently")

            self._append_event(
                connection,
                migration_id=str(row["migration_id"]),
                participant_id=participant_id,
                event_type=event_type,
                from_status=current,
                to_status=target,
                version=next_version,
                payload=payload,
                created_at=updated_at,
            )
            participant = Participant.from_dict(value)
        return participant

    def record_approval(self, receipt: ApprovalReceipt) -> None:
        """Persist an owner approval receipt without changing state implicitly."""

        with self._write_transaction() as connection:
            participant = connection.execute(
                """
                SELECT p.*, m.intent_json AS migration_intent_json,
                       m.status AS migration_status
                FROM participants AS p
                JOIN migrations AS m ON m.migration_id = p.migration_id
                WHERE p.participant_id = ? AND p.migration_id = ?
                """,
                (receipt.participant_id, receipt.migration_id),
            ).fetchone()
            if participant is None:
                raise ParticipantNotFound(receipt.participant_id)
            migration_status = MigrationStatus(str(participant["migration_status"]))
            if migration_status in {
                MigrationStatus.COMMITTED,
                MigrationStatus.ABORTED,
            }:
                raise ParentMigrationTerminal(
                    f"approval is forbidden after parent migration "
                    f"{migration_status.value}"
                )
            current_status = ParticipantStatus(str(participant["status"]))
            if current_status is not ParticipantStatus.NEEDS_APPROVAL:
                raise ApprovalGuardFailed(
                    "owner approval is only accepted while the participant is "
                    f"NEEDS_APPROVAL, not {current_status.value}"
                )
            participant_model = self._participant_from_row(participant)
            if participant_model.kind is not ParticipantKind.SEMANTIC_APPROVAL:
                raise ApprovalGuardFailed(
                    "owner approval is only valid for a semantic-approval participant"
                )
            if receipt.owner_urn not in participant_model.owner_urns:
                raise ApprovalGuardFailed(
                    f"{receipt.owner_urn} is not a DataHub owner of "
                    f"{receipt.participant_id}"
                )
            intent = ChangeIntent.from_json(str(participant["migration_intent_json"]))
            expected_mapping = f"{intent.old_field} -> {intent.new_field}"
            if receipt.approved_mapping != expected_mapping:
                raise ApprovalGuardFailed(
                    "approved mapping must exactly match the ChangeIntent: "
                    f"{expected_mapping}"
                )
            try:
                connection.execute(
                    """
                    INSERT INTO approvals (
                        migration_id, participant_id, owner_urn,
                        receipt_json, approved_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        receipt.migration_id,
                        receipt.participant_id,
                        receipt.owner_urn,
                        canonical_json(receipt.to_dict()),
                        receipt.approved_at,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise StateConflict("this owner approval is already recorded") from error

            self._append_event(
                connection,
                migration_id=receipt.migration_id,
                participant_id=receipt.participant_id,
                event_type="OWNER_APPROVAL_RECORDED",
                from_status=current_status,
                to_status=current_status,
                version=int(participant["version"]),
                payload={"approval": receipt.to_dict()},
                created_at=receipt.approved_at,
            )

    def list_approvals(self, migration_id: str) -> list[ApprovalReceipt]:
        self.get_migration(migration_id)
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT receipt_json FROM approvals
                WHERE migration_id = ?
                ORDER BY approved_at, participant_id, owner_urn
                """,
                (migration_id,),
            ).fetchall()
        finally:
            connection.close()
        return [ApprovalReceipt(**json.loads(str(row["receipt_json"]))) for row in rows]

    def list_events(self, migration_id: str) -> list[StateEvent]:
        self.get_migration(migration_id)
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM events
                WHERE migration_id = ?
                ORDER BY sequence
                """,
                (migration_id,),
            ).fetchall()
        finally:
            connection.close()
        return [
            StateEvent(
                migration_id=str(row["migration_id"]),
                sequence=int(row["sequence"]),
                participant_id=str(row["participant_id"] or ""),
                event_type=str(row["event_type"]),
                from_status=(
                    (
                        ParticipantStatus(str(row["from_status"]))
                        if row["participant_id"] is not None
                        else MigrationStatus(str(row["from_status"]))
                    )
                    if row["from_status"] is not None
                    else None
                ),
                to_status=(
                    ParticipantStatus(str(row["to_status"]))
                    if row["participant_id"] is not None
                    else MigrationStatus(str(row["to_status"]))
                ),
                version=int(row["version"]),
                payload=json.loads(str(row["payload_json"])),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]
