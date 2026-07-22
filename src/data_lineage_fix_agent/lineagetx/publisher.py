from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import urlencode, urlsplit

from .evidence import EvidenceRecorder
from .models import (
    ChangeIntent,
    CoordinationReceipt,
    CoordinationReceiptKind,
    is_commit_sha,
    require_https_url,
    utc_now,
)


class PublicationError(RuntimeError):
    """Candidate publication did not produce safe, unmerged coordination receipts."""


_TRUSTED_GITHUB_API_ORIGINS = frozenset({"https://api.github.com"})


def _require_github_web_url(value: str, field_name: str) -> None:
    require_https_url(value, field_name)
    parsed = urlsplit(value)
    if parsed.hostname != "github.com" or parsed.port is not None:
        raise ValueError(f"{field_name} must use the trusted github.com origin")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


@dataclass(frozen=True)
class CandidateCommit:
    participant_id: str
    repository: str
    branch: str
    commit_sha: str
    changed_files: tuple[str, ...]
    merged: bool = False

    def __post_init__(self) -> None:
        if not all(
            item.strip()
            for item in (
                self.participant_id,
                self.repository,
                self.branch,
                self.commit_sha,
            )
        ):
            raise ValueError("candidate commit identity is incomplete")
        if not self.branch.startswith("lineagetx/"):
            raise ValueError("candidate must use a LineageTX-owned branch")
        if self.merged:
            raise ValueError("LineageTX candidates must remain unmerged")
        if not is_commit_sha(self.commit_sha):
            raise ValueError("candidate commit must be a 40- or 64-character hex SHA")
        if not self.changed_files:
            raise ValueError("candidate commit must contain validated changes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "branch": self.branch,
            "changed_files": list(self.changed_files),
            "commit_sha": self.commit_sha,
            "merged": False,
            "participant_id": self.participant_id,
            "repository": self.repository,
        }


@dataclass(frozen=True)
class PublicationRequest:
    intent: ChangeIntent
    candidates: tuple[CandidateCommit, ...]
    title: str
    body: str
    coordinated_head_branch: str
    coordinated_base_branch: str = "main"
    auto_merge: bool = False

    def __post_init__(self) -> None:
        if self.auto_merge:
            raise ValueError("LineageTX never auto-merges coordinated changes")
        if len(self.candidates) != 3:
            raise ValueError("bounded LineageTX publication requires three candidates")
        participant_ids = [item.participant_id for item in self.candidates]
        if len(participant_ids) != len(set(participant_ids)):
            raise ValueError("candidate participant IDs must be unique")
        if not self.title.strip() or not self.body.strip():
            raise ValueError("coordinated PR title and body are required")
        for branch in (self.coordinated_head_branch, self.coordinated_base_branch):
            if not re.fullmatch(r"[A-Za-z0-9._/-]+", branch) or ".." in branch:
                raise ValueError(f"unsafe coordinated branch name: {branch!r}")


@dataclass(frozen=True)
class PublicationResult:
    candidate_receipts: tuple[CoordinationReceipt, ...]
    coordinated_pr_receipt: CoordinationReceipt
    producer_gate_receipt: CoordinationReceipt

    @property
    def receipts(self) -> tuple[CoordinationReceipt, ...]:
        return (
            *self.candidate_receipts,
            self.coordinated_pr_receipt,
            self.producer_gate_receipt,
        )

    def validate(self, request: PublicationRequest) -> None:
        PublicationStage(
            self.candidate_receipts,
            self.coordinated_pr_receipt,
        ).validate(request)
        if (
            self.producer_gate_receipt.kind
            is not CoordinationReceiptKind.PRODUCER_GATE_RELEASED
        ):
            raise PublicationError("Producer gate receipt has the wrong kind")
        if self.producer_gate_receipt.migration_id != request.intent.migration_id:
            raise PublicationError("publisher returned a receipt for another migration")
        if self.producer_gate_receipt.merged:
            raise PublicationError("publisher claimed that a candidate was merged")
        if (
            self.producer_gate_receipt.evidence_url
            != self.coordinated_pr_receipt.reference
        ):
            raise PublicationError(
                "Producer gate evidence must point to the staged coordination PR"
            )


@dataclass(frozen=True)
class PublicationStage:
    """Receipts that are safe to create before the Producer success gate."""

    candidate_receipts: tuple[CoordinationReceipt, ...]
    coordinated_pr_receipt: CoordinationReceipt

    def validate(self, request: PublicationRequest) -> None:
        receipts = (*self.candidate_receipts, self.coordinated_pr_receipt)
        if len(self.candidate_receipts) != len(request.candidates):
            raise PublicationError("publisher omitted candidate commit receipts")
        if any(item.migration_id != request.intent.migration_id for item in receipts):
            raise PublicationError("publisher returned a receipt for another migration")
        if any(item.merged for item in receipts):
            raise PublicationError("publisher claimed that a candidate was merged")
        if any(
            item.kind is not CoordinationReceiptKind.CANDIDATE_COMMIT
            for item in self.candidate_receipts
        ):
            raise PublicationError("candidate receipts have the wrong kind")
        if (
            self.coordinated_pr_receipt.kind
            is not CoordinationReceiptKind.COORDINATED_PR
        ):
            raise PublicationError("coordinated PR receipt has the wrong kind")
        expected_commits = {
            (f"refs/heads/{item.branch}", item.commit_sha)
            for item in request.candidates
        }
        actual_commits = {
            (item.reference, item.commit_sha) for item in self.candidate_receipts
        }
        if actual_commits != expected_commits:
            raise PublicationError("candidate commit receipts do not match local commits")


class CoordinationPublisher(Protocol):
    """Stages a PR, then idempotently releases/reconciles the Producer gate."""

    def stage(self, request: PublicationRequest) -> PublicationStage: ...

    def release_gate(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult: ...

    def reconcile(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult | None: ...

    def publish(self, request: PublicationRequest) -> PublicationResult: ...


class LocalReceiptPublisher:
    """Network-free publisher for reproducible demos and integration tests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._requests: dict[str, PublicationRequest] = {}
        self._stages: dict[str, PublicationStage] = {}
        self._results: dict[str, PublicationResult] = {}

    def stage(self, request: PublicationRequest) -> PublicationStage:
        migration_id = request.intent.migration_id
        previous = self._requests.get(migration_id)
        if previous is not None and previous != request:
            raise PublicationError("publication retry does not match the staged request")
        if migration_id in self._stages:
            return self._stages[migration_id]

        recorded_at = utc_now()
        candidate_receipts = tuple(
            CoordinationReceipt(
                migration_id=request.intent.migration_id,
                kind=CoordinationReceiptKind.CANDIDATE_COMMIT,
                reference=f"refs/heads/{item.branch}",
                commit_sha=item.commit_sha,
                recorded_at=recorded_at,
                merged=False,
            )
            for item in request.candidates
        )
        coordinated = CoordinationReceipt(
            migration_id=request.intent.migration_id,
            kind=CoordinationReceiptKind.COORDINATED_PR,
            reference=(
                "https://replay.lineagetx.invalid/"
                f"{request.intent.migration_id}/coordinated-pr"
            ),
            recorded_at=recorded_at,
            evidence_url=(
                "https://replay.lineagetx.invalid/"
                f"{request.intent.migration_id}/publication.json"
            ),
            merged=False,
        )
        stage = PublicationStage(candidate_receipts, coordinated)
        stage.validate(request)
        self._requests[migration_id] = request
        self._stages[migration_id] = stage
        return stage

    def release_gate(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult:
        stage.validate(request)
        migration_id = request.intent.migration_id
        if self._stages.get(migration_id) != stage:
            raise PublicationError("gate release requires this publisher's staged request")
        existing = self._results.get(migration_id)
        if existing is not None:
            existing.validate(request)
            return existing

        recorded_at = utc_now()
        gate = CoordinationReceipt(
            migration_id=request.intent.migration_id,
            kind=CoordinationReceiptKind.PRODUCER_GATE_RELEASED,
            reference=(
                "check-run:lineagetx/safe-to-contract:"
                f"{request.intent.producer_head_sha}"
            ),
            recorded_at=recorded_at,
            evidence_url=stage.coordinated_pr_receipt.reference,
            merged=False,
        )
        result = PublicationResult(
            stage.candidate_receipts,
            stage.coordinated_pr_receipt,
            gate,
        )
        result.validate(request)
        recorder = EvidenceRecorder(self.root, request.intent.migration_id)
        recorder.write_json(
            "publication.json",
            {
                "auto_merge": False,
                "candidates": [item.to_dict() for item in request.candidates],
                "coordinated_pr": stage.coordinated_pr_receipt.to_dict(),
                "producer_gate": gate.to_dict(),
                "title": request.title,
            },
        )
        self._results[migration_id] = result
        return result

    def reconcile(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult | None:
        stage.validate(request)
        result = self._results.get(request.intent.migration_id)
        if result is not None:
            result.validate(request)
        return result

    def publish(self, request: PublicationRequest) -> PublicationResult:
        stage = self.stage(request)
        return self.reconcile(request, stage) or self.release_gate(request, stage)


class GitHubAPI(Protocol):
    """Narrow REST seam. There is deliberately no pull-request merge operation."""

    def create_pull_request(
        self,
        repository: str,
        *,
        title: str,
        body: str,
        head: str,
        base: str,
        draft: bool,
    ) -> Mapping[str, Any]: ...

    def find_pull_request(
        self,
        repository: str,
        *,
        head: str,
        base: str,
    ) -> Mapping[str, Any] | None: ...

    def create_commit_status(
        self,
        repository: str,
        sha: str,
        *,
        state: str,
        context: str,
        description: str,
        target_url: str,
    ) -> Mapping[str, Any]: ...

    def get_commit_status(
        self,
        repository: str,
        sha: str,
        *,
        context: str,
    ) -> Mapping[str, Any] | None: ...


class GitHubRESTClient:
    """Minimal fixed-endpoint GitHub REST client for PR creation and gate status."""

    def __init__(
        self,
        token: str,
        *,
        api_url: str = "https://api.github.com",
        timeout_seconds: float = 20.0,
    ) -> None:
        parsed = urlsplit(api_url)
        origin = f"{parsed.scheme}://{parsed.netloc}".lower()
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.port is not None
            or origin not in _TRUSTED_GITHUB_API_ORIGINS
        ):
            raise ValueError(
                "GitHub API URL must be a trusted fixed HTTPS origin without "
                "userinfo, path, query, or fragment"
            )
        if not token:
            raise ValueError("GitHub token is required")
        self._token = token
        self.api_url = origin
        self.timeout_seconds = timeout_seconds
        self._opener = urllib.request.build_opener(_NoRedirect())

    @staticmethod
    def _repository(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", value):
            raise PublicationError(f"invalid fixed GitHub repository: {value!r}")
        return value

    def _read_json(self, request: urllib.request.Request) -> Any:
        try:
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, json.JSONDecodeError) as error:
            raise PublicationError(f"GitHub publication request failed: {error}") from error

    def _post(self, path: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        request = urllib.request.Request(
            f"{self.api_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            method="POST",
        )
        value = self._read_json(request)
        if not isinstance(value, dict):
            raise PublicationError("GitHub returned a non-object response")
        return value

    def _get(self, path: str) -> Any:
        request = urllib.request.Request(
            f"{self.api_url}{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            method="GET",
        )
        return self._read_json(request)

    def create_pull_request(
        self,
        repository: str,
        *,
        title: str,
        body: str,
        head: str,
        base: str,
        draft: bool,
    ) -> Mapping[str, Any]:
        return self._post(
            f"/repos/{self._repository(repository)}/pulls",
            {
                "base": base,
                "body": body,
                "draft": draft,
                "head": head,
                "title": title,
            },
        )

    def find_pull_request(
        self,
        repository: str,
        *,
        head: str,
        base: str,
    ) -> Mapping[str, Any] | None:
        fixed_repository = self._repository(repository)
        owner = fixed_repository.split("/", 1)[0]
        query = urlencode(
            {
                "base": base,
                "head": f"{owner}:{head}",
                "state": "open",
            }
        )
        value = self._get(f"/repos/{fixed_repository}/pulls?{query}")
        if not isinstance(value, list):
            raise PublicationError("GitHub returned a non-list pull request response")
        for item in value:
            if isinstance(item, Mapping):
                return item
        return None

    def create_commit_status(
        self,
        repository: str,
        sha: str,
        *,
        state: str,
        context: str,
        description: str,
        target_url: str,
    ) -> Mapping[str, Any]:
        if not is_commit_sha(sha):
            raise PublicationError("Producer commit must be a 40- or 64-character hex SHA")
        try:
            _require_github_web_url(target_url, "Producer status target_url")
        except ValueError as error:
            raise PublicationError(str(error)) from error
        return self._post(
            f"/repos/{self._repository(repository)}/statuses/{sha}",
            {
                "context": context,
                "description": description,
                "state": state,
                "target_url": target_url,
            },
        )

    def get_commit_status(
        self,
        repository: str,
        sha: str,
        *,
        context: str,
    ) -> Mapping[str, Any] | None:
        if not is_commit_sha(sha):
            raise PublicationError("Producer commit must be a 40- or 64-character hex SHA")
        value = self._get(
            f"/repos/{self._repository(repository)}/commits/{sha}/status"
        )
        if not isinstance(value, Mapping):
            raise PublicationError("GitHub returned a non-object status response")
        statuses = value.get("statuses")
        if not isinstance(statuses, list):
            raise PublicationError("GitHub status response omitted statuses")
        for status in statuses:
            if isinstance(status, Mapping) and status.get("context") == context:
                return status
        return None


class FixedGitHubPublisher:
    """Creates one draft coordination PR and releases a status gate, never merges."""

    def __init__(
        self,
        api: GitHubAPI,
        *,
        coordination_repository: str,
        producer_repository: str,
    ) -> None:
        for repository in (coordination_repository, producer_repository):
            if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
                raise ValueError(f"invalid fixed GitHub repository: {repository!r}")
        self.api = api
        self.coordination_repository = coordination_repository
        self.producer_repository = producer_repository
        self._requests: dict[str, PublicationRequest] = {}
        self._stages: dict[str, PublicationStage] = {}
        self._results: dict[str, PublicationResult] = {}

    def stage(self, request: PublicationRequest) -> PublicationStage:
        if request.intent.producer_repository != self.producer_repository:
            raise PublicationError("Producer repository does not match fixed publisher scope")
        migration_id = request.intent.migration_id
        previous = self._requests.get(migration_id)
        if previous is not None and previous != request:
            raise PublicationError("publication retry does not match the staged request")
        if migration_id in self._stages:
            return self._stages[migration_id]

        find_pull_request = getattr(self.api, "find_pull_request", None)
        pr = (
            find_pull_request(
                self.coordination_repository,
                head=request.coordinated_head_branch,
                base=request.coordinated_base_branch,
            )
            if callable(find_pull_request)
            else None
        )
        if pr is None:
            pr = self.api.create_pull_request(
                self.coordination_repository,
                title=request.title,
                body=request.body,
                head=request.coordinated_head_branch,
                base=request.coordinated_base_branch,
                draft=True,
            )
        pr_url = pr.get("html_url") if isinstance(pr, Mapping) else None
        if not isinstance(pr_url, str):
            raise PublicationError("GitHub did not return a coordination PR URL")
        try:
            _require_github_web_url(pr_url, "coordination PR URL")
        except ValueError as error:
            raise PublicationError(str(error)) from error
        expected_prefix = f"/{self.coordination_repository}/pull/"
        parsed_pr = urlsplit(pr_url)
        if not parsed_pr.path.startswith(expected_prefix) or not parsed_pr.path[
            len(expected_prefix) :
        ].isdigit():
            raise PublicationError("GitHub returned a PR outside the fixed repository")
        if pr.get("merged") is True:
            raise PublicationError("GitHub unexpectedly reported the PR as merged")

        recorded_at = utc_now()
        candidate_receipts = tuple(
            CoordinationReceipt(
                migration_id=request.intent.migration_id,
                kind=CoordinationReceiptKind.CANDIDATE_COMMIT,
                reference=f"refs/heads/{item.branch}",
                commit_sha=item.commit_sha,
                recorded_at=recorded_at,
                merged=False,
            )
            for item in request.candidates
        )
        coordinated = CoordinationReceipt(
            migration_id=request.intent.migration_id,
            kind=CoordinationReceiptKind.COORDINATED_PR,
            reference=pr_url,
            recorded_at=recorded_at,
            evidence_url=pr_url,
            merged=False,
        )
        stage = PublicationStage(candidate_receipts, coordinated)
        stage.validate(request)
        self._requests[migration_id] = request
        self._stages[migration_id] = stage
        return stage

    def _result_from_stage(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult:
        recorded_at = utc_now()
        gate = CoordinationReceipt(
            migration_id=request.intent.migration_id,
            kind=CoordinationReceiptKind.PRODUCER_GATE_RELEASED,
            reference=(
                "check-run:lineagetx/safe-to-contract:"
                f"{request.intent.producer_head_sha}"
            ),
            recorded_at=recorded_at,
            evidence_url=stage.coordinated_pr_receipt.reference,
            merged=False,
        )
        result = PublicationResult(
            stage.candidate_receipts,
            stage.coordinated_pr_receipt,
            gate,
        )
        result.validate(request)
        return result

    def release_gate(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult:
        stage.validate(request)
        migration_id = request.intent.migration_id
        if self._stages.get(migration_id) != stage:
            raise PublicationError("gate release requires this publisher's staged request")
        existing = self._results.get(migration_id)
        if existing is not None:
            existing.validate(request)
            return existing

        status = self.api.create_commit_status(
            self.producer_repository,
            request.intent.producer_head_sha,
            state="success",
            context="lineagetx/safe-to-contract",
            description="0 unverified consumers; safe to merge (merge remains manual)",
            target_url=stage.coordinated_pr_receipt.reference,
        )
        if status.get("state") != "success":
            raise PublicationError("GitHub did not confirm the Producer gate release")
        result = self._result_from_stage(request, stage)
        self._results[migration_id] = result
        return result

    def reconcile(
        self,
        request: PublicationRequest,
        stage: PublicationStage,
    ) -> PublicationResult | None:
        stage.validate(request)
        migration_id = request.intent.migration_id
        existing = self._results.get(migration_id)
        if existing is not None:
            existing.validate(request)
            return existing

        get_status = getattr(self.api, "get_commit_status", None)
        if not callable(get_status):
            return None
        status = get_status(
            self.producer_repository,
            request.intent.producer_head_sha,
            context="lineagetx/safe-to-contract",
        )
        if status is None or status.get("state") != "success":
            return None
        target_url = status.get("target_url")
        if target_url != stage.coordinated_pr_receipt.reference:
            raise PublicationError(
                "existing Producer success gate points outside the staged coordination PR"
            )
        result = self._result_from_stage(request, stage)
        self._results[migration_id] = result
        return result

    def publish(self, request: PublicationRequest) -> PublicationResult:
        stage = self.stage(request)
        return self.reconcile(request, stage) or self.release_gate(request, stage)
