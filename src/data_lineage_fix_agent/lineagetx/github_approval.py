from __future__ import annotations

import hashlib
import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlsplit

from .models import canonical_json, require_https_url, require_utc_timestamp, utc_now
from .participants.semantic_approval import OwnerApproval


class ApprovalVerificationError(RuntimeError):
    """GitHub did not prove that the accountable DataHub owner approved."""


_ISSUE_COMMENT_PATH = re.compile(
    r"^/repos/(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+)/"
    r"issues/comments/(?P<resource_id>[1-9][0-9]*)$"
)
_PULL_REVIEW_PATH = re.compile(
    r"^/repos/(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+)/"
    r"pulls/(?P<pull_number>[1-9][0-9]*)/reviews/"
    r"(?P<resource_id>[1-9][0-9]*)$"
)
_APPROVAL_KEYS = frozenset(
    {
        "decision",
        "migration_id",
        "participant_id",
        "owner_urn",
        "old_field",
        "new_field",
    }
)


@dataclass(frozen=True)
class GitHubApprovalExpectation:
    migration_id: str
    participant_id: str
    owner_urn: str
    old_field: str
    new_field: str


@dataclass(frozen=True)
class VerifiedGitHubApproval:
    """Content-addressed receipt from one authenticated GitHub API resource."""

    migration_id: str
    participant_id: str
    owner_urn: str
    old_field: str
    new_field: str
    approved_at: str
    evidence_url: str
    browser_url: str
    source_api_url: str
    resource_kind: str
    resource_id: int
    resource_node_id: str
    actor_login: str
    actor_id: int
    author_association: str
    evidence_sha256: str
    verified_at: str
    decision: str = "APPROVED"
    verification_provider: str = "github-rest-api-v3"

    def __post_init__(self) -> None:
        require_https_url(self.evidence_url, "GitHub approval evidence_url")
        browser = urlsplit(self.browser_url)
        if (
            browser.scheme != "https"
            or browser.hostname != "github.com"
            or browser.username is not None
            or browser.password is not None
            or browser.query
        ):
            raise ValueError("GitHub approval browser_url must be an HTTPS github.com URL")
        require_https_url(self.source_api_url, "GitHub approval source_api_url")
        require_utc_timestamp(self.approved_at, "approved_at")
        require_utc_timestamp(self.verified_at, "verified_at")
        if self.resource_kind not in {"issue_comment", "pull_request_review"}:
            raise ValueError("unsupported GitHub approval resource kind")
        if self.resource_id <= 0 or self.actor_id <= 0:
            raise ValueError("GitHub approval IDs must be positive")
        if not re.fullmatch(r"[0-9a-f]{64}", self.evidence_sha256):
            raise ValueError("GitHub approval evidence_sha256 must be SHA-256")
        if self.decision != "APPROVED":
            raise ValueError("verified GitHub approval decision must be APPROVED")

    def to_owner_approval(self) -> OwnerApproval:
        return OwnerApproval(
            migration_id=self.migration_id,
            participant_id=self.participant_id,
            owner_urn=self.owner_urn,
            old_field=self.old_field,
            new_field=self.new_field,
            approved_at=self.approved_at,
            evidence_url=self.evidence_url,
            decision=self.decision,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "actor_login": self.actor_login,
            "approved_at": self.approved_at,
            "author_association": self.author_association,
            "browser_url": self.browser_url,
            "decision": self.decision,
            "evidence_sha256": self.evidence_sha256,
            "evidence_url": self.evidence_url,
            "migration_id": self.migration_id,
            "new_field": self.new_field,
            "old_field": self.old_field,
            "owner_urn": self.owner_urn,
            "participant_id": self.participant_id,
            "resource_id": self.resource_id,
            "resource_kind": self.resource_kind,
            "resource_node_id": self.resource_node_id,
            "source_api_url": self.source_api_url,
            "verification_provider": self.verification_provider,
            "verified_at": self.verified_at,
        }


class GitHubApprovalTransport(Protocol):
    def get_json(self, url: str, *, token: str) -> Mapping[str, Any]: ...


class UrllibGitHubApprovalTransport:
    """Narrow read-only client for a single allowlisted GitHub REST resource."""

    def __init__(self, *, timeout_seconds: float = 15.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("GitHub approval timeout must be positive")
        self.timeout_seconds = timeout_seconds

    def get_json(self, url: str, *, token: str) -> Mapping[str, Any]:
        class RejectRedirects(urllib.request.HTTPRedirectHandler):
            def redirect_request(  # type: ignore[override]
                self,
                request: Any,
                file_pointer: Any,
                code: int,
                message: str,
                headers: Any,
                new_url: str,
            ) -> None:
                raise ApprovalVerificationError(
                    "GitHub approval API redirects are not accepted"
                )

        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "LineageTX-approval-verifier",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(url, method="GET", headers=headers)
        try:
            opener = urllib.request.build_opener(RejectRedirects())
            with opener.open(request, timeout=self.timeout_seconds) as response:
                if response.geturl() != url:
                    raise ApprovalVerificationError(
                        "GitHub approval API redirects are not accepted"
                    )
                raw = response.read(1_000_001)
        except ApprovalVerificationError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise ApprovalVerificationError(
                f"GitHub approval API request failed: {type(error).__name__}"
            ) from error
        if len(raw) > 1_000_000:
            raise ApprovalVerificationError("GitHub approval response is too large")
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ApprovalVerificationError(
                "GitHub approval API returned invalid JSON"
            ) from error
        if not isinstance(value, Mapping):
            raise ApprovalVerificationError(
                "GitHub approval API response must be an object"
            )
        return value


class GitHubApprovalVerifier:
    """Verify an exact approval against GitHub-authenticated actor metadata.

    The approval body is a JSON object with exactly the six LineageTX fields.
    Actor identity, timestamps, author association, stable resource ID, and the
    browser evidence URL are taken only from GitHub's API response.
    """

    TRUSTED_ASSOCIATIONS = frozenset({"OWNER", "MEMBER", "COLLABORATOR"})

    def __init__(
        self,
        *,
        owner_login_by_urn: Mapping[str, str],
        explicit_login_allowlist: tuple[str, ...] = (),
        token: str = "",
        transport: GitHubApprovalTransport | None = None,
        now: Callable[[], str] = utc_now,
    ) -> None:
        normalized: dict[str, str] = {}
        for urn, login in owner_login_by_urn.items():
            owner_urn = str(urn).strip()
            github_login = str(login).strip()
            if not owner_urn.startswith("urn:li:") or not github_login:
                raise ValueError("owner-to-GitHub mapping is invalid")
            normalized[owner_urn] = github_login.casefold()
        if not normalized:
            raise ValueError("at least one owner-to-GitHub mapping is required")
        self.owner_login_by_urn = normalized
        self.explicit_login_allowlist = frozenset(
            str(item).strip().casefold()
            for item in explicit_login_allowlist
            if str(item).strip()
        )
        self.token = token
        self.transport = transport or UrllibGitHubApprovalTransport()
        self.now = now

    def verify(
        self,
        expectation: GitHubApprovalExpectation,
        source_api_url: str,
    ) -> VerifiedGitHubApproval:
        resource_kind = self._validate_api_url(source_api_url)
        payload = self.transport.get_json(source_api_url, token=self.token)
        return self._verify_payload(
            expectation,
            source_api_url,
            resource_kind,
            payload,
        )

    @staticmethod
    def _validate_api_url(source_api_url: str) -> str:
        parsed = urlsplit(source_api_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.github.com"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ApprovalVerificationError(
                "approval source must be a canonical HTTPS api.github.com URL"
            )
        if _ISSUE_COMMENT_PATH.fullmatch(parsed.path):
            return "issue_comment"
        if _PULL_REVIEW_PATH.fullmatch(parsed.path):
            return "pull_request_review"
        raise ApprovalVerificationError(
            "approval source must identify one GitHub issue comment or PR review"
        )

    def _verify_payload(
        self,
        expectation: GitHubApprovalExpectation,
        source_api_url: str,
        resource_kind: str,
        payload: Mapping[str, Any],
    ) -> VerifiedGitHubApproval:
        resource_id = payload.get("id")
        node_id = payload.get("node_id")
        html_url = payload.get("html_url")
        body = payload.get("body")
        actor = payload.get("user")
        association = payload.get("author_association")
        if (
            not isinstance(resource_id, int)
            or resource_id <= 0
            or not isinstance(node_id, str)
            or not node_id
            or not isinstance(html_url, str)
            or not isinstance(body, str)
            or not isinstance(actor, Mapping)
            or not isinstance(association, str)
        ):
            raise ApprovalVerificationError(
                "GitHub approval response omitted authenticated resource metadata"
            )
        source_path = urlsplit(source_api_url).path
        source_match = (
            _ISSUE_COMMENT_PATH.fullmatch(source_path)
            if resource_kind == "issue_comment"
            else _PULL_REVIEW_PATH.fullmatch(source_path)
        )
        assert source_match is not None
        if resource_id != int(source_match.group("resource_id")):
            raise ApprovalVerificationError(
                "GitHub API resource ID does not match the requested stable URL"
            )
        browser = urlsplit(html_url)
        expected_prefix = (
            f"/{source_match.group('owner')}/{source_match.group('repo')}/"
        ).casefold()
        if (
            browser.hostname != "github.com"
            or not browser.path.casefold().startswith(expected_prefix)
        ):
            raise ApprovalVerificationError(
                "approval browser evidence must identify the same github.com repository"
            )
        expected_fragment = (
            f"issuecomment-{resource_id}"
            if resource_kind == "issue_comment"
            else f"pullrequestreview-{resource_id}"
        )
        if browser.fragment != expected_fragment:
            raise ApprovalVerificationError(
                "approval browser evidence does not match the stable GitHub resource ID"
            )
        if resource_kind == "pull_request_review" and not browser.path.casefold().endswith(
            f"/pull/{source_match.group('pull_number')}".casefold()
        ):
            raise ApprovalVerificationError(
                "approval browser evidence does not match the reviewed pull request"
            )

        actor_login = actor.get("login")
        actor_id = actor.get("id")
        actor_type = actor.get("type")
        if (
            not isinstance(actor_login, str)
            or not actor_login
            or not isinstance(actor_id, int)
            or actor_id <= 0
            or actor_type != "User"
        ):
            raise ApprovalVerificationError(
                "approval actor must be an authenticated GitHub user"
            )
        expected_login = self.owner_login_by_urn.get(expectation.owner_urn)
        if expected_login is None:
            raise ApprovalVerificationError(
                "the discovered DataHub owner has no trusted GitHub login mapping"
            )
        if actor_login.casefold() != expected_login:
            raise ApprovalVerificationError(
                "GitHub actor does not match the discovered DataHub owner"
            )
        normalized_association = association.upper()
        if (
            normalized_association not in self.TRUSTED_ASSOCIATIONS
            and actor_login.casefold() not in self.explicit_login_allowlist
        ):
            raise ApprovalVerificationError(
                "GitHub actor lacks a trusted repository association or explicit allowlist"
            )

        approval = self._parse_exact_body(body)
        expected_body = {
            "decision": "APPROVED",
            "migration_id": expectation.migration_id,
            "new_field": expectation.new_field,
            "old_field": expectation.old_field,
            "owner_urn": expectation.owner_urn,
            "participant_id": expectation.participant_id,
        }
        if approval != expected_body:
            raise ApprovalVerificationError(
                "GitHub approval body does not match the exact migration and field mapping"
            )

        if resource_kind == "issue_comment":
            approved_at = payload.get("created_at")
            updated_at = payload.get("updated_at")
            if not isinstance(approved_at, str) or updated_at != approved_at:
                raise ApprovalVerificationError(
                    "edited issue comments are not accepted as approval evidence"
                )
        else:
            approved_at = payload.get("submitted_at")
            if payload.get("state") != "APPROVED":
                raise ApprovalVerificationError(
                    "GitHub PR review is not currently APPROVED"
                )
            if not isinstance(payload.get("commit_id"), str) or not payload.get(
                "commit_id"
            ):
                raise ApprovalVerificationError(
                    "GitHub PR review is not bound to a commit"
                )
        if not isinstance(approved_at, str):
            raise ApprovalVerificationError("GitHub approval timestamp is missing")
        try:
            require_utc_timestamp(approved_at, "GitHub approval timestamp")
        except ValueError as error:
            raise ApprovalVerificationError(str(error)) from error

        signed_snapshot = {
            "approval": approval,
            "approved_at": approved_at,
            "actor_id": actor_id,
            "actor_login": actor_login,
            "author_association": normalized_association,
            "evidence_url": html_url,
            "resource_id": resource_id,
            "resource_kind": resource_kind,
            "resource_node_id": node_id,
            "source_api_url": source_api_url,
        }
        digest = hashlib.sha256(
            canonical_json(signed_snapshot).encode("utf-8")
        ).hexdigest()
        return VerifiedGitHubApproval(
            migration_id=expectation.migration_id,
            participant_id=expectation.participant_id,
            owner_urn=expectation.owner_urn,
            old_field=expectation.old_field,
            new_field=expectation.new_field,
            approved_at=approved_at,
            evidence_url=source_api_url,
            browser_url=html_url,
            source_api_url=source_api_url,
            resource_kind=resource_kind,
            resource_id=resource_id,
            resource_node_id=node_id,
            actor_login=actor_login,
            actor_id=actor_id,
            author_association=normalized_association,
            evidence_sha256=digest,
            verified_at=self.now(),
        )

    @staticmethod
    def _parse_exact_body(body: str) -> dict[str, str]:
        def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            value: dict[str, Any] = {}
            for key, item in pairs:
                if key in value:
                    raise ApprovalVerificationError(
                        "GitHub approval body contains a duplicate field"
                    )
                value[key] = item
            return value

        try:
            raw = json.loads(body, object_pairs_hook=reject_duplicates)
        except ApprovalVerificationError:
            raise
        except json.JSONDecodeError as error:
            raise ApprovalVerificationError(
                "GitHub approval body must be one JSON object"
            ) from error
        if not isinstance(raw, dict) or frozenset(raw) != _APPROVAL_KEYS:
            raise ApprovalVerificationError(
                "GitHub approval body must contain exactly the required fields"
            )
        if any(not isinstance(value, str) or not value for value in raw.values()):
            raise ApprovalVerificationError(
                "GitHub approval fields must be non-empty strings"
            )
        return {str(key): str(value) for key, value in raw.items()}
