from __future__ import annotations

import json
from typing import Any, Mapping

import pytest

from data_lineage_fix_agent.lineagetx.github_approval import (
    ApprovalVerificationError,
    GitHubApprovalExpectation,
    GitHubApprovalVerifier,
)


OWNER = "urn:li:corpuser:identity-data-owner"
API_URL = "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/issues/comments/42"


class FakeTransport:
    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, str]] = []

    def get_json(self, url: str, *, token: str) -> Mapping[str, Any]:
        self.calls.append((url, token))
        return self.payload


def expectation() -> GitHubApprovalExpectation:
    return GitHubApprovalExpectation(
        migration_id="ltx-7ba06b0789512486f0f92f3c",
        participant_id="participant-semantic",
        owner_urn=OWNER,
        old_field="customer_id",
        new_field="customer_key",
    )


def approval_body(**changes: str) -> str:
    value = {
        "decision": "APPROVED",
        "migration_id": expectation().migration_id,
        "participant_id": expectation().participant_id,
        "owner_urn": OWNER,
        "old_field": "customer_id",
        "new_field": "customer_key",
    }
    value.update(changes)
    return json.dumps(value, sort_keys=True)


def issue_comment(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": 42,
        "node_id": "IC_kwDO-lineagetx-42",
        "html_url": (
            "https://github.com/Lukeknow0/data-lineage-fix-agent/"
            "issues/7#issuecomment-42"
        ),
        "body": approval_body(),
        "created_at": "2026-07-17T02:00:00Z",
        "updated_at": "2026-07-17T02:00:00Z",
        "author_association": "MEMBER",
        "user": {"login": "DataOwner", "id": 314, "type": "User"},
    }
    value.update(changes)
    return value


def verifier(payload: Mapping[str, Any], **kwargs: Any) -> GitHubApprovalVerifier:
    return GitHubApprovalVerifier(
        owner_login_by_urn={OWNER: "dataowner"},
        transport=FakeTransport(payload),
        token="not-written-to-receipt",
        now=lambda: "2026-07-17T02:01:00Z",
        **kwargs,
    )


def test_verifies_exact_unedited_github_issue_comment() -> None:
    transport = FakeTransport(issue_comment())
    check = GitHubApprovalVerifier(
        owner_login_by_urn={OWNER: "dataowner"},
        transport=transport,
        token="github-test-token",
        now=lambda: "2026-07-17T02:01:00Z",
    )

    receipt = check.verify(expectation(), API_URL)

    assert receipt.actor_login == "DataOwner"
    assert receipt.owner_urn == OWNER
    assert receipt.old_field == "customer_id"
    assert receipt.new_field == "customer_key"
    assert receipt.resource_kind == "issue_comment"
    assert len(receipt.evidence_sha256) == 64
    assert "token" not in json.dumps(receipt.to_dict()).lower()
    assert transport.calls == [(API_URL, "github-test-token")]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (issue_comment(body=approval_body(new_field="wrong")), "exact migration"),
        (
            issue_comment(user={"login": "imposter", "id": 99, "type": "User"}),
            "does not match",
        ),
        (
            issue_comment(author_association="CONTRIBUTOR"),
            "association",
        ),
        (
            issue_comment(updated_at="2026-07-17T02:02:00Z"),
            "edited issue comments",
        ),
        (issue_comment(id=43), "resource ID"),
    ],
)
def test_rejects_forged_or_mutable_comment_fields(
    payload: Mapping[str, Any], message: str
) -> None:
    with pytest.raises(ApprovalVerificationError, match=message):
        verifier(payload).verify(expectation(), API_URL)


def test_explicit_allowlist_can_replace_repository_association_not_owner_mapping() -> None:
    receipt = verifier(
        issue_comment(author_association="CONTRIBUTOR"),
        explicit_login_allowlist=("DATAOWNER",),
    ).verify(expectation(), API_URL)
    assert receipt.author_association == "CONTRIBUTOR"

    with pytest.raises(ApprovalVerificationError, match="does not match"):
        GitHubApprovalVerifier(
            owner_login_by_urn={OWNER: "someone-else"},
            explicit_login_allowlist=("dataowner",),
            transport=FakeTransport(issue_comment()),
        ).verify(expectation(), API_URL)


def test_verifies_approved_pull_request_review() -> None:
    api_url = (
        "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/"
        "pulls/9/reviews/88"
    )
    payload = issue_comment(
        id=88,
        html_url=(
            "https://github.com/Lukeknow0/data-lineage-fix-agent/"
            "pull/9#pullrequestreview-88"
        ),
        submitted_at="2026-07-17T02:00:00Z",
        state="APPROVED",
        commit_id="1" * 40,
    )
    payload.pop("created_at")
    payload.pop("updated_at")

    receipt = verifier(payload).verify(expectation(), api_url)

    assert receipt.resource_kind == "pull_request_review"
    assert receipt.resource_id == 88


@pytest.mark.parametrize(
    "url",
    [
        "http://api.github.com/repos/o/r/issues/comments/1",
        "https://evil.example/repos/o/r/issues/comments/1",
        "https://api.github.com/repos/o/r/issues/1/comments",
        "https://api.github.com/repos/o/r/issues/comments/1?token=x",
    ],
)
def test_rejects_noncanonical_api_sources(url: str) -> None:
    with pytest.raises(ApprovalVerificationError, match="approval source"):
        verifier(issue_comment()).verify(expectation(), url)
