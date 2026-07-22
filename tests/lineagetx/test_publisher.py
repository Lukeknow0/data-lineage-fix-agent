from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from data_lineage_fix_agent.lineagetx.models import (
    ChangeIntent,
    CoordinationReceipt,
    CoordinationReceiptKind,
)
from data_lineage_fix_agent.lineagetx.publisher import (
    CandidateCommit,
    FixedGitHubPublisher,
    GitHubRESTClient,
    LocalReceiptPublisher,
    PublicationError,
    PublicationRequest,
)


def _intent() -> ChangeIntent:
    return ChangeIntent.create(
        producer_repository="acme/commerce-producer",
        producer_pr_number=42,
        producer_base_sha="producer-base",
        producer_head_sha="producer-head",
        source_asset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)",
        old_field="customer_id",
        new_field="customer_key",
        contract_schema_fingerprint="sha256:contract-v2",
        created_at="2026-07-17T01:02:03.000000Z",
    )


def _request(*, auto_merge: bool = False) -> PublicationRequest:
    intent = _intent()
    candidates = tuple(
        CandidateCommit(
            participant_id=f"consumer-{index}",
            repository=f"acme/repo-{index}",
            branch=f"lineagetx/{intent.migration_id}/consumer-{index}",
            commit_sha=f"{index + 1:040x}",
            changed_files=(f"consumer-{index}.txt",),
        )
        for index in range(3)
    )
    return PublicationRequest(
        intent=intent,
        candidates=candidates,
        title="LineageTX coordinated customer key migration",
        body="Three candidates verified; merge remains manual.",
        coordinated_head_branch=f"lineagetx/{intent.migration_id}/coordination",
        auto_merge=auto_merge,
    )


def test_local_publisher_is_network_free_and_never_merges(tmp_path: Path) -> None:
    request = _request()
    result = LocalReceiptPublisher(tmp_path / "evidence").publish(request)

    result.validate(request)
    assert len(result.candidate_receipts) == 3
    assert all(
        item.kind is CoordinationReceiptKind.CANDIDATE_COMMIT
        for item in result.candidate_receipts
    )
    assert all(not item.merged for item in result.receipts)
    payload = json.loads((tmp_path / "evidence/publication.json").read_text())
    assert payload["auto_merge"] is False
    assert {item["commit_sha"] for item in payload["candidates"]} == {
        f"{1:040x}",
        f"{2:040x}",
        f"{3:040x}",
    }


def test_publication_validation_binds_candidate_refs_and_gate_evidence(
    tmp_path: Path,
) -> None:
    request = _request()
    result = LocalReceiptPublisher(tmp_path / "evidence").publish(request)
    wrong_candidate = CoordinationReceipt(
        migration_id=request.intent.migration_id,
        kind=CoordinationReceiptKind.CANDIDATE_COMMIT,
        reference="refs/heads/lineagetx/wrong/branch",
        commit_sha=result.candidate_receipts[0].commit_sha,
        recorded_at=result.candidate_receipts[0].recorded_at,
    )
    with pytest.raises(PublicationError, match="do not match"):
        type(result)(
            (wrong_candidate, *result.candidate_receipts[1:]),
            result.coordinated_pr_receipt,
            result.producer_gate_receipt,
        ).validate(request)

    wrong_gate = CoordinationReceipt(
        migration_id=request.intent.migration_id,
        kind=CoordinationReceiptKind.PRODUCER_GATE_RELEASED,
        reference=result.producer_gate_receipt.reference,
        recorded_at=result.producer_gate_receipt.recorded_at,
        evidence_url="https://github.com/acme/other/pull/1",
    )
    with pytest.raises(PublicationError, match="staged coordination PR"):
        type(result)(
            result.candidate_receipts,
            result.coordinated_pr_receipt,
            wrong_gate,
        ).validate(request)


def test_publication_request_rejects_auto_merge() -> None:
    with pytest.raises(ValueError, match="never auto-merges"):
        _request(auto_merge=True)


class FakeGitHubAPI:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

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
        self.calls.append(
            (
                "create_pull_request",
                (repository,),
                {"title": title, "body": body, "head": head, "base": base, "draft": draft},
            )
        )
        return {
            "html_url": "https://github.com/acme/coordination/pull/9",
            "number": 9,
            "draft": True,
            "merged": False,
        }

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
        self.calls.append(
            (
                "create_commit_status",
                (repository, sha),
                {
                    "state": state,
                    "context": context,
                    "description": description,
                    "target_url": target_url,
                },
            )
        )
        return {"state": "success", "target_url": target_url}


def test_fixed_github_publisher_only_creates_draft_pr_then_gate_status() -> None:
    api = FakeGitHubAPI()
    request = _request()
    publisher = FixedGitHubPublisher(
        api,
        coordination_repository="acme/coordination",
        producer_repository="acme/commerce-producer",
    )

    result = publisher.publish(request)

    assert [item[0] for item in api.calls] == [
        "create_pull_request",
        "create_commit_status",
    ]
    assert api.calls[0][2]["draft"] is True
    assert api.calls[1][2]["context"] == "lineagetx/safe-to-contract"
    assert result.coordinated_pr_receipt.reference.endswith("/pull/9")
    assert result.producer_gate_receipt.kind is (
        CoordinationReceiptKind.PRODUCER_GATE_RELEASED
    )
    assert all(not item.merged for item in result.receipts)


@pytest.mark.parametrize(
    "api_url",
    (
        "http://api.github.com",
        "https://user@api.github.com",
        "https://api.github.com/attacker",
        "https://api.github.com?redirect=evil",
        "https://api.github.com#fragment",
        "https://api.github.com.evil.invalid",
        "https://github.example/api/v3",
    ),
)
def test_github_client_restricts_api_to_fixed_trusted_origin(api_url: str) -> None:
    with pytest.raises(ValueError, match="trusted fixed HTTPS origin"):
        GitHubRESTClient("test-token", api_url=api_url)


def test_fixed_publisher_rejects_pr_evidence_query_before_gate_release() -> None:
    class QueryURLAPI(FakeGitHubAPI):
        def create_pull_request(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
            response = dict(super().create_pull_request(*args, **kwargs))
            response["html_url"] = (
                "https://github.com/acme/coordination/pull/9?token=private"
            )
            return response

    api = QueryURLAPI()
    publisher = FixedGitHubPublisher(
        api,
        coordination_repository="acme/coordination",
        producer_repository="acme/commerce-producer",
    )
    with pytest.raises(PublicationError, match="query"):
        publisher.stage(_request())
    assert [item[0] for item in api.calls] == ["create_pull_request"]


def test_fixed_publisher_reconciles_success_after_gate_response_is_lost() -> None:
    class LostResponseAPI(FakeGitHubAPI):
        def __init__(self) -> None:
            super().__init__()
            self.persisted_status: Mapping[str, Any] | None = None

        def create_commit_status(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
            repository, sha = args
            target_url = kwargs["target_url"]
            self.calls.append(("create_commit_status", (repository, sha), dict(kwargs)))
            self.persisted_status = {
                "context": "lineagetx/safe-to-contract",
                "state": "success",
                "target_url": target_url,
            }
            raise PublicationError("response lost after external success")

        def get_commit_status(
            self,
            repository: str,
            sha: str,
            *,
            context: str,
        ) -> Mapping[str, Any] | None:
            assert repository == "acme/commerce-producer"
            assert sha == _request().intent.producer_head_sha
            assert context == "lineagetx/safe-to-contract"
            return self.persisted_status

    api = LostResponseAPI()
    request = _request()
    publisher = FixedGitHubPublisher(
        api,
        coordination_repository="acme/coordination",
        producer_repository="acme/commerce-producer",
    )
    stage = publisher.stage(request)
    with pytest.raises(PublicationError, match="response lost"):
        publisher.release_gate(request, stage)

    recovered = publisher.reconcile(request, stage)
    assert recovered is not None
    recovered.validate(request)
    assert recovered.producer_gate_receipt.kind is (
        CoordinationReceiptKind.PRODUCER_GATE_RELEASED
    )
    assert len([call for call in api.calls if call[0] == "create_commit_status"]) == 1


@pytest.mark.parametrize("target_url", (None, ""))
def test_fixed_publisher_reconcile_rejects_success_without_exact_target_url(
    target_url: str | None,
) -> None:
    class MissingTargetAPI(FakeGitHubAPI):
        def get_commit_status(
            self,
            repository: str,
            sha: str,
            *,
            context: str,
        ) -> Mapping[str, Any] | None:
            return {
                "context": context,
                "state": "success",
                "target_url": target_url,
            }

    api = MissingTargetAPI()
    request = _request()
    publisher = FixedGitHubPublisher(
        api,
        coordination_repository="acme/coordination",
        producer_repository="acme/commerce-producer",
    )
    stage = publisher.stage(request)

    with pytest.raises(PublicationError, match="staged coordination PR"):
        publisher.reconcile(request, stage)


def test_new_fixed_publisher_instance_recovers_existing_pr_and_success_gate() -> None:
    class ExistingPublicationAPI(FakeGitHubAPI):
        def find_pull_request(
            self,
            repository: str,
            *,
            head: str,
            base: str,
        ) -> Mapping[str, Any] | None:
            assert repository == "acme/coordination"
            assert head.startswith("lineagetx/")
            assert base == "main"
            return {
                "html_url": "https://github.com/acme/coordination/pull/9",
                "number": 9,
                "draft": True,
                "merged": False,
            }

        def get_commit_status(
            self,
            repository: str,
            sha: str,
            *,
            context: str,
        ) -> Mapping[str, Any] | None:
            return {
                "context": context,
                "state": "success",
                "target_url": "https://github.com/acme/coordination/pull/9",
            }

    api = ExistingPublicationAPI()
    request = _request()
    recovered = FixedGitHubPublisher(
        api,
        coordination_repository="acme/coordination",
        producer_repository="acme/commerce-producer",
    ).publish(request)

    recovered.validate(request)
    assert api.calls == []
