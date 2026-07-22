from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from data_lineage_fix_agent.lineagetx import worktrees as worktrees_module
from data_lineage_fix_agent.lineagetx.worktrees import (
    BaseRepositoryChanged,
    MergedCandidateError,
    WorktreeError,
    WorktreeManager,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "lineagetx" / "repos"


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout
    return completed.stdout.strip()


def _repository(tmp_path: Path, name: str = "data-platform") -> Path:
    repo = tmp_path / name
    shutil.copytree(FIXTURES / name, repo)
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "LineageTX Test")
    _git(repo, "config", "user.email", "lineagetx@example.invalid")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "fixture baseline")
    return repo


def test_prepare_and_abort_never_write_to_base_checkout(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    base_sha = _git(repo, "rev-parse", "HEAD")
    base_sql = (repo / "dbt/models/stg_orders.sql").read_text(encoding="utf-8")

    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    candidate_sql = session.worktree / "dbt/models/stg_orders.sql"
    candidate_sql.write_text(
        candidate_sql.read_text(encoding="utf-8").replace(
            "customer_id", "customer_key"
        ),
        encoding="utf-8",
    )

    assert manager.changed_files(session) == ("dbt/models/stg_orders.sql",)
    assert _git(repo, "rev-parse", "HEAD") == base_sha
    assert _git(repo, "status", "--porcelain=v1") == ""
    assert (repo / "dbt/models/stg_orders.sql").read_text(encoding="utf-8") == base_sql

    candidate_sha = manager.commit_candidate(
        session,
        allowed_paths=("dbt/models/stg_orders.sql",),
        message="LineageTX: prepare dbt consumer",
    )
    assert candidate_sha != base_sha
    manager.abort(session)

    assert not session.worktree.exists()
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "show-ref",
                "--verify",
                f"refs/heads/{session.branch}",
            ],
            check=False,
        ).returncode
        != 0
    )
    assert _git(repo, "rev-parse", "HEAD") == base_sha
    assert _git(repo, "status", "--porcelain=v1") == ""


def test_prepare_refuses_a_dirty_base_repository(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "dbt/models/stg_orders.sql").write_text("SELECT 1\n", encoding="utf-8")

    with pytest.raises(WorktreeError, match="must be clean"):
        WorktreeManager(tmp_path / "isolated").prepare(
            repo_id="data-platform",
            base_repo=repo,
            migration_id="ltx-customer-key-001",
        )


def test_prepare_refuses_a_pinned_sha_other_than_checked_out_head(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    original_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "second.txt").write_text("second commit\n", encoding="utf-8")
    _git(repo, "add", "second.txt")
    _git(repo, "commit", "-m", "second")

    with pytest.raises(WorktreeError, match="must match the pinned base_sha"):
        WorktreeManager(tmp_path / "isolated").prepare(
            repo_id="data-platform",
            base_repo=repo,
            migration_id="ltx-customer-key-001",
            base_sha=original_sha,
        )


def test_abort_refuses_to_delete_a_candidate_already_merged(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    sql = session.worktree / "dbt/models/stg_orders.sql"
    sql.write_text(sql.read_text().replace("customer_id", "customer_key"))
    manager.commit_candidate(
        session,
        allowed_paths=("dbt/models/stg_orders.sql",),
        message="candidate",
    )
    _git(repo, "merge", "--ff-only", session.branch)

    with pytest.raises(MergedCandidateError, match="already merged"):
        manager.abort(session)

    assert session.worktree.exists()
    assert _git(repo, "rev-parse", "HEAD") == _git(
        session.worktree, "rev-parse", "HEAD"
    )


def test_detects_any_change_to_base_checkout_during_session(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    (repo / "unexpected.txt").write_text("not owned by LineageTX\n", encoding="utf-8")

    with pytest.raises(BaseRepositoryChanged):
        manager.assert_base_untouched(session)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("filter.attacker.clean", "touch /tmp/lineagetx-filter-clean"),
        ("filter.attacker.smudge", "touch /tmp/lineagetx-filter-smudge"),
        ("filter.attacker.process", "touch /tmp/lineagetx-filter-process"),
        ("core.fsmonitor", "touch /tmp/lineagetx-fsmonitor"),
        ("diff.attacker.textconv", "touch /tmp/lineagetx-textconv"),
        ("diff.external", "touch /tmp/lineagetx-external-diff"),
        ("merge.attacker.driver", "touch /tmp/lineagetx-merge-driver"),
        ("include.path", "/tmp/attacker-controlled-git-config"),
    ],
)
def test_prepare_rejects_repository_local_executable_git_config(
    tmp_path: Path,
    key: str,
    value: str,
) -> None:
    repo = _repository(tmp_path)
    _git(repo, "config", "--local", key, value)

    with pytest.raises(WorktreeError, match="executable settings"):
        WorktreeManager(tmp_path / "isolated").prepare(
            repo_id="data-platform",
            base_repo=repo,
            migration_id="ltx-customer-key-001",
        )


def test_candidate_commit_disables_repository_pre_commit_hooks(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    marker = tmp_path / "hook-executed"
    hook = repo / ".git/hooks/pre-commit"
    hook.write_text(
        "#!/bin/sh\nprintf executed > \"" + str(marker) + "\"\n",
        encoding="utf-8",
    )
    hook.chmod(0o755)
    manager = WorktreeManager(tmp_path / "isolated")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    sql = session.worktree / "dbt/models/stg_orders.sql"
    sql.write_text(sql.read_text().replace("customer_id", "customer_key"))

    manager.commit_candidate(
        session,
        allowed_paths=("dbt/models/stg_orders.sql",),
        message="candidate without hooks",
    )

    assert not marker.exists()


def test_publication_guard_reproves_head_ref_parent_and_exact_diff(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    relative = "dbt/models/stg_orders.sql"
    sql = session.worktree / relative
    sql.write_text(sql.read_text().replace("customer_id", "customer_key"))
    candidate_sha = manager.commit_candidate(
        session,
        allowed_paths=(relative,),
        message="verified candidate",
    )

    assert manager.validate_committed_candidate(
        session,
        expected_commit_sha=candidate_sha,
        allowed_paths=(relative,),
    ) == (relative,)

    # A second clean commit can have the same final allow-listed diff while no
    # longer being a direct child of the pinned base.  Publication must reject it.
    sql.write_text(sql.read_text() + "\n-- unverified follow-up\n")
    _git(session.worktree, "add", relative)
    _git(session.worktree, "commit", "-m", "unverified follow-up")
    second_sha = _git(session.worktree, "rev-parse", "HEAD")
    with pytest.raises(WorktreeError, match="exactly the pinned base SHA"):
        manager.validate_committed_candidate(
            session,
            expected_commit_sha=second_sha,
            allowed_paths=(relative,),
        )


def test_publication_guard_rejects_ref_rewrite_and_committed_path_expansion(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    relative = "dbt/models/stg_orders.sql"
    sql = session.worktree / relative
    sql.write_text(sql.read_text().replace("customer_id", "customer_key"))
    (session.worktree / "unexpected.txt").write_text("not allow-listed\n")
    candidate_sha = manager.commit_candidate(
        session,
        allowed_paths=(relative, "unexpected.txt"),
        message="expanded candidate",
    )
    with pytest.raises(WorktreeError, match="non-allow-listed"):
        manager.validate_committed_candidate(
            session,
            expected_commit_sha=candidate_sha,
            allowed_paths=(relative,),
        )

    _git(session.worktree, "reset", "--hard", session.base_sha)
    with pytest.raises(WorktreeError, match="HEAD no longer matches"):
        manager.validate_committed_candidate(
            session,
            expected_commit_sha=candidate_sha,
            allowed_paths=(relative, "unexpected.txt"),
        )


def test_porcelain_z_validates_both_rename_source_and_destination(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
    )
    source = "dbt/models/stg_orders.sql"
    destination = "dbt/models/stg_orders_renamed.sql"
    _git(session.worktree, "mv", source, destination)

    assert manager.changed_files(session) == tuple(sorted((source, destination)))
    with pytest.raises(WorktreeError, match="non-allow-listed"):
        manager.validate_allowlist(session, (destination,))
    assert manager.validate_allowlist(session, (source, destination)) == tuple(
        sorted((source, destination))
    )


def test_same_repository_can_have_isolated_participant_candidates(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    manager = WorktreeManager(tmp_path / "isolated")
    dbt = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
        candidate_id="consumer-dbt",
    )
    airflow = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id="ltx-customer-key-001",
        candidate_id="consumer-airflow",
    )

    assert dbt.repo_id == airflow.repo_id == "data-platform"
    assert dbt.branch != airflow.branch
    assert dbt.worktree != airflow.worktree
    assert dbt.base_sha == airflow.base_sha == _git(repo, "rev-parse", "HEAD")

    manager.abort(dbt)
    manager.abort(airflow)
    assert _git(repo, "status", "--porcelain=v1") == ""


def test_git_runner_uses_fixed_binary_minimal_environment_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="")

    monkeypatch.setattr(worktrees_module.subprocess, "run", fake_run)
    worktrees_module._run_git(Path("/trusted/repository"), "status")

    command = captured["command"]
    assert isinstance(command, list)
    assert command[0] == worktrees_module._GIT_BINARY
    assert "core.hooksPath=/dev/null" in command
    assert "core.fsmonitor=false" in command
    assert "commit.gpgSign=false" in command
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert set(environment).issubset(
        {
            "PATH",
            "HOME",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "GIT_CONFIG_NOSYSTEM",
            "GIT_TERMINAL_PROMPT",
            "GIT_PAGER",
            "PAGER",
            "GIT_EDITOR",
            "GIT_SEQUENCE_EDITOR",
        }
    )
    assert captured["timeout"] == 20


def test_git_runner_fails_closed_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def timed_out(command: list[str], **_: object):
        raise subprocess.TimeoutExpired(command, 20)

    monkeypatch.setattr(worktrees_module.subprocess, "run", timed_out)
    with pytest.raises(WorktreeError, match="exceeded 20s timeout"):
        worktrees_module._run_git(Path("/trusted/repository"), "status")
