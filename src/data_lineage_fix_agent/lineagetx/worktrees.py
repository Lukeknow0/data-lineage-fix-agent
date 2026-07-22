from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


class WorktreeError(RuntimeError):
    """Raised when an isolated repository session cannot be proven safe."""


class BaseRepositoryChanged(WorktreeError):
    """Raised when the base checkout changed during a LineageTX session."""


class MergedCandidateError(WorktreeError):
    """Raised when ABORT is asked to delete a candidate already merged upstream."""


@dataclass(frozen=True)
class RepositorySession:
    repo_id: str
    candidate_id: str
    migration_id: str
    base_repo: Path
    worktree: Path
    branch: str
    base_sha: str
    base_status: str


_GIT_TIMEOUT_SECONDS = 20
_GIT_BINARY = shutil.which("git", path=os.defpath)
_GIT_SAFE_OPTIONS = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "commit.gpgSign=false",
    "-c",
    "tag.gpgSign=false",
    "-c",
    "gc.auto=0",
)
_EXECUTABLE_CONFIG = re.compile(
    r"^(?:"
    r"filter\..*\.(?:clean|smudge|process)|"
    r"diff\..*\.textconv|diff\.external|"
    r"merge\..*\.driver|"
    r"difftool\..*\.cmd|mergetool\..*\.cmd|"
    r"credential\..*\.helper|"
    r"core\.(?:fsmonitor|hookspath|askpass|sshcommand|editor|pager)|"
    r"sequence\.editor|gpg(?:\..*)?\.program|pager\..*|"
    r"submodule\..*\.update|tar\..*\.command|"
    r"extensions\.worktreeconfig|"
    r"include\..*|includeif\..*"
    r")$",
    re.IGNORECASE,
)


def _minimal_git_environment() -> dict[str, str]:
    environment = {
        "PATH": os.defpath,
        "HOME": "/nonexistent-lineagetx-home",
        "LANG": "C",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_PAGER": "cat",
        "PAGER": "cat",
        "GIT_EDITOR": "true",
        "GIT_SEQUENCE_EDITOR": "true",
    }
    if tmpdir := os.environ.get("TMPDIR"):
        environment["TMPDIR"] = tmpdir
    return environment


def _run_git(
    repo: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    if _GIT_BINARY is None:
        raise WorktreeError("git was not found in the fixed system binary path")
    command = [
        _GIT_BINARY,
        "--no-pager",
        *_GIT_SAFE_OPTIONS,
        "-C",
        str(repo),
        *args,
    ]
    try:
        completed = subprocess.run(
            command,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            env=_minimal_git_environment(),
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise WorktreeError(
            f"git {' '.join(args)} exceeded {_GIT_TIMEOUT_SECONDS}s timeout"
        ) from exc
    except OSError as exc:
        raise WorktreeError(f"unable to execute the fixed git binary: {exc}") from exc
    if check and completed.returncode != 0:
        raise WorktreeError(
            f"git {' '.join(args)} failed in {repo}: {completed.stdout.strip()}"
        )
    return completed


def _reject_executable_local_config(repo: Path) -> None:
    """Reject repository-local settings capable of launching arbitrary programs."""

    completed = _run_git(
        repo,
        "config",
        "--local",
        "--no-includes",
        "--name-only",
        "--null",
        "--list",
        check=False,
    )
    if completed.returncode != 0:
        raise WorktreeError(
            f"unable to audit repository-local git config: {completed.stdout.strip()}"
        )
    dangerous = sorted(
        key
        for key in completed.stdout.split("\0")
        if key and _EXECUTABLE_CONFIG.fullmatch(key)
    )
    if dangerous:
        raise WorktreeError(
            "repository local git config contains executable settings: "
            + ", ".join(dangerous)
        )


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-.").lower()
    if not slug:
        raise WorktreeError("repository and migration identifiers must not be empty")
    return slug[:80]


def _safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise WorktreeError(f"unsafe repository path: {value!r}")
    return path.as_posix()


def git_status_paths(repo: Path) -> tuple[str, ...]:
    """Return every changed path from porcelain v1 -z, including rename pairs."""

    output = _run_git(
        repo,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    ).stdout
    records = output.split("\0")
    paths: list[str] = []
    index = 0
    while index < len(records):
        record = records[index]
        index += 1
        if not record:
            continue
        if len(record) < 4 or record[2] != " ":
            raise WorktreeError("malformed git porcelain record")
        status = record[:2]
        destination_or_path = _safe_relative_path(record[3:])
        paths.append(destination_or_path)
        if "R" in status or "C" in status:
            if index >= len(records) or not records[index]:
                raise WorktreeError("rename/copy porcelain record is missing source path")
            source_path = _safe_relative_path(records[index])
            index += 1
            paths.append(source_path)
    return tuple(sorted(set(paths)))


def git_head_and_branch(repo: Path) -> tuple[str, str]:
    head = _run_git(repo, "rev-parse", "HEAD").stdout.strip()
    branch = _run_git(repo, "symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip()
    return head, branch


class WorktreeManager:
    """Owns temporary LineageTX branches without writing to the base checkout.

    The manager deliberately has no generic command-execution API. Participant
    adapters receive a fixed worktree path and can only modify allow-listed files.
    """

    BRANCH_PREFIX = "lineagetx/"

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def prepare(
        self,
        *,
        repo_id: str,
        base_repo: Path,
        migration_id: str,
        base_sha: str | None = None,
        candidate_id: str | None = None,
    ) -> RepositorySession:
        base_repo = base_repo.resolve()
        top_level = Path(
            _run_git(base_repo, "rev-parse", "--show-toplevel").stdout.strip()
        ).resolve()
        if top_level != base_repo:
            raise WorktreeError(f"base_repo must be the repository root: {top_level}")
        if self.root == base_repo or self.root.is_relative_to(base_repo):
            raise WorktreeError(
                "worktree manager root must be outside the base repository"
            )
        _reject_executable_local_config(base_repo)

        status = _run_git(
            base_repo,
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
        ).stdout
        if status:
            raise WorktreeError("base repository must be clean before PREPARING")

        head = _run_git(base_repo, "rev-parse", "HEAD").stdout.strip()
        requested_sha = base_sha or head
        resolved_sha = _run_git(
            base_repo, "rev-parse", f"{requested_sha}^{{commit}}"
        ).stdout.strip()
        if resolved_sha != head:
            raise WorktreeError(
                "base checkout HEAD must match the pinned base_sha before PREPARING"
            )

        migration_slug = _slug(migration_id)
        candidate_slug = _slug(candidate_id or repo_id)
        branch = f"{self.BRANCH_PREFIX}{migration_slug}/{candidate_slug}"
        destination = (self.root / migration_slug / candidate_slug).resolve()
        if not destination.is_relative_to(self.root):
            raise WorktreeError("computed worktree escaped the manager root")
        if destination.exists():
            raise WorktreeError(f"worktree destination already exists: {destination}")

        if _run_git(
            base_repo,
            "show-ref",
            "--verify",
            "--quiet",
            f"refs/heads/{branch}",
            check=False,
        ).returncode == 0:
            raise WorktreeError(f"candidate branch already exists: {branch}")

        destination.parent.mkdir(parents=True, exist_ok=True)
        _reject_executable_local_config(base_repo)
        _run_git(
            base_repo,
            "worktree",
            "add",
            "-b",
            branch,
            str(destination),
            resolved_sha,
        )
        session = RepositorySession(
            repo_id=repo_id,
            candidate_id=candidate_id or repo_id,
            migration_id=migration_id,
            base_repo=base_repo,
            worktree=destination,
            branch=branch,
            base_sha=resolved_sha,
            base_status=status,
        )
        self.assert_base_untouched(session)
        return session

    def assert_base_untouched(self, session: RepositorySession) -> None:
        current_head = _run_git(session.base_repo, "rev-parse", "HEAD").stdout.strip()
        current_status = _run_git(
            session.base_repo,
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
        ).stdout
        if current_head != session.base_sha or current_status != session.base_status:
            raise BaseRepositoryChanged(
                "base checkout changed; refusing to continue the isolated transaction"
            )

    def changed_files(self, session: RepositorySession) -> tuple[str, ...]:
        self._assert_owned_session(session)
        return git_status_paths(session.worktree)

    def validate_allowlist(
        self,
        session: RepositorySession,
        allowed_paths: Iterable[str],
        *,
        require_all: bool = False,
    ) -> tuple[str, ...]:
        allowed = {_safe_relative_path(path) for path in allowed_paths}
        changed = set(self.changed_files(session))
        unexpected = changed - allowed
        if unexpected:
            raise WorktreeError(
                f"candidate changed non-allow-listed files: {sorted(unexpected)}"
            )
        if require_all and changed != allowed:
            raise WorktreeError(
                f"candidate must change exactly {sorted(allowed)}, got {sorted(changed)}"
            )
        return tuple(sorted(changed))

    def diff(self, session: RepositorySession) -> str:
        self._assert_owned_session(session)
        return _run_git(
            session.worktree,
            "diff",
            "--no-ext-diff",
            "--binary",
            session.base_sha,
            "--",
        ).stdout

    def commit_candidate(
        self,
        session: RepositorySession,
        *,
        allowed_paths: Iterable[str],
        message: str,
    ) -> str:
        _reject_executable_local_config(session.base_repo)
        changed = self.validate_allowlist(session, allowed_paths)
        if not changed:
            raise WorktreeError("cannot commit an empty candidate")
        _run_git(session.worktree, "add", "--", *changed)
        _run_git(session.worktree, "commit", "-m", message)
        candidate_sha = _run_git(session.worktree, "rev-parse", "HEAD").stdout.strip()
        self.assert_base_untouched(session)
        return candidate_sha

    def validate_committed_candidate(
        self,
        session: RepositorySession,
        *,
        expected_commit_sha: str,
        allowed_paths: Iterable[str],
    ) -> tuple[str, ...]:
        """Re-prove an immutable candidate immediately before publication.

        Verification performed while preparing a candidate is not durable proof
        that the branch still names the same commit at publication time.  This
        guard therefore checks the checked-out HEAD, the branch ref, the commit
        object and its sole parent, the clean worktree, and the committed diff.
        """

        self._assert_owned_session(session)
        _reject_executable_local_config(session.base_repo)
        self.assert_base_untouched(session)

        if self.changed_files(session):
            raise WorktreeError("candidate worktree changed after verification")

        head, branch = git_head_and_branch(session.worktree)
        if head != expected_commit_sha:
            raise WorktreeError(
                "candidate worktree HEAD no longer matches the persisted commit"
            )
        if branch != session.branch:
            raise WorktreeError(
                "candidate worktree branch no longer matches the owned branch"
            )

        branch_ref = f"refs/heads/{session.branch}"
        branch_lookup = _run_git(
            session.base_repo,
            "rev-parse",
            "--verify",
            f"{branch_ref}^{{commit}}",
            check=False,
        )
        if (
            branch_lookup.returncode != 0
            or branch_lookup.stdout.strip() != expected_commit_sha
        ):
            raise WorktreeError(
                "candidate branch ref no longer matches the persisted commit"
            )

        commit_lookup = _run_git(
            session.base_repo,
            "rev-parse",
            "--verify",
            f"{expected_commit_sha}^{{commit}}",
            check=False,
        )
        if (
            commit_lookup.returncode != 0
            or commit_lookup.stdout.strip() != expected_commit_sha
        ):
            raise WorktreeError("persisted candidate commit object does not exist")

        ancestry = _run_git(
            session.base_repo,
            "rev-list",
            "--parents",
            "-n",
            "1",
            expected_commit_sha,
        ).stdout.strip().split()
        if ancestry != [expected_commit_sha, session.base_sha]:
            raise WorktreeError(
                "candidate commit must have exactly the pinned base SHA as its parent"
            )

        allowed = {_safe_relative_path(path) for path in allowed_paths}
        diff_output = _run_git(
            session.base_repo,
            "diff",
            "--no-ext-diff",
            "--no-renames",
            "--name-only",
            "-z",
            session.base_sha,
            expected_commit_sha,
            "--",
        ).stdout
        committed_paths = {
            _safe_relative_path(path) for path in diff_output.split("\0") if path
        }
        unexpected = committed_paths - allowed
        if unexpected:
            raise WorktreeError(
                "committed candidate changed non-allow-listed files: "
                f"{sorted(unexpected)}"
            )
        if committed_paths != allowed:
            raise WorktreeError(
                f"committed candidate must change exactly {sorted(allowed)}, "
                f"got {sorted(committed_paths)}"
            )
        return tuple(sorted(committed_paths))

    def abort(self, session: RepositorySession) -> None:
        """Remove only this manager's unmerged LineageTX worktree and branch."""

        self._assert_owned_session(session)
        branch_ref = f"refs/heads/{session.branch}"
        branch_lookup = _run_git(
            session.base_repo, "rev-parse", "--verify", branch_ref, check=False
        )
        if branch_lookup.returncode != 0:
            return
        branch_sha = branch_lookup.stdout.strip()
        if branch_sha != session.base_sha:
            containing_refs = {
                ref
                for ref in _run_git(
                    session.base_repo,
                    "for-each-ref",
                    f"--contains={branch_sha}",
                    "--format=%(refname)",
                    "refs/heads",
                    "refs/remotes",
                ).stdout.splitlines()
                if ref and ref != branch_ref
            }
            if containing_refs:
                raise MergedCandidateError(
                    "candidate is already merged or reachable from another branch; "
                    "ABORT never rolls "
                    f"back merged changes ({sorted(containing_refs)})"
                )
            # The checked-out base HEAD may be detached and therefore absent from
            # for-each-ref; check it explicitly as a final conservative guard.
            base_head = _run_git(session.base_repo, "rev-parse", "HEAD").stdout.strip()
            merged_into_head = _run_git(
                session.base_repo,
                "merge-base",
                "--is-ancestor",
                branch_sha,
                base_head,
                check=False,
            )
            if merged_into_head.returncode == 0:
                raise MergedCandidateError(
                    "candidate is already merged; ABORT never rolls back merged changes"
                )

        registered = self._registered_worktree_branch(session)
        if registered and registered != branch_ref:
            raise WorktreeError("registered worktree branch does not match the session")
        if session.worktree.exists():
            _run_git(
                session.base_repo,
                "worktree",
                "remove",
                "--force",
                str(session.worktree),
            )
        _run_git(session.base_repo, "branch", "-D", session.branch)
        self.assert_base_untouched(session)

    def _registered_worktree_branch(self, session: RepositorySession) -> str | None:
        output = _run_git(session.base_repo, "worktree", "list", "--porcelain").stdout
        current_path: Path | None = None
        current_branch: str | None = None
        for line in [*output.splitlines(), ""]:
            if line.startswith("worktree "):
                current_path = Path(line.removeprefix("worktree ")).resolve()
                current_branch = None
            elif line.startswith("branch "):
                current_branch = line.removeprefix("branch ")
            elif not line and current_path == session.worktree.resolve():
                return current_branch
        return None

    def _assert_owned_session(self, session: RepositorySession) -> None:
        worktree = session.worktree.resolve()
        if not worktree.is_relative_to(self.root):
            raise WorktreeError("session worktree is outside this manager's root")
        if not session.branch.startswith(self.BRANCH_PREFIX):
            raise WorktreeError("refusing to manage a non-LineageTX branch")
        if session.base_repo.resolve() == worktree:
            raise WorktreeError("base checkout cannot be used as a candidate worktree")
