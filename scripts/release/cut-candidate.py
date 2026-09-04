#!/usr/bin/env python3
"""Cut a release-candidate branch: bump the version, commit, push.

Pushing the branch fires Tier 3 (.github/workflows/release.yml), which runs the
full version matrix. When it is green, the maintainer tags X.Y.Z from that
branch's HEAD.

This is auto-bump, NOT auto-release. Nothing here tags, merges, or publishes —
a human reads the Tier 3 result and decides.

Why this exists: the version lives in two places that must agree, and there is
no on-tag workflow checking that they do (spec decision 7). Bumping by hand
means remembering both, and the failure is silent — a package that installs
under a version range it does not claim. This script is the only supported way
to bump, precisely because it writes both together.

Usage:
    uv run scripts/release/cut-candidate.py --patch
    uv run scripts/release/cut-candidate.py --minor
    uv run scripts/release/cut-candidate.py --major
    uv run scripts/release/cut-candidate.py --version 6.3.0

    --dry-run     show what would change, touch nothing
    --no-push     commit locally but do not push (Tier 3 will not fire)

Stdlib only, so it runs under any interpreter without a synced environment.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DBT_PROJECT = REPO_ROOT / "dbt_project.yml"
README = REPO_ROOT / "README.md"

# `version: '6.2.0'` in dbt_project.yml
DBT_VERSION_RE = re.compile(r"""^(version:\s*['"])(\d+\.\d+\.\d+)(['"])$""", re.M)

# `    version: [">=6.2.0", "<6.3.0"]` in the README install snippet.
# The upper bound is the next MINOR, so the range admits patch releases only.
README_VERSION_RE = re.compile(
    r"""(version:\s*\[">=)(\d+\.\d+\.\d+)(",\s*"<)(\d+\.\d+\.\d+)("\])"""
)


def die(msg: str) -> None:
    sys.exit(f"error: {msg}")


def git(*args: str, capture: bool = True) -> str:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=capture,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        die(f"git {' '.join(args)} failed: {detail}")
    return (result.stdout or "").strip()


def read_current_version() -> str:
    match = DBT_VERSION_RE.search(DBT_PROJECT.read_text())
    if not match:
        die(f"could not find a `version:` line in {DBT_PROJECT}")
    return match.group(2)


def bump(version: str, part: str) -> str:
    major, minor, patch = (int(p) for p in version.split("."))
    if part == "major":
        return f"{major + 1}.0.0"
    if part == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def next_minor(version: str) -> str:
    major, minor, _ = (int(p) for p in version.split("."))
    return f"{major}.{minor + 1}.0"


def preflight(target_branch: str) -> None:
    """Refuse to run from a state that would produce a confusing branch.

    Each check exists because the failure is quiet rather than loud: cutting
    from a feature branch, or with uncommitted work, produces a
    release-candidate that looks legitimate and contains something unreviewed.
    """
    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    if branch != "main":
        die(f"must be on main to cut a release candidate (on '{branch}')")

    if git("status", "--porcelain"):
        die("working tree is dirty; commit or stash first")

    # A stale local branch from an abandoned attempt would otherwise be
    # committed onto, mixing old and new work.
    if git("branch", "--list", target_branch):
        die(f"branch '{target_branch}' already exists locally")

    if git("ls-remote", "--heads", "origin", target_branch):
        die(f"branch '{target_branch}' already exists on origin")

    # Not fatal, but cutting from a stale main is a real mistake: the release
    # would silently omit merged work.
    git("fetch", "--quiet", "origin", "main")
    behind = git("rev-list", "--count", "HEAD..origin/main")
    if behind != "0":
        die(f"local main is {behind} commit(s) behind origin/main; pull first")


def rewrite_versions(new_version: str, dry_run: bool) -> list[str]:
    """Update both version locations. Returns a description of each change."""
    changes: list[str] = []

    project_text = DBT_PROJECT.read_text()
    new_project_text, count = DBT_VERSION_RE.subn(
        lambda m: f"{m.group(1)}{new_version}{m.group(3)}", project_text
    )
    if count != 1:
        die(f"expected exactly 1 version line in {DBT_PROJECT.name}, found {count}")
    changes.append(f"{DBT_PROJECT.name}: version -> {new_version}")

    readme_text = README.read_text()
    upper = next_minor(new_version)
    new_readme_text, count = README_VERSION_RE.subn(
        lambda m: f"{m.group(1)}{new_version}{m.group(3)}{upper}{m.group(5)}",
        readme_text,
    )
    if count != 1:
        die(
            f"expected exactly 1 install-snippet version range in {README.name}, "
            f"found {count} — has the Quickstart section changed shape?"
        )
    changes.append(f'{README.name}: version range -> [">={new_version}", "<{upper}"]')

    if not dry_run:
        DBT_PROJECT.write_text(new_project_text)
        README.write_text(new_readme_text)

    return changes


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cut a release-candidate branch.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--major", action="store_const", const="major", dest="part")
    group.add_argument("--minor", action="store_const", const="minor", dest="part")
    group.add_argument("--patch", action="store_const", const="patch", dest="part")
    group.add_argument("--version", help="explicit X.Y.Z, overrides the bump flags")
    parser.add_argument("--dry-run", action="store_true", help="change nothing")
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="commit locally but do not push (Tier 3 will not fire)",
    )
    args = parser.parse_args()

    current = read_current_version()

    if args.version:
        if not re.fullmatch(r"\d+\.\d+\.\d+", args.version):
            die(f"--version must be X.Y.Z, got '{args.version}'")
        new_version = args.version
    else:
        new_version = bump(current, args.part)

    if new_version == current:
        die(f"new version equals current version ({current})")

    target_branch = f"release-candidate/{new_version}"

    print(f"current version : {current}")
    print(f"new version     : {new_version}")
    print(f"branch          : {target_branch}")
    print()

    if args.dry_run:
        for change in rewrite_versions(new_version, dry_run=True):
            print(f"  would write  {change}")
        print("\ndry run: nothing written, no branch created")
        return

    preflight(target_branch)

    git("checkout", "-b", target_branch)
    try:
        for change in rewrite_versions(new_version, dry_run=False):
            print(f"  wrote  {change}")
        git("add", str(DBT_PROJECT), str(README))
        git("commit", "-m", f"release candidate {new_version}")
    except SystemExit:
        # Leave the tree inspectable, but do not strand the maintainer on a
        # half-built branch they did not ask to be on.
        git("checkout", "main")
        git("branch", "-D", target_branch)
        raise

    if args.no_push:
        print(f"\ncommitted on {target_branch}; not pushed (--no-push)")
        print("Tier 3 will not run until you push.")
        return

    git("push", "--set-upstream", "origin", target_branch)

    print(f"\npushed {target_branch}")
    print("Tier 3 (release.yml) is now running the full version matrix.")
    print("When it is green, tag from this branch's HEAD:")
    print(f"    git tag {new_version} && git push origin {new_version}")


if __name__ == "__main__":
    main()
