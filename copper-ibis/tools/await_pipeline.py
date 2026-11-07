"""Drive a bundle's pipeline to a settled state and report honestly on the result.

``databricks bundle run`` is not usable for this in CI, for two reasons found the hard
way:

1. ``bundle deploy`` starts an update of its own, so an immediately following ``run``
   fails with *"An active update already exists"*.
2. A pipeline reaching ``IDLE`` does **not** mean the update succeeded -- a FAILED update
   also leaves the pipeline idle. The update's own state is the thing to assert on.

This script waits for any in-flight update, optionally triggers a fresh one, polls to a
terminal state, and exits non-zero unless that state is COMPLETED. On failure it prints
the error events, which is what you actually need from a CI log.

    python tools/await_pipeline.py --target dev --trigger
    python tools/await_pipeline.py --target dev --errors-only
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

TERMINAL_UPDATE_STATES = {"COMPLETED", "FAILED", "CANCELED"}
SUCCESS_UPDATE_STATE = "COMPLETED"


def cli_path() -> str:
    return os.environ.get("DATABRICKS_CLI", "databricks")


def run_cli(args: list[str], profile: str | None) -> str:
    """Invoke the Databricks CLI and return stdout, raising on failure."""
    command = [cli_path(), *args]
    if profile:
        command += ["--profile", profile]

    env = dict(os.environ)
    # Git Bash on Windows rewrites a leading "/api/..." into a filesystem path, which
    # turns every API call into a 404.
    env["MSYS_NO_PATHCONV"] = "1"

    completed = subprocess.run(command, capture_output=True, text=True, env=env)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout


def parse_json(text: str):
    start = min((i for i in (text.find("{"), text.find("[")) if i >= 0), default=-1)
    if start < 0:
        raise ValueError(f"no JSON in CLI output: {text[:200]}")
    return json.loads(text[start:])


def pipeline_id_from_bundle(target: str, profile: str | None, resource: str) -> str:
    """Read the deployed pipeline's id out of the bundle summary."""
    summary = parse_json(run_cli(["bundle", "summary", "--target", target, "-o", "json"], profile))
    pipelines = (summary.get("resources") or {}).get("pipelines") or {}
    entry = pipelines.get(resource)
    if not entry or not entry.get("id"):
        available = ", ".join(pipelines) or "none"
        raise SystemExit(
            f"pipeline '{resource}' not found in the {target} bundle (found: {available}). "
            "Has it been deployed?"
        )
    return entry["id"]


def get_pipeline(pipeline_id: str, profile: str | None) -> dict:
    return parse_json(run_cli(["api", "get", f"/api/2.0/pipelines/{pipeline_id}"], profile))


def latest_update(pipeline: dict) -> dict:
    updates = pipeline.get("latest_updates") or []
    return updates[0] if updates else {}


def wait_for_settled(pipeline_id: str, profile: str | None, timeout: int, poll: int) -> dict:
    """Block until the newest update reaches a terminal state, and return it."""
    deadline = time.monotonic() + timeout
    last_reported = None

    while time.monotonic() < deadline:
        update = latest_update(get_pipeline(pipeline_id, profile))
        state = update.get("state")

        if state != last_reported:
            print(f"  update {str(update.get('update_id'))[:8]}: {state}", flush=True)
            last_reported = state

        if state in TERMINAL_UPDATE_STATES:
            return update
        time.sleep(poll)

    raise SystemExit(f"timed out after {timeout}s waiting for the pipeline to settle")


def trigger_update(pipeline_id: str, profile: str | None, full_refresh: bool) -> None:
    body = json.dumps({"full_refresh": full_refresh})
    run_cli(["api", "post", f"/api/2.0/pipelines/{pipeline_id}/updates", "--json", body], profile)


def print_errors(pipeline_id: str, profile: str | None, limit: int = 100) -> None:
    """Print the error events and exception messages for the most recent run."""
    path = f"/api/2.0/pipelines/{pipeline_id}/events?max_results={limit}"
    events = parse_json(run_cli(["api", "get", path], profile)).get("events", [])

    printed = False
    for event in events:
        if event.get("level") == "ERROR":
            summary = " ".join(event.get("message", "").split())
            print(f"\nERROR {event.get('event_type')}: {summary}")
            printed = True
        for exception in (event.get("error") or {}).get("exceptions", []):
            print(f"\n  {exception.get('class_name')}")
            message = exception.get("message", "")
            for line in message.splitlines()[:12]:
                print(f"    {line}")
            printed = True

    if not printed:
        print("no error events found in the most recent events window")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="dev", help="bundle target")
    parser.add_argument("--resource", default="turbine_pipeline", help="bundle pipeline key")
    parser.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    parser.add_argument(
        "--trigger",
        action="store_true",
        help="start a fresh update after any in-flight one settles",
    )
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument(
        "--errors-only",
        action="store_true",
        help="just print error events for the latest run, then exit 0",
    )
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--poll", type=int, default=15)
    args = parser.parse_args(argv)

    pipeline_id = pipeline_id_from_bundle(args.target, args.profile, args.resource)
    print(f"pipeline {pipeline_id} (target {args.target})")

    if args.errors_only:
        print_errors(pipeline_id, args.profile)
        return 0

    # `bundle deploy` starts its own update; let it finish before starting another.
    print("waiting for any in-flight update to settle...")
    settled = wait_for_settled(pipeline_id, args.profile, args.timeout, args.poll)

    if args.trigger:
        print("triggering a fresh update...")
        trigger_update(pipeline_id, args.profile, args.full_refresh)
        # Give the API a moment to register the new update before polling, or the first
        # poll can still see the previous terminal state.
        time.sleep(args.poll)
        settled = wait_for_settled(pipeline_id, args.profile, args.timeout, args.poll)

    state = settled.get("state")
    # IDLE on the pipeline is not success -- only the update's own state tells you that.
    if state != SUCCESS_UPDATE_STATE:
        print(f"\nupdate finished as {state}, expected {SUCCESS_UPDATE_STATE}")
        print_errors(pipeline_id, args.profile)
        return 1

    print(f"\nupdate {SUCCESS_UPDATE_STATE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
