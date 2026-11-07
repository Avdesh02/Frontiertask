"""Fixtures for tests that run against a live Databricks workspace.

Skipping policy, which matters more than it looks: these tests skip when the environment
is *deliberately* not configured (a laptop with no credentials), but **fail** when the
environment claims to be configured and something is wrong. Skipping on a missing
dependency or a broken connection would let CI report success while asserting nothing --
which is exactly what happened before ``REQUIRE_INTEGRATION_TESTS`` existed.

CI sets ``REQUIRE_INTEGRATION_TESTS=1`` so a skip there is a hard error.
"""

from __future__ import annotations

import os

import pytest


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else value


def _required() -> bool:
    """Whether a skip should be escalated to a failure."""
    return _env("REQUIRE_INTEGRATION_TESTS", "").lower() in ("1", "true", "yes")


def _unavailable(reason: str):
    """Skip locally, fail loudly where these tests are mandatory."""
    if _required():
        pytest.fail(
            f"{reason}\n"
            "REQUIRE_INTEGRATION_TESTS is set, so this is a failure rather than a skip -- "
            "a silently skipped suite reports success while asserting nothing."
        )
    pytest.skip(reason)


@pytest.fixture(scope="session")
def catalog() -> str:
    return _env("DATABRICKS_CATALOG", "wind_dev")


@pytest.fixture(scope="session")
def workspace():
    """Databricks SDK client, authenticated from the ambient environment.

    Auth comes from the SDK's default credential chain -- a PAT in DATABRICKS_TOKEN, a
    CLI profile, or OIDC -- so nothing about authentication is hardcoded here.
    """
    if not _env("DATABRICKS_HOST"):
        _unavailable("DATABRICKS_HOST is not set")

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        _unavailable(f"databricks-sdk is not installed: {exc}")

    return WorkspaceClient()


@pytest.fixture(scope="session")
def spark(workspace):
    """A Spark session backed by the remote workspace via Databricks Connect."""
    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        _unavailable(
            f"databricks-connect is not installed ({exc}); install the `integration` extra"
        )

    cluster_id = _env("DATABRICKS_CLUSTER_ID")
    try:
        if cluster_id:
            session = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
        else:
            # Serverless compute: no cluster to provision or keep warm.
            session = DatabricksSession.builder.serverless(True).getOrCreate()
    except Exception as exc:
        _unavailable(f"could not open a Databricks Connect session: {exc}")

    yield session
    session.stop()
