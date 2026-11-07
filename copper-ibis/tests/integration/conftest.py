"""Fixtures for tests that run against a live Databricks workspace.

These require credentials and a deployed `dev` bundle. They are selected by the
`integration_test` marker and skipped automatically when the environment is not
configured, so a plain `pytest` on a laptop does not fail with auth errors.
"""

from __future__ import annotations

import os

import pytest


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value.strip() if isinstance(value, str) else value


@pytest.fixture(scope="session")
def catalog() -> str:
    return _env("DATABRICKS_CATALOG", "wind_dev")


@pytest.fixture(scope="session")
def workspace():
    """Databricks SDK client, authenticated from the ambient environment.

    In CI that is OIDC (`DATABRICKS_AUTH_TYPE=github-oidc`); locally it is a profile or
    `DATABRICKS_TOKEN`. The SDK's default credential chain handles both, so nothing about
    auth is hardcoded here.
    """
    if not _env("DATABRICKS_HOST"):
        pytest.skip("DATABRICKS_HOST is not set; skipping integration tests")

    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


@pytest.fixture(scope="session")
def spark(workspace):
    """A Spark session backed by the remote workspace via Databricks Connect."""
    try:
        from databricks.connect import DatabricksSession
    except ImportError:  # pragma: no cover - depends on which extra is installed
        pytest.skip("databricks-connect is not installed; install the `integration` extra")

    serverless = _env("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
    cluster_id = _env("DATABRICKS_CLUSTER_ID")

    builder = DatabricksSession.builder
    if cluster_id:
        session = builder.clusterId(cluster_id).getOrCreate()
    else:
        session = builder.serverless(serverless == "auto" or bool(serverless)).getOrCreate()

    yield session
    session.stop()
