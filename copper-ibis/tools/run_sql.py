"""Run SQL statements against a Databricks SQL warehouse via the Statement Execution API.

A convenience wrapper used during setup and manual verification. It shells out to the
Databricks CLI so it inherits whatever authentication the CLI profile already has -- no
credentials are read or stored here.

    python tools/run_sql.py --profile wind --warehouse <id> "SELECT 1"
    python tools/run_sql.py --profile wind --warehouse <id> --file statements.sql

Statements in a file are split on semicolons and run in order, stopping at the first
failure.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

DEFAULT_CLI = os.path.expanduser("~/bin/databricks.exe")


def run_statement(cli: str, profile: str, warehouse: str, statement: str, timeout: str = "50s"):
    """Execute one statement, returning the parsed API response."""
    payload = {
        "warehouse_id": warehouse,
        "statement": statement,
        "wait_timeout": timeout,
    }

    # The CLI reads the body from a file to avoid shell quoting problems with embedded
    # quotes in SQL.
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8"
    ) as handle:
        json.dump(payload, handle)
        body_path = handle.name

    try:
        env = dict(os.environ)
        # Git Bash on Windows rewrites a leading "/api/..." into a filesystem path,
        # which turns every call into a 404.
        env["MSYS_NO_PATHCONV"] = "1"

        completed = subprocess.run(
            [
                cli, "api", "post", "/api/2.0/sql/statements",
                "--profile", profile,
                "--json", f"@{body_path.replace(os.sep, '/')}",
            ],
            capture_output=True,
            text=True,
            env=env,
        )
    finally:
        os.unlink(body_path)

    if completed.returncode != 0:
        return {"status": {"state": "CLI_ERROR", "error": {"message": completed.stderr.strip()}}}

    # The CLI prints a banner on some installs; find where the JSON actually starts.
    stdout = completed.stdout
    brace = stdout.find("{")
    if brace == -1:
        return {"status": {"state": "NO_JSON", "error": {"message": stdout.strip()[:500]}}}
    return json.loads(stdout[brace:])


def substitute(text: str, values: dict[str, str]) -> str:
    """Replace ``${key}`` placeholders, failing loudly on any that are left over.

    An unsubstituted placeholder would otherwise reach Databricks as literal SQL and
    produce a confusing parse error far from the real cause -- a forgotten ``--set``.
    """
    for key, value in values.items():
        text = text.replace("${" + key + "}", value)

    leftover = sorted(set(re.findall(r"\$\{(\w+)\}", text)))
    if leftover:
        raise SystemExit(
            "missing substitutions for: "
            + ", ".join(leftover)
            + "\nPass them with --set KEY=VALUE"
        )
    return text


def split_statements(text: str) -> list[str]:
    """Split a script on statement boundaries.

    A plain ``text.split(";")`` breaks as soon as a semicolon appears inside a quoted
    string or a comment, so this tracks quoting and comment state. Comment-only
    fragments are dropped -- the API rejects a statement with no SQL in it.
    """
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    in_line_comment = False
    in_block_comment = False

    index = 0
    while index < len(text):
        char = text[index]
        pair = text[index : index + 2]

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                current.append(char)
            index += 1
            continue

        if in_block_comment:
            if pair == "*/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        if quote:
            current.append(char)
            # Doubled quote is an escaped quote, not a terminator.
            if char == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    current.append(text[index + 1])
                    index += 2
                    continue
                quote = None
            index += 1
            continue

        if pair == "--":
            in_line_comment = True
            index += 2
            continue
        if pair == "/*":
            in_block_comment = True
            index += 2
            continue
        if char in ("'", '"', "`"):
            quote = char
            current.append(char)
            index += 1
            continue
        if char == ";":
            statements.append("".join(current))
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def describe(response: dict) -> tuple[str, str]:
    status = response.get("status", {})
    state = status.get("state", "UNKNOWN")
    message = ""
    if status.get("error"):
        message = status["error"].get("message", "")
    return state, message


def print_result(response: dict) -> None:
    """Print a result set as simple aligned columns, if there is one."""
    manifest = response.get("manifest") or {}
    result = response.get("result") or {}
    rows = result.get("data_array")
    if not rows:
        return
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    if columns:
        print("  " + " | ".join(columns))
        print("  " + "-+-".join("-" * len(c) for c in columns))
    for row in rows:
        print("  " + " | ".join("NULL" if v is None else str(v) for v in row))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("statement", nargs="?", help="SQL to run")
    parser.add_argument("--file", help="file of semicolon-separated statements")
    parser.add_argument("--profile", default="wind")
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--cli", default=DEFAULT_CLI)
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="substitute ${KEY} in the SQL, e.g. --set catalog=wind_prod (repeatable)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="keep going after a failed statement instead of stopping",
    )
    args = parser.parse_args(argv)

    substitutions = {}
    for item in args.set:
        if "=" not in item:
            parser.error(f"--set expects KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        substitutions[key.strip()] = value.strip()

    if args.file:
        statements = split_statements(
            substitute(open(args.file, encoding="utf-8").read(), substitutions)
        )
    elif args.statement:
        statements = [args.statement]
    else:
        parser.error("provide a statement or --file")

    failures = 0
    for statement in statements:
        preview = " ".join(statement.split())[:100]
        response = run_statement(args.cli, args.profile, args.warehouse, statement)
        state, message = describe(response)

        if state == "SUCCEEDED":
            print(f"OK    {preview}")
            print_result(response)
        else:
            failures += 1
            print(f"{state}  {preview}")
            if message:
                print(f"      {message}")
            if not args.continue_on_error:
                return 1

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
