"""Synthetic wind turbine telemetry generator.

Two modes:

* ``--out DIR``      write files to a local directory (inspect them, or upload later)
* ``--volume PATH``  write straight into a Unity Catalog Volume path, e.g.
                     ``/Volumes/wind_dev/raw/landing/sensors`` -- works when run on a
                     Databricks cluster or as a job task.

Drip mode (``--interval`` with ``--duration``) is what makes the continuous pipeline
visibly real-time: each tick writes one file and rows appear in Unity Catalog seconds later.

A configurable fraction of readings are deliberately malformed so the quarantine table is
exercised rather than sitting permanently empty.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import random
import sys
import time
import uuid

TAGS = ("gear_friction", "oil_temperature", "blade_velocity")

UNITS = {
    "gear_friction": "coef",
    "oil_temperature": "celsius",
    "blade_velocity": "rpm",
}

# (centre, swing) per tag -- the generator oscillates around the centre so charts have
# visible structure rather than white noise.
PROFILE = {
    "gear_friction": (0.40, 0.08),
    "oil_temperature": (72.0, 9.0),
    "blade_velocity": (13.0, 4.0),
}

SITES = ("SITE-NORTH", "SITE-SOUTH", "SITE-EAST", "SITE-WEST")

MODELS = (
    ("V150-4.2", "Vestas", 4200, 105, 150),
    ("SG-6.0-170", "Siemens Gamesa", 6000, 115, 170),
    ("N163-5.X", "Nordex", 5700, 118, 163),
    ("GE-5.5-158", "GE Vernova", 5500, 101, 158),
)


def _iso(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def turbine_ids(count: int) -> list[str]:
    return [f"WTG-{i:04d}" for i in range(1, count + 1)]


def site_for(turbine_id: str) -> str:
    return SITES[hash(turbine_id) % len(SITES)]


def make_reading(tag: str, phase: float, bad: bool) -> dict:
    centre, swing = PROFILE[tag]
    value = centre + swing * math.sin(phase) + random.gauss(0, swing * 0.15)

    quality = "GOOD"
    if random.random() < 0.04:
        quality = "SUSPECT"

    if bad:
        # Three distinct corruption modes, so violation_reasons shows variety.
        mode = random.choice(("null_value", "bad_quality", "stuck_high"))
        if mode == "null_value":
            value = None
        elif mode == "bad_quality":
            quality = "BROKEN"
        else:
            # A stuck or unscaled sensor reporting a physically impossible magnitude.
            # Deliberately a large *finite* number rather than Infinity: bare `Infinity`
            # is not valid JSON, and relying on a parser extension to read it would make
            # the fixture depend on reader options rather than on the quality rules.
            value = centre * 1000.0

    return {"value": value, "unit": UNITS[tag], "quality": quality}


def make_payload(turbine_id: str, seq: int, now: dt.datetime, bad_rate: float) -> dict:
    phase = seq / 8.0 + hash(turbine_id) % 7
    return {
        "turbine_id": turbine_id,
        "site_id": site_for(turbine_id),
        "event_ts": _iso(now),
        "seq": seq,
        "readings": {
            tag: make_reading(tag, phase, bad=random.random() < bad_rate) for tag in TAGS
        },
    }


def make_turbine_records(ids: list[str], change_seq_start: int = 1) -> list[dict]:
    """Dimension rows. Called twice by --with-dimension to produce an SCD2 history."""
    records = []
    now = dt.datetime.now(dt.timezone.utc)
    for i, tid in enumerate(ids):
        model, maker, power, hub, rotor = MODELS[i % len(MODELS)]
        records.append(
            {
                "turbine_id": tid,
                "site_id": site_for(tid),
                "model": model,
                "manufacturer": maker,
                "rated_power_kw": power,
                "hub_height_m": hub,
                "rotor_diameter_m": rotor,
                "commissioned_on": (
                    dt.date(2019, 1, 1) + dt.timedelta(days=(i * 37) % 2200)
                ).isoformat(),
                "operation": "UPSERT",
                "change_seq": change_seq_start,
                "changed_at": _iso(now),
            }
        )
    return records


def write_jsonl(records: list[dict], directory: str, prefix: str) -> str:
    os.makedirs(directory, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    path = os.path.join(directory, f"{prefix}-{stamp}-{uuid.uuid4().hex[:8]}.json")
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    dest = parser.add_mutually_exclusive_group(required=True)
    dest.add_argument("--out", help="local output directory")
    dest.add_argument("--volume", help="UC Volume path, e.g. /Volumes/cat/raw/landing/sensors")

    parser.add_argument("--turbines", type=int, default=10)
    parser.add_argument("--batches", type=int, default=1, help="files to write (non-drip mode)")
    parser.add_argument(
        "--interval", type=float, default=0.0, help="seconds between files; enables drip mode"
    )
    parser.add_argument("--duration", type=float, default=0.0, help="drip mode total seconds")
    parser.add_argument(
        "--bad-rate",
        type=float,
        default=0.05,
        help="fraction of readings deliberately corrupted, to exercise quarantine",
    )
    parser.add_argument(
        "--with-dimension",
        metavar="DIR_OR_VOLUME",
        help="also write turbine dimension CDC files here (two generations, to create "
        "SCD2 history)",
    )
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    if args.seed is not None:
        random.seed(args.seed)

    target = args.out or args.volume
    ids = turbine_ids(args.turbines)

    if args.with_dimension:
        first = make_turbine_records(ids, change_seq_start=1)
        path = write_jsonl(first, args.with_dimension, "turbines")
        print(f"dimension v1: {len(first)} rows -> {path}")

        # A second generation with changed attributes, so dim_turbine has real history
        # to prove the SCD2 as-of join works.
        second = make_turbine_records(ids, change_seq_start=2)
        for record in second[: max(1, len(second) // 3)]:
            record["rated_power_kw"] += 200
            record["site_id"] = "SITE-REASSIGNED"
        path = write_jsonl(second, args.with_dimension, "turbines")
        print(f"dimension v2: {len(second)} rows -> {path}")

    seq = 0
    written = 0

    def emit_one_file() -> None:
        nonlocal seq, written
        now = dt.datetime.now(dt.timezone.utc)
        payloads = [make_payload(tid, seq, now, args.bad_rate) for tid in ids]
        path = write_jsonl(payloads, target, "sensors")
        written += 1
        print(f"[{written}] {len(payloads)} payloads -> {path}", flush=True)
        seq += 1

    if args.interval > 0:
        deadline = time.monotonic() + (args.duration or 60.0)
        print(
            f"drip mode: every {args.interval}s for {args.duration or 60.0}s "
            f"into {target}",
            flush=True,
        )
        while time.monotonic() < deadline:
            emit_one_file()
            time.sleep(args.interval)
    else:
        for _ in range(args.batches):
            emit_one_file()

    print(f"done: {written} file(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
