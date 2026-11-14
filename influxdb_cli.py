#!/usr/bin/env python3
"""
influxdb_signals.py — list the signals (entities) of an InfluxDB v2 bucket
and plot them as time series.

A signal is identified by the 'entity_id' tag (the Home Assistant schema);
data series without that tag fall back to the measurement name.

Dependencies:
    pip install influxdb-client pandas matplotlib

Connection settings are resolved in this order (first match wins):

    1. command-line flags: --url, --token, --org, --bucket
    2. environment variables: INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG,
       INFLUXDB_BUCKET
    3. an env file with KEY=VALUE lines (see below)

The env file is looked up at ./env, then next to this script; use --env-file
to point at another location. Recognised keys (case-insensitive):

    URL or INFLUXDB_URL or SERVER         e.g. https://influx.example.com:8086
    TOKEN or INFLUXDB_TOKEN               API token with read access to the bucket
    ORG or INFLUXDB_ORG or ORG_NAME       organisation name
    BUCKET or INFLUXDB_BUCKET or BUCKET_NAME  bucket to inspect

Examples:

    # List every signal in the bucket: fields, types, size, time range
    python influxdb_signals.py --url https://influx.example.com:8086 \
        --token "$TOKEN" --org my-org --bucket sensors --range all --list

    # Plot two signals (all of their numeric fields), last 7 days, save to a file
    python influxdb_signals.py --url ... --token "$TOKEN" --org my-org \
        --bucket sensors --plot kitchen_temperature,living_humidity \
        --range -7d --output plot.png

    # Plot a single field of a single signal
    python influxdb_signals.py ... --plot kitchen_temperature:value --output temp.png

    # Plot with interpolated lines instead of stairs
    python influxdb_signals.py ... --plot kitchen_temperature --style line --output temp.png

Signal names are entity ids (e.g. kitchen_temperature, living_humidity);
run with --list to see the signals available in the bucket.
"""

import argparse
import os
import re
import sys
from datetime import datetime

import pandas as pd
import urllib3
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException

DURATION_RE = re.compile(r"^-?(\d+[smhdwMy])+$")


class InfluxError(Exception):
    """Fatal error reported to the user without a traceback."""


# ---------------------------------------------------------------------------
# Low-level API helpers
# ---------------------------------------------------------------------------

def flux_str(value):
    """Quote a string for use in a Flux query."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def flux_start(range_arg):
    """Translate the --range option into a Flux range() start argument."""
    if range_arg in ("all", "0"):
        return "0"
    if not DURATION_RE.match(range_arg):
        raise InfluxError(
            f"invalid --range {range_arg!r} "
            "(use a Flux duration like -30m, -24h, -7d, or 'all')"
        )
    return range_arg


def api_error(exc, url):
    """Translate a client exception into a user-facing InfluxError."""
    if isinstance(exc, ApiException):
        if exc.status in (401, 403):
            return InfluxError(
                f"authentication failed (HTTP {exc.status}) — check your token"
            )
        message = exc.reason or str(exc)
        body = getattr(exc, "body", None)
        if body:
            message = f"{message}: {body}"
        return InfluxError(f"HTTP {exc.status} from {url}: {message}")
    return InfluxError(f"cannot reach InfluxDB at {url}: {exc}")


def run_query(client, org, query):
    """Run a Flux query and return the result rows as a list of typed dicts."""
    try:
        tables = client.query_api().query(query=query, org=org)
    except ApiException as exc:
        raise api_error(exc, client.url)
    except urllib3.exceptions.HTTPError as exc:
        raise api_error(exc, client.url)
    return [dict(record.values) for table in tables for record in table.records]


def check_server(client):
    try:
        health = client.health()
    except ApiException as exc:
        if exc.status in (401, 403):
            # /health needs no auth; another status means the server answered
            return
        raise api_error(exc, client.url)
    except urllib3.exceptions.HTTPError as exc:
        raise api_error(exc, client.url)
    if health.status != "pass":
        raise InfluxError(f"server at {client.url} is not healthy: {health.message}")


def check_bucket(client, org, bucket):
    try:
        found = client.buckets_api().find_bucket_by_name(bucket)
    except ApiException as exc:
        raise api_error(exc, client.url)
    except urllib3.exceptions.HTTPError as exc:
        raise api_error(exc, client.url)
    if found is None:
        raise InfluxError(f"bucket {bucket!r} not found in organisation {org!r}")


# ---------------------------------------------------------------------------
# Flux queries
# ---------------------------------------------------------------------------

# How the signal (entity) of a record is derived: the 'entity_id' tag for
# Home Assistant data, falling back to the measurement name. Grouping by
# the tag itself (rather than a computed column) keeps the storage-level
# pushdown of count()/min()/max() and makes listing an order of magnitude
# faster; series without the tag are grouped by measurement instead.


def fields_query(bucket, start):
    """One row per (signal, field) with a sample value -> field types."""
    return f"""from(bucket: {flux_str(bucket)})
  |> range(start: {start})
  |> group(columns: ["entity_id", "_measurement", "_field"])
  |> limit(n: 1)
  |> keep(columns: ["entity_id", "_measurement", "_field", "_value"])"""


def counts_query(bucket, start):
    """Number of stored points per (signal, field)."""
    return f"""from(bucket: {flux_str(bucket)})
  |> range(start: {start})
  |> group(columns: ["entity_id", "_measurement", "_field"])
  |> count()
  |> keep(columns: ["entity_id", "_measurement", "_field", "_value"])"""


def time_bounds_query(bucket, start, fn):
    """Earliest ('min') or latest ('max') timestamp per signal."""
    return f"""from(bucket: {flux_str(bucket)})
  |> range(start: {start})
  |> drop(columns: ["_value"])
  |> group(columns: ["entity_id", "_measurement"])
  |> {fn}(column: "_time")
  |> rename(columns: {{_time: "{fn}_seen"}})
  |> keep(columns: ["entity_id", "_measurement", "{fn}_seen"])"""


def plot_query(bucket, start, name, field, limit):
    """Time series of one signal (or one of its fields), pivoted wide.

    'name' is an entity_id, or a measurement name for data without the tag.
    """
    signal = (
        f"(exists r.entity_id and r.entity_id == {flux_str(name)})"
        f" or ((not exists r.entity_id) and r._measurement == {flux_str(name)})"
    )
    if field:
        predicate = f"({signal}) and r._field == {flux_str(field)}"
    else:
        predicate = signal
    return f"""from(bucket: {flux_str(bucket)})
  |> range(start: {start})
  |> filter(fn: (r) => {predicate})
  |> limit(n: {limit})
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"])"""


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------

def type_name(value):
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "time"
    if isinstance(value, str):
        return "string"
    return "unknown"


def fmt_time(value):
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def signal_name(row):
    """The signal (entity) name of a result row: entity_id tag, else measurement."""
    return row.get("entity_id") or row.get("_measurement")


def list_signals(client, org, bucket, start, range_label):
    field_rows = run_query(client, org, fields_query(bucket, start))
    if not field_rows:
        print(f"No data found in bucket {bucket!r} for range {range_label}.")
        return
    count_rows = run_query(client, org, counts_query(bucket, start))
    first_rows = run_query(client, org, time_bounds_query(bucket, start, "min"))
    last_rows = run_query(client, org, time_bounds_query(bucket, start, "max"))

    signals = {}
    for row in field_rows:
        name = signal_name(row)
        signals.setdefault(name, {"fields": {}, "points": 0})
        signals[name]["fields"].setdefault(
            row.get("_field"), type_name(row.get("_value"))
        )
    for row in count_rows:
        name = signal_name(row)
        if name in signals:
            signals[name]["points"] += int(row.get("_value") or 0)
    first_seen = {}
    for row in first_rows:
        name, when = signal_name(row), row.get("min_seen")
        if when and (name not in first_seen or when < first_seen[name]):
            first_seen[name] = when
    last_seen = {}
    for row in last_rows:
        name, when = signal_name(row), row.get("max_seen")
        if when and (name not in last_seen or when > last_seen[name]):
            last_seen[name] = when

    table = pd.DataFrame(
        [
            {
                "signal": name,
                "fields": ", ".join(info["fields"]),
                "types": ", ".join(info["fields"].values()),
                "points": f"{info['points']}",
                "first seen (UTC)": fmt_time(first_seen.get(name)),
                "last seen (UTC)": fmt_time(last_seen.get(name)),
            }
            for name, info in sorted(signals.items())
        ]
    )
    print(f"Bucket {bucket!r} — {len(table)} signal(s), range: {range_label}\n")
    print(table.to_string(index=False))


def plot_signals(client, org, bucket, selectors, start, limit, output, range_label,
                 style):
    import matplotlib

    if output:
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    frames = []
    for selector in selectors:
        if ":" in selector:
            name, field = selector.split(":", 1)
        else:
            name, field = selector, None
        rows = run_query(
            client, org, plot_query(bucket, start, name, field, limit)
        )
        if not rows:
            print(
                f"warning: no data for signal {selector!r} in range {range_label} "
                "(run with --list to see the available signals)",
                file=sys.stderr,
            )
            continue
        df = pd.DataFrame(rows)
        df["_time"] = pd.to_datetime(df["_time"], utc=True)
        df = df.set_index("_time").sort_index()
        meta = {"result", "table", "_start", "_stop", "_measurement", "_field"}
        value_cols = [c for c in df.columns if c not in meta]
        numeric = df[value_cols].select_dtypes("number")
        if numeric.empty:
            print(
                f"warning: signal {selector!r} has no numeric field to plot",
                file=sys.stderr,
            )
            continue
        numeric.columns = [f"{selector}.{c}" for c in numeric.columns]
        frames.append(numeric)

    if not frames:
        raise InfluxError("nothing to plot: no numeric data for the selected signal(s)")

    data = pd.concat(frames, axis=1).sort_index()
    # make column names unique in case two selectors overlap
    seen = {}
    unique_cols = []
    for col in data.columns:
        if col in seen:
            seen[col] += 1
            unique_cols.append(f"{col} #{seen[col]}")
        else:
            seen[col] = 0
            unique_cols.append(col)
    data.columns = unique_cols

    fig, ax = plt.subplots(figsize=(12, 6))
    drawstyle = "steps-post" if style == "stairs" else "default"
    for col in data.columns:
        # The merged index is the union of every signal's timestamps, so each
        # column is NaN wherever its signal had no sample; matplotlib would
        # break the line at those points. Plot only that signal's own points
        # (each frame was sorted by time before the merge, so order holds).
        series = data[col].dropna()
        ax.plot(series.index, series, drawstyle=drawstyle, linewidth=1.2,
                label=col)
    ax.set_title(f"InfluxDB bucket {bucket!r} — {range_label}")
    ax.set_xlabel("time (UTC)")
    ax.set_ylabel("value")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize="small", loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()

    if output:
        fig.savefig(output, dpi=150)
        print(f"Plot saved to {output}")
    else:
        plt.show()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def find_env_file():
    """Locate the default env file: ./env, then env next to this script."""
    candidates = (
        os.path.join(os.getcwd(), "env"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "env"),
    )
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def load_env_file(path):
    """Parse a shell-style KEY=VALUE file into a dict with uppercase keys.

    Blank lines and '#' comments are ignored, an optional 'export ' prefix is
    accepted, and surrounding single or double quotes are stripped.
    """
    values = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            key, sep, value = line.partition("=")
            if not sep:
                continue
            key = key.strip().upper()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            values[key] = value
    return values


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="List the signals (entities) of an InfluxDB v2 bucket "
        "and plot them as time series.",
        epilog="examples:\n"
        "  %(prog)s --url https://influx.example.com:8086 --token $TOKEN "
        "--org my-org --bucket sensors --list\n"
        "  %(prog)s ... --plot kitchen_temperature,living_humidity "
        "--range -7d --output plot.png\n"
        "  %(prog)s ... --plot kitchen_temperature:value --output temp.png\n"
        "\n"
        "signal names are entity ids: run with --list to see the signals "
        "available in the bucket\n"
        "connection settings may also come from the INFLUXDB_* environment\n"
        "variables or from an env file (./env by default, see --env-file)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url",
                        help="InfluxDB v2 base URL, e.g. https://influx.example.com:8086 "
                        "(env INFLUXDB_URL or URL/SERVER in the env file)")
    parser.add_argument("--token",
                        help="API token (env INFLUXDB_TOKEN or TOKEN in the env file)")
    parser.add_argument("--org",
                        help="organisation (env INFLUXDB_ORG or ORG/ORG_NAME in the env file)")
    parser.add_argument("--bucket",
                        help="bucket to inspect (env INFLUXDB_BUCKET or BUCKET/BUCKET_NAME in the env file)")
    parser.add_argument("--env-file", metavar="FILE",
                        help="KEY=VALUE file with connection settings, used for anything "
                        "not given as a flag or environment variable "
                        "(default: ./env, then env next to this script)")
    parser.add_argument("--range", default="-30d", metavar="RANGE",
                        help="time range as a Flux duration (-30m, -24h, -7d) or 'all' (default: -30d)")
    parser.add_argument("--list", action="store_true",
                        help="list the signals (default action when --plot is not given)")
    parser.add_argument("--plot", metavar="SIGNALS",
                        help="comma-separated signals to plot, each 'entity_id' or 'entity_id:field' "
                        "(e.g. temperature,humidity or temperature:value)")
    parser.add_argument("--style", choices=("stairs", "line"), default="stairs",
                        help="draw signals as stairs (zero-order hold: each value is held "
                        "until the next sample) or as interpolated lines "
                        "(default: %(default)s)")
    parser.add_argument("--limit", type=int, default=5000, metavar="N",
                        help="max points per field downloaded for plotting (default: 5000)")
    parser.add_argument("--output", metavar="FILE",
                        help="save the plot to FILE instead of displaying it")
    parser.add_argument("--insecure", action="store_true",
                        help="do not verify the server's TLS certificate (self-signed certs)")
    args = parser.parse_args(argv)

    if args.env_file:
        if not os.path.isfile(args.env_file):
            parser.error(f"env file not found: {args.env_file}")
        env_path = args.env_file
    else:
        env_path = find_env_file()
    try:
        file_values = load_env_file(env_path) if env_path else {}
    except OSError as exc:
        parser.error(f"cannot read env file {env_path}: {exc}")

    missing = []
    for name, env_var, file_keys in (
        ("url", "INFLUXDB_URL", ("INFLUXDB_URL", "URL", "SERVER")),
        ("token", "INFLUXDB_TOKEN", ("INFLUXDB_TOKEN", "TOKEN")),
        ("org", "INFLUXDB_ORG", ("INFLUXDB_ORG", "ORG", "ORG_NAME", "ORGANIZATION")),
        ("bucket", "INFLUXDB_BUCKET", ("INFLUXDB_BUCKET", "BUCKET", "BUCKET_NAME")),
    ):
        value = getattr(args, name) or os.environ.get(env_var)
        if not value:
            value = next(
                (file_values[key] for key in file_keys if file_values.get(key)), None
            )
        if not value:
            missing.append(f"--{name}")
        setattr(args, name, value)
    if missing:
        parser.error(
            "missing connection settings: " + ", ".join(missing)
            + " (use the flags, the INFLUXDB_* environment variables, or an env file)"
        )
    return args


def main(argv=None):
    args = parse_args(argv)
    url = args.url.rstrip("/")

    if args.insecure:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    client = InfluxDBClient(
        url=url,
        token=args.token,
        org=args.org,
        verify_ssl=not args.insecure,
        timeout=120_000,
    )

    try:
        check_server(client)
        check_bucket(client, args.org, args.bucket)
        start = flux_start(args.range)
        if args.list or not args.plot:
            list_signals(client, args.org, args.bucket, start, args.range)
        if args.plot:
            selectors = [s.strip() for s in args.plot.split(",") if s.strip()]
            if args.list:
                print()
            plot_signals(
                client, args.org, args.bucket,
                selectors, start, args.limit, args.output, args.range, args.style,
            )
    except InfluxError as exc:
        sys.exit(f"error: {exc}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
