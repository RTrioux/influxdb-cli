"""
influxdb_signals.py — the signals (entities) of an InfluxDB v2 bucket as
pandas frames, wrapped in one connection-holding class.

A signal is identified by the 'entity_id' tag (the Home Assistant schema);
data series without that tag fall back to the measurement name.

Connection settings (each of url/token/org/bucket) are resolved in this
order (first match wins):

    1. constructor arguments
    2. environment variables: INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG,
       INFLUXDB_BUCKET
    3. an env file with KEY=VALUE lines: ./env, then next to this module;
       point at another location with the env_file argument or the CLI's
       --env-file. Recognised keys (case-insensitive):

           URL or INFLUXDB_URL or SERVER      e.g. https://influx.example.com:8086
           TOKEN or INFLUXDB_TOKEN            API token with read access
           ORG or INFLUXDB_ORG or ORG_NAME    organisation name
           BUCKET or INFLUXDB_BUCKET          bucket to inspect

Quickstart (see also influxdb_cli.py, the command-line frontend):

    from influxdb_signals import InfluxDBSignals, interpolate

    db = InfluxDBSignals()                       # settings from ./env...
    db.list_signals("-90d")                      # one row per signal
    raw = db.get_signals("kitchen_temp", "-90d")  # wide frame, UTC index
    db.plot("kitchen_temp,living_temp", range_="-7d", output="plot.png")
    db.export("kitchen_temp,living_temp", "signals.h5", range_="-90d")
    db.close()

    # math with non-synchronized signals: align them on a common grid first
    raw = db.get_signals("kitchen_temp,living_temp", "-90d")
    aligned = interpolate(raw, freq="5min")       # zero-order hold by default
    delta = aligned.iloc[:, 0] - aligned.iloc[:, 1]

Use InfluxDBSignals as a context manager (with InfluxDBSignals(...) as db:)
to close the connection automatically.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import urllib3
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException

from timeseries import DataError, export, interpolate, load, plot

__all__ = ["InfluxDBSignals", "InfluxError", "DataError",
           "interpolate", "export", "load", "plot"]

DURATION_RE = re.compile(r"^-?(\d+[smhdwMy])+$")

# (setting, environment variables, env-file keys)
_SETTINGS = (
    ("url", ("INFLUXDB_URL",), ("INFLUXDB_URL", "URL", "SERVER")),
    ("token", ("INFLUXDB_TOKEN",), ("INFLUXDB_TOKEN", "TOKEN")),
    ("org", ("INFLUXDB_ORG",), ("INFLUXDB_ORG", "ORG", "ORG_NAME", "ORGANIZATION")),
    ("bucket", ("INFLUXDB_BUCKET",), ("INFLUXDB_BUCKET", "BUCKET", "BUCKET_NAME")),
)

META_COLUMNS = frozenset(
    {"result", "table", "_start", "_stop", "_measurement", "_field"}
)


class InfluxError(Exception):
    """Fatal error reported to the user without a traceback."""


# ---------------------------------------------------------------------------
# Low-level API helpers
# ---------------------------------------------------------------------------

def flux_str(value):
    """Quote a string for use in a Flux query."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def flux_start(range_arg):
    """Translate a range like '-24h' or 'all' into a Flux range() start."""
    if range_arg in ("all", "0"):
        return "0"
    if not DURATION_RE.match(range_arg):
        raise InfluxError(
            f"invalid range {range_arg!r} "
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
# Small helpers
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


def split_selectors(selectors):
    """Selectors as a string ('a,b' or 'a:value'), or an iterable -> list."""
    if isinstance(selectors, str):
        return [s.strip() for s in selectors.split(",") if s.strip()]
    return [s.strip() for s in selectors if s and s.strip()]


# ---------------------------------------------------------------------------
# Configuration (env file)
# ---------------------------------------------------------------------------

def find_env_file():
    """Locate the default env file: ./env, then env next to this module."""
    module_dir = Path(__file__).resolve().parent
    candidates = (Path.cwd() / "env", module_dir / "env")
    for path in candidates:
        if path.is_file():
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
# The main class
# ---------------------------------------------------------------------------

class InfluxDBSignals:
    """One InfluxDB v2 connection: list, fetch, plot and export signals.

    Instantiating resolves the connection settings (arguments > INFLUXDB_*
    environment variables > env file, see the module docstring), connects,
    checks the server's health and that the bucket exists, and raises
    InfluxError otherwise; all errors then surface as InfluxError/DataError.
    Instances work as context managers ('with InfluxDBSignals() as db:').
    """

    def __init__(self, url=None, token=None, org=None, bucket=None, *,
                 env_file=None, insecure=False, timeout_ms=120_000):
        if env_file is not None:
            env_path = Path(env_file)
            if not env_path.is_file():
                raise InfluxError(f"env file not found: {env_file}")
        else:
            env_path = find_env_file()
        try:
            file_values = load_env_file(env_path) if env_path else {}
        except OSError as exc:
            raise InfluxError(f"cannot read env file {env_path}: {exc}")

        given = {"url": url, "token": token, "org": org, "bucket": bucket}
        missing = []
        for name, env_vars, file_keys in _SETTINGS:
            value = given[name] or next(
                (os.environ[v] for v in env_vars if os.environ.get(v)), None
            )
            if not value:
                value = next(
                    (file_values[k] for k in file_keys if file_values.get(k)),
                    None,
                )
            if not value:
                missing.append(name)
            setattr(self, name, value)
        if missing:
            raise InfluxError(
                "missing connection settings: " + ", ".join(missing)
                + " (pass them as arguments, set the INFLUXDB_* environment"
                " variables, or provide an env file)"
            )
        self.url = self.url.rstrip("/")

        if insecure:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            verify_ssl=not insecure,
            timeout=timeout_ms,
        )
        check_server(self.client)
        check_bucket(self.client, self.org, self.bucket)

    # -- connection lifecycle ------------------------------------------------

    def close(self):
        """Close the underlying HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    # -- features ------------------------------------------------------------

    def list_signals(self, range_="-30d"):
        """Return one row per signal: fields, types, points, first/last seen."""
        start = flux_start(range_)
        field_rows = run_query(self.client, self.org,
                               fields_query(self.bucket, start))
        if not field_rows:
            return pd.DataFrame()

        count_rows = run_query(self.client, self.org,
                               counts_query(self.bucket, start))
        first_rows = run_query(self.client, self.org,
                               time_bounds_query(self.bucket, start, "min"))
        last_rows = run_query(self.client, self.org,
                              time_bounds_query(self.bucket, start, "max"))

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

        def bounds(rows, key):
            best = {}
            for row in rows:
                name, when = signal_name(row), row.get(key)
                if when and (name not in best or when < best[name]):
                    best[name] = when
            return best

        first_seen = bounds(first_rows, "min_seen")
        last_seen = bounds(last_rows, "max_seen")

        return pd.DataFrame(
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

    def get_signals(self, selectors, range_="-30d", *, limit=5000,
                    align=None, interpolation="zoh"):
        """Fetch signals as one wide frame: UTC DatetimeIndex, one numeric
        column per selector ('entity' or 'entity:field'; a string like
        'a,b' or an iterable are both accepted).

        The index is the union of every signal's timestamps, so columns hold
        NaN wherever their signal had no sample. Pass align (a pandas
        frequency like "5min", or "auto") to resample everything onto one
        common grid via timeseries.interpolate() — that frame is made for
        math between signals. 'interpolation' is "zoh" (default), "linear"
        or "nearest".
        """
        start = flux_start(range_)
        frames = []
        for selector in split_selectors(selectors):
            name, _, field = selector.partition(":")
            rows = run_query(self.client, self.org,
                             plot_query(self.bucket, start, name, field, limit))
            if not rows:
                print(
                    f"warning: no data for signal {selector!r} in range {range_} "
                    "(run with --list to see the available signals)",
                    file=sys.stderr,
                )
                continue
            df = pd.DataFrame(rows)
            df["_time"] = pd.to_datetime(df["_time"], utc=True)
            df = df.set_index("_time").sort_index()
            value_cols = [c for c in df.columns if c not in META_COLUMNS]
            numeric = df[value_cols].select_dtypes("number")
            if numeric.empty:
                print(
                    f"warning: signal {selector!r} has no numeric field, skipped",
                    file=sys.stderr,
                )
                continue
            numeric.columns = [f"{selector}.{c}" for c in numeric.columns]
            frames.append(numeric)

        if not frames:
            raise InfluxError(
                "no numeric data for the selected signal(s) "
                f"in range {range_}"
            )

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

        if align is not None:
            freq = None if align == "auto" else align
            data = interpolate(data, freq=freq, method=interpolation)
        return data

    def plot(self, selectors, *, range_="-30d", limit=5000, align=None,
             interpolation="zoh", style="stairs", output=None):
        """Fetch signals and draw them (stairs by default, see timeseries.plot).
        output: save the image to this path instead of opening a window."""
        data = self.get_signals(selectors, range_, limit=limit, align=align,
                                interpolation=interpolation)
        plot(data, style=style,
             title=f"InfluxDB bucket {self.bucket!r} — {range_}",
             output=output)
        if output:
            print(f"Plot saved to {output}")

    def export(self, selectors, path, *, range_="-30d", limit=5000,
               align=None, interpolation="zoh"):
        """Fetch signals and write them to a file (format by suffix: .h5,
        .csv, .parquet, .json — see timeseries.export). Returns the path."""
        data = self.get_signals(selectors, range_, limit=limit, align=align,
                                interpolation=interpolation)
        target = export(data, path)
        print(f"Exported {len(data)} row(s), {data.shape[1]} column(s) to {target}")
        return target
