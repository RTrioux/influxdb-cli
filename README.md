# influxdb-cli

A tiny tool to list, plot, export and do math with the signals (entities)
stored in an [InfluxDB v2](https://www.influxdata.com/) bucket — the Home
Assistant schema by default (a signal is identified by its `entity_id`, e.g.
`thermometre_cuisine_temperature`).

## Files

| file | role |
| --- | --- |
| `influxdb_cli.py` | the command-line interface (thin frontend) |
| `influxdb_signals.py` | the library: `InfluxDBSignals` class (connection, queries) |
| `timeseries.py` | the library: pure pandas alignment, plot, export/load |

## Requirements

- Python 3
- `pip install influxdb-client pandas matplotlib`
- optional, for exports: `pip install tables` (HDF5), `pip install pyarrow` (Parquet)

## Configuration

Create an `env` file next to the script (or point to one with `--env-file`):

```
URL=https://influx.example.com:8086
TOKEN=your-api-token
ORG=your-org
BUCKET=your-bucket
```

Each setting can also be passed as a flag (`--url`, `--token`, `--org`,
`--bucket`) or an environment variable (`INFLUXDB_URL`, `INFLUXDB_TOKEN`,
`INFLUXDB_ORG`, `INFLUXDB_BUCKET`). Flags win over environment variables,
which win over the env file.

## Command line

List the signals available in the bucket, with fields, point counts and
first/last timestamps:

```bash
./influxdb_cli.py --list
```

Plot one or more signals over the last 90 days (a window opens, or use
`--output FILE` to save instead):

```bash
./influxdb_cli.py --plot thermometre_cuisine_temperature --range -90d
./influxdb_cli.py --plot thermometre_cuisine_temperature,thermometre_salon_temperature --range -90d
```

Export signals to a file (format from the suffix: `.h5`, `.csv`, `.parquet`,
`.json`) — here resampled on a common 1-minute grid so the columns are
synchronized:

```bash
./influxdb_cli.py --export thermometre_cuisine_temperature,thermometre_salon_temperature --range -90d --align 1min --output signals.h5
```

Handy options:

| Option | Description |
| --- | --- |
| `--range RANGE` | Flux duration (`-24h`, `-7d`, `-90d`) or `all` (default `-30d`) |
| `--export SIGNALS` | export the signals to `--output FILE` |
| `--align FREQ` | resample on a common time grid: pandas frequency (`5min`, `1h`) or `auto` |
| `--interpolation` | `zoh` (default: each value held until the next sample), `linear`, `nearest` |
| `--style` | `stairs` (default, zero-order hold) or `line` |
| `--limit N` | Max points downloaded per field (default 5000) |
| `--output FILE` | output file for `--plot` or `--export` |
| `--insecure` | Skip TLS certificate verification (self-signed certs) |

Run `./influxdb_cli.py --help` for the full list.

## As a Python library

Everything the CLI does is available programmatically. Instantiating
`InfluxDBSignals` resolves the connection settings (arguments > `INFLUXDB_*`
environment variables > env file), connects and validates; then use its
methods:

```python
from influxdb_signals import InfluxDBSignals, interpolate

db = InfluxDBSignals()                    # settings from ./env or env vars
db.list_signals("-90d")                   # one row per signal
raw = db.get_signals("kitchen_temp,living_temp", "-90d")
db.plot("kitchen_temp", range_="-30d", output="kitchen.png")
db.export("kitchen_temp,living_temp", "signals.h5", range_="-90d")
db.close()                                # or: with InfluxDBSignals() as db: ...
```

Signals are not sampled at the same instants, so `raw` has one row per
timestamp with `NaN` wherever a signal had no sample. Before doing math
between signals, align them on a common grid with `interpolate()`:

```python
aligned = interpolate(raw, freq="5min")   # zero-order hold by default
# or: interpolate(raw, freq="auto", method="linear")

delta = aligned.iloc[:, 0] - aligned.iloc[:, 1]   # synchronized math
```

`freq` is any pandas frequency or `auto` (grid derived from the data).
Methods: `zoh` (default; each value held until its signal's next real
sample, like the CLI's stairs), `linear` and `nearest`. Timestamps before a
signal's first sample stay `NaN`; use `max_hold=N` to stop holding values
after a gap of more than N grid steps.

Exported files load back with `load()` and can be re-aligned later:

```python
from timeseries import export, load

export(aligned, "signals.h5")
frame = load("signals.h5")
```
