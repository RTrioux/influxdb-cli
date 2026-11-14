"""
timeseries.py — pure pandas helpers for signal frames (no database access).

A *signal frame* is a pandas DataFrame with a UTC DatetimeIndex (one row per
sample) and one numeric column per signal/field, as built by
influxdb_signals.InfluxDBSignals.get_signals(). This module aligns, plots,
exports and loads signal frames.

Alignment (interpolation)
-------------------------

Signals are usually not sampled at the same instants, so cellwise math on raw
frames is meaningless: between two samples of signal A there may be dozens of
timestamps of signal B, and on the merged index one of the two values is always
NaN. interpolate() resamples every column onto ONE common time grid, producing
a frame where rows are synchronized and math just works:

    from timeseries import interpolate

    aligned = interpolate(frame, freq="5min")                 # ZOH by default
    delta = aligned["kitchen_temp.value"] - aligned["living_temp.value"]

'freq' may be any pandas offset string ("30s", "5min", "1h", "1D"), or None /
"auto" to derive the grid from the data: the median sampling interval, rounded
to a nice value ("30min", "1h", ...).

Three interpolation methods:

    zoh      zero-order hold (default): each sample is held until the next
             one — the same convention as the CLI's stairs style.
    linear   time-based linear interpolation between neighbouring samples.
    nearest  nearest sample, however far it is (bound it with max_hold).

Timestamps before a signal's first sample stay NaN: there is nothing to hold
or interpolate from. With max_hold=N, a value stops being held/interpolated N
grid steps after the last real sample, so a dead sensor does not hold forever
over a gap; the default None holds/interpolates across any gap.

Export / load
-------------

The export format is chosen by the file suffix: HDF5 (.h5/.hdf/.hdf5 — needs
`pip install tables`), CSV, Parquet (`pip install pyarrow`) and JSON.

    from timeseries import export, load

    export(aligned, "signals.h5")     # round-trips: load(export(df)) == df
    frame = load("signals.h5")

Plot
----

plot(frame, ...) draws one line per column, breaking lines only where a signal
genuinely has no sample (NaN), with a stairs (ZOH) or line style:

    from timeseries import plot

    plot(interpolate(frame, freq="1h"), output="plot.png")
"""

from __future__ import annotations

import pathlib

import pandas as pd

__all__ = ["DataError", "interpolate", "default_freq", "export", "load", "plot"]

HDF_KEY = "signals"
EXPORT_SUFFIXES = ("h5", "hdf", "hdf5", "csv", "parquet", "json")


class DataError(Exception):
    """Fatal error in a pure-data operation, reported without a traceback."""


# ---------------------------------------------------------------------------
# Grid frequency
# ---------------------------------------------------------------------------

def _snap_seconds(seconds):
    """Round a number of seconds to the nearest nice pandas offset string."""
    candidates = []
    for unit, secs in (("s", 1), ("min", 60), ("h", 3600), ("D", 86400)):
        for mult in (1, 2, 5, 10, 15, 30):
            candidates.append((mult * secs, f"{mult}{unit}"))
    candidates.append((7 * 86400, "7D"))
    return min(candidates, key=lambda c: (abs(c[0] - seconds), c[0]))[1]


def default_freq(df):
    """A sensible grid frequency for this frame: the median sampling interval,
    rounded to a nice pandas offset (e.g. "30min", "1h")."""
    if not isinstance(df.index, pd.DatetimeIndex) or len(df.index) < 2:
        return "1min"
    steps = df.index.to_series().diff()
    steps = steps[steps > pd.Timedelta(0)]
    if steps.empty:
        return "1min"
    return _snap_seconds(steps.median().total_seconds())


# ---------------------------------------------------------------------------
# Alignment / interpolation
# ---------------------------------------------------------------------------

def interpolate(df, freq=None, method="zoh", *, grid=None, max_hold=None):
    """Reindex a signal frame onto a common time grid, filling the gaps.

    df:       signal frame (UTC DatetimeIndex recommended, one column per
              signal); returns a NEW frame, sorted on the grid.
    freq:     grid frequency as a pandas offset ("30s", "5min", "1h"); None
              or "auto" derives it from the data (median sample interval).
    method:   "zoh" (zero-order hold, default), "linear" (time-based
              interpolation between neighbouring samples) or "nearest".
    grid:     an explicit DatetimeIndex to align onto (overrides freq).
    max_hold: maximum number of grid steps a value may be held or
              interpolated over a gap (None = no limit).

    Rows before a signal's first sample stay NaN; rows after a gap stay NaN
    once max_hold grid steps have passed. Use the result for math: all
    columns share the same timestamps.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise DataError("interpolate needs a DataFrame with a DatetimeIndex")
    if df.empty:
        return df.copy()
    data = df.sort_index()
    if not data.index.is_unique:
        raise DataError("interpolate needs unique timestamps "
                        f"(the index has {len(data) - data.index.nunique()} duplicates)")

    if grid is None:
        if freq is None or (isinstance(freq, str) and freq.lower() == "auto"):
            freq = default_freq(data)
        try:
            # anchor the grid at the floor of the first timestamp so grid
            # rows land on round instants (22:45:00, not 22:46:22.129)
            grid = pd.date_range(data.index.min().floor(freq),
                                 data.index.max(), freq=freq)
        except (ValueError, TypeError) as exc:
            raise DataError(f"invalid grid frequency {freq!r}: {exc}")
        grid.name = data.index.name
    else:
        grid = pd.DatetimeIndex(grid).sort_values()
        grid.name = data.index.name

    if method not in ("zoh", "linear", "nearest"):
        raise DataError(
            f"unknown interpolation method {method!r} (use 'zoh', 'linear' or 'nearest')"
        )

    # Each column aligns from ITS OWN non-null samples: buckets may contain
    # NaN values (e.g. timestamps that only stored a string field), and the
    # grid frequency is arbitrary, so a frame-level reindex would either
    # propagate those NaNs or drop off-grid samples entirely.
    out = pd.DataFrame(index=grid)
    for col in data.columns:
        cleaned = data[col].dropna()
        if cleaned.empty:
            out[col] = float("nan")
        elif method == "zoh":
            # label-based forward fill: each grid row takes the value of its
            # signal's last real sample at or before it — zero-order hold
            out[col] = cleaned.reindex(grid, method="ffill", limit=max_hold)
        elif method == "linear":
            # interpolate on the union of sample and grid timestamps so no
            # off-grid sample is lost, then keep the grid rows
            merged = cleaned.reindex(cleaned.index.union(grid))
            out[col] = merged.interpolate(method="time",
                                          limit=max_hold).reindex(grid)
        else:  # nearest
            tolerance = None
            if max_hold is not None and len(grid) > 1:
                tolerance = max_hold * pd.Timedelta(grid.to_series().diff().median())
            out[col] = cleaned.reindex(grid, method="nearest",
                                       tolerance=tolerance)
    return out


# ---------------------------------------------------------------------------
# Export / load
# ---------------------------------------------------------------------------

def export(df, path):
    """Write a signal frame to a file; the format follows the file suffix:
    .h5/.hdf/.hdf5 (HDF5, needs 'tables'), .csv, .parquet (needs 'pyarrow'),
    .json. Returns the written path."""
    path = pathlib.Path(path)
    if df.empty:
        raise DataError("nothing to export: the frame is empty")
    suffix = path.suffix.lower().lstrip(".")
    if suffix not in EXPORT_SUFFIXES:
        raise DataError(
            f"unsupported export format {path.suffix!r} for {path.name} "
            f"(expected one of: {', '.join('.' + s for s in EXPORT_SUFFIXES)})"
        )
    try:
        if suffix in ("h5", "hdf", "hdf5"):
            df.to_hdf(path, key=HDF_KEY, mode="w", format="table")
        elif suffix == "csv":
            df.to_csv(path)
        elif suffix == "parquet":
            df.to_parquet(path)
        else:
            df.to_json(path, orient="table", date_format="iso",
                       date_unit="ns", indent=2)
    except ImportError as exc:
        need = "tables" if suffix in ("h5", "hdf", "hdf5") else "pyarrow"
        raise DataError(
            f"cannot export to {path.suffix}: missing optional dependency "
            f"(pip install {need}): {exc}"
        )
    except (OSError, ValueError) as exc:
        raise DataError(f"cannot write {path}: {exc}")
    return path


def load(path):
    """Read back a signal frame written by export(); returns a UTC-indexed,
    time-sorted DataFrame."""
    path = pathlib.Path(path)
    if not path.is_file():
        raise DataError(f"file not found: {path}")
    suffix = path.suffix.lower().lstrip(".")
    try:
        if suffix in ("h5", "hdf", "hdf5"):
            df = pd.read_hdf(path, key=HDF_KEY)
        elif suffix == "csv":
            df = pd.read_csv(path, index_col=0)
        elif suffix == "parquet":
            df = pd.read_parquet(path)
        elif suffix == "json":
            df = pd.read_json(path, orient="table", convert_dates=False)
        else:
            raise DataError(
                f"unsupported format {path.suffix!r} for {path.name} "
                f"(expected one of: {', '.join('.' + s for s in EXPORT_SUFFIXES)})"
            )
    except (ImportError, OSError, ValueError) as exc:
        if isinstance(exc, ImportError):
            need = "tables" if suffix in ("h5", "hdf", "hdf5") else "pyarrow"
            raise DataError(f"cannot load {path.suffix} (pip install {need}): {exc}")
        raise DataError(f"cannot read {path}: {exc}")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    elif df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df.sort_index()


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

def plot(df, *, style="stairs", title=None, ylabel="value", output=None,
         show=True):
    """Draw one line per column of a signal frame.

    style: "stairs" (zero-order hold, default) or "line" (interpolated).
    output: file path to save the image instead of opening a window; when
            given, the format is chosen by matplotlib from the suffix.
    Lines break only where a signal really has no sample (NaN), so raw
    merged frames plot each signal with its own points.
    """
    import matplotlib

    if output:
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 6))
    drawstyle = "steps-post" if style == "stairs" else "default"
    for col in df.columns:
        # A merged frame's index is the union of every signal's timestamps,
        # so each column is NaN wherever its signal had no sample; matplotlib
        # would break the line at those points. Plot only that signal's own
        # points (frames are sorted by time, so order holds).
        series = df[col].dropna()
        if series.empty:
            continue
        ax.plot(series.index, series.tolist(), drawstyle=drawstyle,
                linewidth=1.2, label=col)
    if title:
        ax.set_title(title)
    ax.set_xlabel("time (UTC)")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    if ax.get_legend_handles_labels()[0]:
        ax.legend(fontsize="small", loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()

    if output:
        fig.savefig(output, dpi=150)
        plt.close(fig)
    elif show:
        plt.show()
    return pathlib.Path(output) if output else None
