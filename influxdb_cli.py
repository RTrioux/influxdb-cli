#!/usr/bin/env python3
"""
influxdb_cli.py — command-line frontend to the influxdb_signals module: list
the signals (entities) of an InfluxDB v2 bucket and plot/export them.

All the actual work (connection, queries, alignment, plotting, exporting)
lives in influxdb_signals / timeseries; this script only wires command-line
arguments to it. The connection settings are documented in those modules and
resolved as: flags > INFLUXDB_* environment variables > an env file (./env).

Examples:

    # List every signal in the bucket: fields, types, size, time range
    ./influxdb_cli.py --list

    # Plot two signals (all of their numeric fields), last 90 days
    ./influxdb_cli.py --plot thermometre_cuisine_temperature,thermometre_salon_temperature --range -90d

    # Save the plot to a file, plot interpolated lines instead of stairs
    ./influxdb_cli.py --plot thermometre_cuisine_temperature --range -7d \
        --style line --output plot.png

    # Export two signals to a file (format from the suffix: .h5, .csv, ...),
    # resampled onto a common grid so the columns are synchronized
    ./influxdb_cli.py --export thermometre_cuisine_temperature,thermometre_salon_temperature \
        --range -90d --align 1min --output signals.h5

Signal names are entity ids (e.g. kitchen_temperature); run with --list to
see the signals available in the bucket.
"""

import argparse
import sys

from influxdb_signals import InfluxDBSignals, InfluxError
from timeseries import DataError


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="List the signals (entities) of an InfluxDB v2 bucket, "
                    "plot them and export them to files.",
        epilog="examples:\n"
               "  %(prog)s --list\n"
               "  %(prog)s --plot kitchen_temperature,living_humidity \\\n"
               "        --range -7d --output plot.png\n"
               "  %(prog)s --export kitchen_temperature,living_humidity \\\n"
               "        --range -90d --align 5min --output signals.h5\n"
               "\n"
               "everything here is also available as a Python library:\n"
               "    from influxdb_signals import InfluxDBSignals, interpolate\n"
               "\n"
               "signal names are entity ids: run with --list to see the signals\n"
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
                        help="list the signals (default action when --plot/--export are not given)")
    parser.add_argument("--plot", metavar="SIGNALS",
                        help="comma-separated signals to plot, each 'entity_id' or 'entity_id:field' "
                        "(e.g. temperature,humidity or temperature:value)")
    parser.add_argument("--export", metavar="SIGNALS",
                        help="comma-separated signals to export to --output "
                        "(format from the file suffix: .h5/.hdf5, .csv, .parquet, .json)")
    parser.add_argument("--align", metavar="FREQ",
                        help="resample the plotted/exported signals onto a common time grid "
                        "before drawing/writing, so all signals are synchronized "
                        "(pandas frequency like 30s, 5min, 1h, or 'auto' to derive it "
                        "from the data; without it the raw samples are used)")
    parser.add_argument("--interpolation", choices=("zoh", "linear", "nearest"),
                        default="zoh",
                        help="how to interpolate during --align: zoh holds each value until "
                        "the next sample (default), linear interpolates between "
                        "neighbouring samples, nearest picks the closest sample")
    parser.add_argument("--style", choices=("stairs", "line"), default="stairs",
                        help="draw signals as stairs (zero-order hold: each value is held "
                        "until the next sample) or as interpolated lines "
                        "(default: %(default)s)")
    parser.add_argument("--limit", type=int, default=5000, metavar="N",
                        help="max points per field downloaded for plotting (default: 5000)")
    parser.add_argument("--output", metavar="FILE",
                        help="save the plot or the export to FILE instead of displaying it")
    parser.add_argument("--insecure", action="store_true",
                        help="do not verify the server's TLS certificate (self-signed certs)")
    args = parser.parse_args(argv)

    if args.plot and args.export:
        parser.error("--plot and --export are mutually exclusive; run them separately")
    if args.export and not args.output:
        parser.error("--export requires --output FILE")
    return args


def main(argv=None):
    args = parse_args(argv)
    listing = args.list or not (args.plot or args.export)
    try:
        with InfluxDBSignals(url=args.url, token=args.token, org=args.org,
                             bucket=args.bucket, env_file=args.env_file,
                             insecure=args.insecure) as signals:
            if listing:
                table = signals.list_signals(args.range)
                if table.empty:
                    print(f"No data found in bucket {signals.bucket!r} "
                          f"for range {args.range}.")
                else:
                    print(f"Bucket {signals.bucket!r} — {len(table)} signal(s), "
                          f"range: {args.range}\n")
                    print(table.to_string(index=False))
            if args.plot:
                if listing:
                    print()
                signals.plot(args.plot, range_=args.range, limit=args.limit,
                              align=args.align,
                              interpolation=args.interpolation,
                              style=args.style, output=args.output)
            if args.export:
                if listing:
                    print()
                signals.export(args.export, args.output, range_=args.range,
                               limit=args.limit, align=args.align,
                               interpolation=args.interpolation)
    except (InfluxError, DataError) as exc:
        sys.exit(f"error: {exc}")


if __name__ == "__main__":
    main()
