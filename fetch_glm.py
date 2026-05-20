#!/usr/bin/env python3
"""
FIRESTORM lightning pipeline — pulls GOES-R GLM (Geostationary Lightning
Mapper) Level-2 LCFA flash records from NOAA's Open Data S3 buckets,
decodes the netCDF, merges across GOES-East (G16) + GOES-West (G18),
and writes a slim JSON the frontend reads via raw.githubusercontent.com.

Why this exists: there is no browser-fetchable real-time lightning feed
that doesn't require a Vaisala NLDN contract (~$50k/yr). GLM is the free
federal alternative — staring optical lightning sensor on the GOES
constellation, ~20-second cadence per satellite, CONUS + Caribbean +
most of S. America (G16) + Pacific + Alaska + western CONUS (G18).

What the frontend used to do (v2_140 and earlier): call
generateDemoLightning(40), drop 40 random points around hand-picked
region centroids. Nothing real. This pipeline replaces that.

Detection caveats — read these once, don't be surprised later:
  • GLM is OPTICAL, not RF. It sees the flash bloom; ground networks
    measure the EM pulse. As a result GLM cannot distinguish CG (cloud-
    to-ground) from IC (intra-cloud) by itself. Every flash is labeled
    type='flash' downstream. If a future operator needs CG-only
    classification, that requires NLDN cross-correlation — out of scope
    here.
  • CG-only detection efficiency is ~70%; total flash detection is much
    higher. For situational awareness on a wildfire dashboard this is
    fine; for IC strategic ignition decisions you'd want NLDN.
  • End-to-end latency from strike to JSON: ~2–4 minutes. (Strike →
    GLM downlink ~30s → ground processing ~30s → S3 publish ~30s → our
    cron poll up to 5 min → push.)

Output: data/lightning.json
Shape: { "generated_at": ISO8601,
         "window_minutes": 15,
         "counts": {"total": N, "g16": N16, "g18": N18},
         "flashes": [ {lat, lng, energy_fJ, age_sec, sat}, ... ] }

Source: s3://noaa-goes19/GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/*.nc  (East, replaced G16 2025)
        s3://noaa-goes18/GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/*.nc  (West, replaced G17 2023)
        Public, anonymous, no auth, no egress charge.

Requires: boto3, botocore, netCDF4, numpy. No API key.
"""

from __future__ import annotations
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import netCDF4
import numpy as np

# ── Config ───────────────────────────────────────────────────────────
# Window: how far back to pull files. 15 min × ~3 files/min = ~45 files
# per satellite per cycle. Each file is ~50–500 KB. ~30 MB total in
# memory per run — well inside GHA's free tier.
WINDOW_MINUTES = 15

# Cap on flashes emitted per cycle. The full firehose (CONUS active
# convection day) can be 50k+ flashes per 15-min window. The frontend
# only renders ~100–500 markers usefully. We sort by energy descending
# and keep the top MAX_FLASHES, which biases toward the strongest /
# most operationally meaningful strikes.
MAX_FLASHES = 5000

# Satellites to poll. As of mid-2026 the operational constellation is:
#   G19 = GOES-East (75.2°W)  — replaced G16 in April 2025
#   G18 = GOES-West (137.2°W) — replaced G17 in 2023
# G16's GLM-L2-LCFA bucket prefix is empty / not produced anymore. We
# probed it 2026-05-20 and got zero keys; using G16 here would silently
# drop CONUS-East coverage forever, so we use G19. (Leaving G16 wired
# was the bug that bit firestorm-aircraft-data's first cut of MODIS —
# verify-before-claim.)
SATELLITES = [
    {'id': 'g19', 'bucket': 'noaa-goes19', 'name': 'GOES-East'},
    {'id': 'g18', 'bucket': 'noaa-goes18', 'name': 'GOES-West'},
]

PRODUCT_PREFIX = 'GLM-L2-LCFA'

# Anonymous S3 client — NOAA's Open Data buckets are public.
# UNSIGNED skips the AWS credential lookup entirely; this works in any
# environment (GHA runner, local dev, anywhere with internet).
s3 = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED, read_timeout=20, retries={'max_attempts': 3}),
)


def _list_recent_keys(bucket: str, since: datetime) -> list[str]:
    """List GLM L2 LCFA object keys created since `since` (UTC).

    The GLM bucket layout is:
        GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/<filename>.nc

    where DDD is day-of-year (zero-padded 3-digit). Files are produced
    every 20 seconds, so a 15-min window has ~45 files per satellite.

    To avoid listing the whole bucket, we walk only the hour prefixes
    that overlap our window.
    """
    now = datetime.now(timezone.utc)
    keys: list[str] = []

    # Build the (year, day-of-year, hour) prefixes covering the window.
    # Worst case crosses an hour boundary, so we walk current hour AND
    # the previous hour. Crossing day or year boundary is also handled
    # because we recompute from the timestamp.
    cursor = since
    seen_prefixes: set[str] = set()
    while cursor <= now:
        prefix = f'{PRODUCT_PREFIX}/{cursor.year:04d}/{cursor.timetuple().tm_yday:03d}/{cursor.hour:02d}/'
        if prefix not in seen_prefixes:
            seen_prefixes.add(prefix)
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []) or []:
                    if obj['LastModified'] >= since:
                        keys.append(obj['Key'])
        cursor += timedelta(hours=1)

    return keys


def _decode_glm_file(bucket: str, key: str, cutoff_epoch: float) -> list[dict]:
    """Download a GLM netCDF blob, decode flashes, return list of dicts.

    GLM L2 LCFA files contain three parallel datasets: events (raw
    optical pulses), groups (clusters of events), and flashes (clusters
    of groups). We use FLASHES — that's the operator-meaningful unit
    that maps 1:1 to "a lightning strike".

    Variables we read:
      flash_lat, flash_lon          — float32 degrees
      flash_energy                  — float32 femtojoules (fJ)
      flash_time_offset_of_first_event — float32 seconds since
                                        product_time epoch
      product_time                  — float64 seconds since 2000-01-01
                                        12:00:00 UTC (J2000)

    We compute UTC timestamp per flash and drop anything older than
    the cutoff (caller's window).
    """
    flashes: list[dict] = []
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        raw = resp['Body'].read()
        # netCDF4 needs a real file or in-memory dataset. Use Dataset
        # with memory= keyword (netCDF4 ≥ 1.5.0 supports this).
        ds = netCDF4.Dataset('inmem', mode='r', memory=raw)
        try:
            # product_time is seconds since J2000 (2000-01-01 12:00:00 UTC).
            # Convert to a unix epoch for trivial comparison.
            j2000_epoch = datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()
            product_time = float(ds.variables['product_time'][...].item()) + j2000_epoch

            lat = ds.variables['flash_lat'][:]
            lon = ds.variables['flash_lon'][:]
            energy = ds.variables['flash_energy'][:]
            offset = ds.variables['flash_time_offset_of_first_event'][:]
        finally:
            ds.close()

        # Vectorize: drop NaN/inf and stale flashes.
        n = len(lat)
        for i in range(n):
            try:
                la = float(lat[i])
                ln = float(lon[i])
                en = float(energy[i])
                of = float(offset[i])
            except Exception:
                continue
            if not (np.isfinite(la) and np.isfinite(ln) and np.isfinite(en)):
                continue
            t = product_time + of
            if t < cutoff_epoch:
                continue
            flashes.append({
                'lat': round(la, 4),
                'lng': round(ln, 4),
                # GLM emits energy in joules (netCDF4 auto-applies scale_factor).
                # Typical flash energies are 1e-15 → 1e-12 J. Multiply by 1e15
                # so the frontend gets human-readable "femtojoule" integers
                # (~1–1000) instead of float-exponent noise.
                'energy_fJ': round(en * 1e15, 1),
                'epoch': round(t, 1),
            })
    except Exception as e:
        print(f'[glm] decode skip {key}: {e}', file=sys.stderr)
    return flashes


def _fetch_satellite(sat: dict, since: datetime) -> list[dict]:
    """Pull all flashes from one satellite's window."""
    bucket = sat['bucket']
    sat_id = sat['id']
    keys = _list_recent_keys(bucket, since)
    print(f'[glm] {sat_id}: {len(keys)} L2 LCFA files in last {WINDOW_MINUTES}m')
    cutoff = since.timestamp()
    out: list[dict] = []
    for key in keys:
        out.extend(_decode_glm_file(bucket, key, cutoff))
    for f in out:
        f['sat'] = sat_id
    return out


def main() -> int:
    now = datetime.now(timezone.utc)
    since = now - timedelta(minutes=WINDOW_MINUTES)
    now_epoch = now.timestamp()

    all_flashes: list[dict] = []
    counts: dict[str, int] = {}
    for sat in SATELLITES:
        try:
            sat_flashes = _fetch_satellite(sat, since)
            counts[sat['id']] = len(sat_flashes)
            all_flashes.extend(sat_flashes)
        except Exception as e:
            print(f'[glm] satellite {sat["id"]} failed: {e}', file=sys.stderr)
            counts[sat['id']] = 0

    # Sort by energy descending so the strongest flashes win the cap.
    all_flashes.sort(key=lambda f: f.get('energy_fJ', 0), reverse=True)
    if len(all_flashes) > MAX_FLASHES:
        print(f'[glm] capping {len(all_flashes)} → {MAX_FLASHES} flashes (kept strongest by energy)')
        all_flashes = all_flashes[:MAX_FLASHES]

    # Convert epoch → age_sec for the frontend, then drop epoch.
    # Front-end only cares "how old is this strike" relative to fetch.
    for f in all_flashes:
        f['age_sec'] = max(0, int(round(now_epoch - f.pop('epoch', now_epoch))))
        f['type'] = 'flash'  # GLM cannot distinguish CG from IC; see module docstring

    payload = {
        'generated_at': now.replace(microsecond=0).isoformat(),
        'window_minutes': WINDOW_MINUTES,
        'counts': {
            'total': len(all_flashes),
            **counts,
        },
        'source': 'NOAA GOES-R GLM L2 LCFA via NOAA Open Data on AWS S3 (public, no auth)',
        'flashes': all_flashes,
    }

    out_path = os.path.join(os.path.dirname(__file__), 'data', 'lightning.json')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(payload, f, separators=(',', ':'))

    sat_summary = ', '.join(f'{k}={v}' for k, v in counts.items())
    print(f'[glm] wrote {out_path}: {len(all_flashes)} flashes ({sat_summary})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
