# Architecture — `firestorm-lightning-data`

This document is the durable reference for the FIRESTORM lightning data
pipeline. Read this before making changes to the cron, the netCDF
decoder, or the JSON schema.

---

## What this is

A cron-driven pipeline that polls NOAA's public GOES-R GLM (Geostationary
Lightning Mapper) Level-2 LCFA product, decodes the netCDF files, and
publishes a slim JSON the FIRESTORM frontend reads via
`raw.githubusercontent.com`.

Runs on free GitHub Actions infrastructure. No AWS account required.
No credentials. No upstream API key.

---

## Why this exists

Until FIRESTORM v2_141, the lightning layer rendered 40 randomly-generated
points around hand-picked region centroids (`generateDemoLightning(40)` in
`index.html`). The visual presentation gave no indication the data was
synthetic. Operator caught it via OpenAI's voice rug-pull in v2_140
("About 40 strikes detected — but this is just a demonstration feed"),
prompting the demand for a real source.

GLM is the only free real-time lightning feed that doesn't require:
- A Vaisala NLDN contract (~$50k/yr)
- A USPLN / ENTLN data agreement
- A NWS LDM subscription with on-prem ingest

The tradeoff: GLM is satellite-optical, not ground-RF, so it cannot
distinguish CG from IC and has ~70% CG detection efficiency versus
NLDN's ~95%. For wildfire dashboard situational awareness this is
acceptable. For strategic ignition decisions on an active fire, an
operator should cross-reference NLDN.

---

## End-to-end data flow

```
┌─────────────────────────────┐
│   GOES-19 (East, 75.2°W)    │   Optical lightning sensor
│   GOES-18 (West, 137.2°W)   │   1-Hz frame rate, 8-km pixel
└─────────────┬───────────────┘
              │  ~30s downlink + ~30s ground processing
              ▼
┌─────────────────────────────┐
│  NOAA Ground System (NSOF)  │   Wallops Ground Station
│  L2 LCFA processing         │   Lightning Cluster Filter Algorithm
└─────────────┬───────────────┘
              │  ~30s S3 publish lag
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Public S3 buckets (NOAA Open Data on AWS, us-east-1)           │
│    s3://noaa-goes19/GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/*.nc          │
│    s3://noaa-goes18/GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/*.nc          │
│  ~20-second cadence per satellite. 50–500 KB per file.          │
│  netCDF-4 format. Anonymous read, zero egress charge.           │
└─────────────┬───────────────────────────────────────────────────┘
              │  Up to 5-min cron poll lag (GHA cron floor)
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  GitHub Actions runner (this repo)                              │
│    .github/workflows/update-lightning.yml — */5 * * * *         │
│    fetch_glm.py:                                                │
│      1. List recent keys (last 15 min) in both buckets          │
│      2. Download netCDF blobs in-memory                         │
│      3. Decode flash variables (lat, lon, energy, time)         │
│      4. Filter to window, dedupe, sort by energy desc           │
│      5. Cap at top 5000 flashes                                 │
│      6. Compute age_sec relative to fetch time                  │
│      7. Write data/lightning.json (~450 KB)                     │
│      8. git commit + push if file changed                       │
└─────────────┬───────────────────────────────────────────────────┘
              │  GitHub raw CDN (instant after push)
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  FIRESTORM frontend (index.html v2_141+)                        │
│    fetchLightning() reads:                                      │
│      raw.githubusercontent.com/Deasus/firestorm-lightning-data/ │
│        main/data/lightning.json                                 │
│    renderLightningLayer() draws ArcGIS markers, popup includes  │
│      energy + sat ID + age                                      │
│    query_lightning + query_region voice tools surface real      │
│      flash counts per state                                     │
└─────────────────────────────────────────────────────────────────┘
```

**End-to-end latency from strike to frontend:** 2–7 minutes. Floor is
GLM downlink + ground processing + S3 publish (~90s combined). Cron
adds 0–5 min on top of that. GitHub raw CDN propagation is effectively
instant after push.

---

## Why GOES-19 and GOES-18 specifically

The GOES constellation rotates as new satellites come online. As of
2026-05-20:

| Slot | Active satellite | Bucket | Replaced |
|---|---|---|---|
| GOES-East (75.2°W) | **G19** | `noaa-goes19` | G16 (April 2025) |
| GOES-West (137.2°W) | **G18** | `noaa-goes18` | G17 (2023) |

**Critical:** the `noaa-goes16` and `noaa-goes17` buckets still exist
but their `GLM-L2-LCFA/` prefixes are empty / no longer produced. Wiring
G16 (the obvious training-data answer for GOES-East) would silently drop
CONUS-East coverage and the pipeline would emit only the West.

When NOAA rotates again — likely G19→G20 in the early 2030s — the same
pattern applies. Probe the bucket before trusting any "current
satellite" claim from documentation. Verify-before-claim.

---

## File: `fetch_glm.py`

### Configuration block (top of file)

| Constant | Value | Why |
|---|---|---|
| `WINDOW_MINUTES` | 15 | Wide enough to absorb GHA cron jitter (1–5 min lag) and present a stable "last 15 min of strikes" view rather than a punctuated 5-min slice. ~45 files per satellite per cycle, ~30 MB in memory total. Well inside GHA's free tier. |
| `MAX_FLASHES` | 5000 | Frontend renders ~100–500 markers usefully; 5000 is a safety ceiling for active convection. JSON size at 5000 ≈ 450 KB, which `raw.githubusercontent` serves comfortably. Sorted by energy descending so the cap biases toward operationally meaningful strikes. |
| `SATELLITES` | G19 + G18 | See §"Why GOES-19 and GOES-18 specifically" above. |
| `PRODUCT_PREFIX` | `GLM-L2-LCFA` | The public Level-2 product. Level-1b is the raw event stream (much larger, no operator value here). |

### Anonymous S3 client

```python
s3 = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED, read_timeout=20, retries={'max_attempts': 3}),
)
```

`UNSIGNED` skips the AWS credential lookup entirely. NOAA's Open Data
buckets are configured for anonymous access. This works in any
environment — GHA runner, local dev, anywhere with internet — without
an AWS account. There are no rate limits on anonymous reads documented;
in practice we've never hit one.

### `_list_recent_keys()`

The bucket layout is `GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/<file>.nc` where
`DDD` is day-of-year (zero-padded 3-digit). To avoid listing the whole
bucket, we walk only the hour prefixes that overlap our window.

Worst case: window crosses an hour boundary, so we walk current hour
AND previous hour. Window crossing day or year boundaries is also
handled because we recompute the prefix from each timestamp in the
walk.

### `_decode_glm_file()`

Each L2 LCFA file contains three parallel datasets:

- **events** — raw optical pulses (~10k–100k per file). Each is a
  single 2-ms detection in a single 8-km pixel.
- **groups** — clustered events (~1k–10k per file). One group ≈ one
  "stroke" (within a flash).
- **flashes** — clustered groups (the operator-meaningful unit, ~100–
  1000 per file). One flash ≈ one "lightning bolt."

We use **flashes** — that's the unit that maps 1:1 to "a lightning
strike" in operator language.

Variables read:

| Variable | Type | Units | Notes |
|---|---|---|---|
| `flash_lat` | float32 | degrees | -90 to +90 |
| `flash_lon` | float32 | degrees | -180 to +180 |
| `flash_energy` | float32 | joules | netCDF4 auto-applies `scale_factor`; we multiply by 1e15 to emit human-readable femtojoules |
| `flash_time_offset_of_first_event` | float32 | seconds | Offset from `product_time` |
| `product_time` | float64 | seconds since J2000 (2000-01-01 12:00:00 UTC) | Per-file reference time |

Per-flash UTC timestamp = `product_time` (J2000-relative) + J2000 epoch
+ `flash_time_offset_of_first_event`. Anything older than the cutoff is
dropped.

NaN/inf filtering is required — GLM occasionally emits garbage values
for flashes that fail the LCFA quality filter.

### Output schema

```json
{
  "generated_at": "2026-05-20T22:00:00+00:00",
  "window_minutes": 15,
  "counts": {
    "total": 5000,
    "g19": 16929,
    "g18": 7191
  },
  "source": "NOAA GOES-R GLM L2 LCFA via NOAA Open Data on AWS S3 (public, no auth)",
  "flashes": [
    {
      "lat": 38.4365,
      "lng": -98.6172,
      "energy_fJ": 142.7,
      "age_sec": 47,
      "sat": "g19",
      "type": "flash"
    },
    ...
  ]
}
```

**Schema notes:**

- `counts.total` is the number of flashes in the JSON payload (post-cap).
- `counts.<satID>` is the pre-cap raw count from each satellite. Useful
  for debugging coverage drops — if `g19` goes to zero suddenly, the
  East bird may have rotated.
- `flashes[].type` is always `'flash'`. GLM cannot distinguish CG from
  IC. Documented here so the frontend / voice tool can avoid the
  CG/IC labels they used in the old demo schema.
- `flashes[].age_sec` is computed at JSON-write time, not strike time.
  When the frontend re-fetches, the AI should compute "how long ago"
  from `(now_epoch - generated_at_epoch + flash.age_sec)`. (In practice
  the frontend uses `flash.age_sec` directly because the JSON is
  refetched faster than meaningful drift accumulates.)
- `flashes[].sat` is `'g19'` or `'g18'`. Useful for debugging coverage
  gaps near the boundary (~120°W).

---

## File: `.github/workflows/update-lightning.yml`

### Cron cadence

`*/5 * * * *` — every 5 minutes. **GHA's documented minimum.** Anything
tighter (`*/1`, `*/2`, `*/3`) is silently downgraded by GitHub. We've hit
that downgrade in practice on `firestorm-aircraft-data`.

5-min cadence is acceptable because the 15-min window in `fetch_glm.py`
absorbs cron jitter. The frontend always sees the last 15 min of
strikes regardless of when the last cron actually ran.

### Concurrency

```yaml
concurrency:
  group: update-lightning
  cancel-in-progress: false
```

If a previous run is still going when the next cron fires, the new run
queues behind it rather than replacing it. NOAA's S3 list operations
can be slow during convective surges (10s+ of files to list); cancelling
mid-run would leave a corrupt state. Queue is correct.

### Permissions

```yaml
permissions:
  contents: write
```

Required for `GITHUB_TOKEN` to push commits back to the repo. Without
this, the commit step succeeds locally but the push gets a 403 error.

### Dependencies

```yaml
- name: Install deps
  run: pip install boto3 botocore netCDF4 numpy
```

Linux x86_64 wheels exist for all four packages on Python 3.11. No
apt packages or system libraries needed (`netCDF4` bundles HDF5 in its
wheel). Install time: ~30 seconds on a cold runner.

### Commit-only-if-changed pattern

```yaml
git add data/lightning.json
if git diff --cached --quiet; then
  echo 'No changes, skipping commit'
else
  git commit -m "..."
  git push
fi
```

Same pattern as the other `firestorm-*-data` pipelines. During a
quiet weather period the JSON may be near-identical between cycles
(timestamps roll, but counts and approximate flash locations stay
similar). Skipping the commit avoids the noise and keeps the git log
useful as a "lightning-active periods" timeline.

---

## Operational caveats

### GLM detection efficiency

- **CG strikes:** ~70% (vs NLDN's ~95%)
- **Total flashes (CG + IC):** much higher, ~90%+
- **Cloud obscuration:** GLM sees through most cloud layers but very
  dense overcast can attenuate the optical signal. Rare; typically
  affects <5% of flashes during a major MCS event.

### Coverage gaps

- GLM is geostationary at ~35,786 km altitude. Above ~55° latitude
  the view geometry degrades and detection efficiency drops.
- Below ~55° latitude, CONUS / Caribbean / Mexico / most of S. America
  are well-covered by G19; Pacific / Alaska / Hawaii / western CONUS
  by G18.
- Africa / Europe / Asia / Australia have **no GLM coverage**. The
  frontend's voice tool routes operators to `web_search` for those
  regions.
- Boundary overlap near 120°W: both G19 and G18 see strikes there; we
  do NOT dedupe across satellites because each emits its own
  observation independently. In practice this is a minor double-count
  (<1% of flashes) that doesn't affect operator decisions.

### Pipeline failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `g19=0` in counts every cycle | NOAA rotated to G20+ | Update `SATELLITES` in `fetch_glm.py`, probe new bucket |
| `g18=0` in counts every cycle | NOAA rotated to G19→West (unlikely) | Same |
| Workflow times out | NOAA S3 latency spike | Workflow has 5-min timeout; mostly self-recovers; if persistent, check NOAA status page |
| `data/lightning.json` not changing | Cron not running OR commit-skip path always hit (unlikely if window has flashes) | Check Actions tab for runs; manual `workflow_dispatch` if needed |
| Frontend `fetch()` fails | `raw.githubusercontent.com` down (rare) OR repo went private (don't do that) | Check repo visibility = public; check `curl https://raw.githubusercontent.com/...` directly |
| `netCDF4` import error in CI | Wheel missing for runner OS | Pin Python to 3.11; netCDF4 wheels are reliable on cp311-manylinux |

### Cost

| Component | Cost |
|---|---|
| NOAA Open Data S3 reads | $0 (NOAA pays; anonymous reads have no egress charge) |
| GHA cron (public repo) | $0 (free tier covers this; ~3 minutes/cycle × 12 cycles/hour = 36 min/hour, well under the 2000 min/month limit even for private repos) |
| GitHub raw CDN serving the JSON | $0 |
| Frontend `fetch()` from operator browsers | $0 |
| **Total** | **$0 / month** |

The pipeline is dependency-free on any paid AWS / SaaS service. As long
as NOAA continues publishing GLM (mandate is through the GOES-R series
end-of-life ~2036 at minimum) and GitHub continues offering free public
repo CI, this runs forever.

---

## Future work / not implemented

Things deliberately out of scope for v2_141:

1. **CG-only classification.** Would require NLDN cross-correlation
   ($) or hand-rolled stroke-detection from L1b events (complex, low
   value). If a fire IC needs CG-only, they should use NWS/Vaisala
   public dashboards.
2. **Cluster decay / dwell-time visualization.** The frontend currently
   renders each flash as a point; a "fading heatmap" of recent activity
   (last 30 min) is more operator-useful but requires frontend work,
   not pipeline work.
3. **Africa / Europe / Asia coverage.** EUMETSAT MTG-I has a comparable
   sensor (Lightning Imager) for Europe / Africa; no public S3 mirror
   yet. Japan's MP-PAWR could cover Asia. Both deferred until operator
   demand surfaces.
4. **S3 mirror to `firestorm-pipeline-data`.** The other
   `firestorm-*-data` pipelines mirror their JSON to an S3 bucket for
   redundancy. Lightning will follow the same pattern (see `Phase 9` in
   `firestorm-aws-build-guide.md`); not gating v2_141 ship.
5. **Per-satellite separate JSON files.** Could split into
   `lightning-east.json` + `lightning-west.json` if size becomes a
   concern. Not needed at 450 KB combined.
6. **Adaptive window sizing.** Could shrink the window during quiet
   periods and grow it during convective surges. Not worth the
   complexity; 15 min is fine.

---

## Cross-references

- **Frontend integration:** `~/Projects/firestorm/index.html` — search
  for `LIGHTNING_FEED_URL` (around line 8135), `query_lightning` voice
  tool (around line 13099), `query_region` lightning path (around line
  12572).
- **Session handoff:** `~/Projects/firestorm/SESSION_HANDOFF_2026-05-20_v141.md`
  — full context for the v2_139 → v2_141 arc.
- **Build guide:** `~/Projects/firestorm/firestorm-aws-build-guide.md`
  — Phase 5 lists `lightning.json` in the pipeline-S3 table; Phase 9
  documents the S3 mirror step.
- **Sister pipelines (same architectural pattern):**
  - `firestorm-aircraft-data` — ADSBx aircraft
  - `firestorm-wind-data` — GFS wind
  - `firestorm-hrrr-data` — HRRR thermo + wind
  - `firestorm-news-data` — GDELT news
  - `firestorm-aqi-data` — AirNow AQI
  - `firestorm-satellite-data` — MODIS / VIIRS
- **Standing rule that motivated this:** `feedback_demo_data_policy.md`
  in the FIRESTORM project memory.
