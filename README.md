# firestorm-lightning-data

Real-time lightning feed for [FIRESTORM](https://github.com/Deasus/Firestorm),
mirroring GOES-R GLM Level-2 LCFA flashes from NOAA's Open Data S3 buckets
to a slim JSON the FIRESTORM frontend reads via `raw.githubusercontent.com`.

Same architectural shape as `firestorm-aircraft-data`, `firestorm-wind-data`,
`firestorm-news-data`: GitHub Actions cron poll → public S3 source → slim
JSON → frontend `fetch()`.

## What this replaces

Until v2_141, FIRESTORM's lightning layer rendered 40 randomly-generated
points around hand-picked region centroids (`generateDemoLightning(40)`).
The visual presentation gave no indication the data was synthetic. This
pipeline replaces that demo path with real strikes.

## Source

GOES-R Geostationary Lightning Mapper (GLM) Level-2 Lightning Cluster
Filter Algorithm (LCFA) product:

- **GOES-East (G19):** `s3://noaa-goes19/GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/` (replaced G16 in April 2025)
- **GOES-West (G18):** `s3://noaa-goes18/GLM-L2-LCFA/<YYYY>/<DDD>/<HH>/` (replaced G17 in 2023)

Public, anonymous, no auth, no egress charge. NOAA Open Data on AWS.

Native cadence: ~20 seconds per satellite. Each file is netCDF-4,
~50–500 KB.

## Output

`data/lightning.json` — flat JSON consumed by the FIRESTORM frontend.

```json
{
  "generated_at": "2026-05-20T22:00:00+00:00",
  "window_minutes": 15,
  "counts": { "total": 1234, "g19": 800, "g18": 434 },
  "source": "NOAA GOES-R GLM L2 LCFA via NOAA Open Data on AWS S3 (public, no auth)",
  "flashes": [
    { "lat": 38.4, "lng": -98.6, "energy_fJ": 142.7, "age_sec": 47, "type": "flash", "sat": "g19" },
    ...
  ]
}
```

Cap of 5000 flashes per cycle, sorted by energy descending. The frontend
only renders ~100–500 markers usefully, so the cap biases toward the
strongest / most operationally meaningful strikes.

## Caveats (read these once)

- **GLM cannot distinguish CG from IC by itself.** It's an optical sensor
  measuring the flash bloom in the 777.4 nm oxygen line; ground networks
  measure the EM pulse. Every flash is labeled `type: "flash"`. CG-only
  classification requires NLDN cross-correlation — out of scope here.
- **Detection efficiency:** ~70% for CG strikes, much higher for total
  flash count. Fine for situational awareness; not a substitute for
  NLDN if sub-second latency or guaranteed CG detection matters.
- **End-to-end latency:** strike → JSON ≈ 2–7 minutes. (Strike → GLM
  downlink ~30s → ground processing ~30s → S3 publish ~30s → cron poll
  up to 5 min → push.)
- **Coverage gaps:** GOES is geostationary at ~35,786 km altitude. Polar
  regions above ~55° latitude have degraded view geometry. CONUS,
  Caribbean, Mexico, most of S. America (G16) + Pacific, Alaska, western
  CONUS (G18) are well covered.

## Running locally

```bash
pip install boto3 botocore netCDF4 numpy
python fetch_glm.py
# → writes data/lightning.json
```

No credentials needed. The script uses `boto3` with `UNSIGNED` config
against NOAA's public buckets.

## Cost

$0. NOAA pays for the bucket; GHA cron is free for public repos; no AWS
account required to read the data.

## License

Pipeline code is not licensed for redistribution outside FIRESTORM.
The underlying GLM data is U.S. federal government produced and is in
the public domain.
