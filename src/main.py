#!/usr/bin/env python3
"""
Retrieve real-time location for RET tram line 24 (direction De Esch)
from two open data APIs and print the results.

APIs used
---------
1. **OVapi GTFS-RT** – publicly hosted Dutch feed that aggregates operator
   vehicle positions at https://gtfs.ovapi.nl/nl/vehiclePositions.pb
2. **Plannerstack NDOV GTFS-RT** – alternate GTFS-RT gateway provided by
   Plannerstack at http://gtfsrt.plannerstack.com/vehiclePositions

Both endpoints expose protobuf files conforming to the GTFS-Realtime
specification. The script fetches each feed, scans for RET vehicles that
belong to tram 24 whose head-sign indicates the destination “De Esch”,
and prints a human-readable one-liner per matching vehicle.

Installation
------------
Create a virtual-env and install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install requests gtfs-realtime-bindings protobuf
```

Running the script simply prints the latest coordinates:

```bash
python ret_tram24_locations.py
```

If no vehicles are matched (e.g. outside service hours) the script will
report that fact but still exit cleanly.
"""

import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2

# ---- configuration -------------------------------------------------------
OVAPI_URL = "https://gtfs.ovapi.nl/nl/vehiclePositions.pb"
PLANNERSTACK_URL = "http://gtfsrt.plannerstack.com/vehiclePositions"

# Line identification parameters
LINE_PUBLIC_NUMBER = {"24", "024"}  # some feeds left-pad with zeros
DESTINATION_KEYWORDS = ("DE ESCH", "ESCH")  # case-insensitive match
# DataOwnerCode for RET is "RET" but not all feeds expose it directly

# ---- helper functions ----------------------------------------------------

def fetch_feed(url: str) -> bytes:
    """Download feed and return raw protobuf bytes."""
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.content


def parse_feed(data: bytes, source: str):
    """Yield vehicle dicts for matching tram 24 → De Esch."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        trip = vp.trip

        # route_id often contains the public line number
        line = (trip.route_id or trip.trip_short_name or "").lstrip("0")
        headsign = trip.trip_headsign or ""

        match_line = line in LINE_PUBLIC_NUMBER
        match_dest = any(k.lower() in headsign.lower() for k in DESTINATION_KEYWORDS)

        if match_line and match_dest:
            yield {
                "source": source,
                "vehicle_id": vp.vehicle.id,
                "timestamp": datetime.fromtimestamp(vp.timestamp, tz=timezone.utc).isoformat(),
                "lat": vp.position.latitude,
                "lon": vp.position.longitude,
                "bearing": vp.position.bearing,
                "headsign": headsign,
            }

# ---- main ----------------------------------------------------------------

def main():
    all_matches = []

    for url, label in (
        (OVAPI_URL, "OVapi"),
        (PLANNERSTACK_URL, "Plannerstack"),
    ):
        try:
            data = fetch_feed(url)
            all_matches.extend(parse_feed(data, label))
        except Exception as exc:
            print(f"⚠️  {label} feed unavailable: {exc}")

    if not all_matches:
        print("No live RET tram 24 → De Esch vehicles reported right now.")
        return

    print("Live positions for RET tram 24 → De Esch:\n")
    for v in all_matches:
        print(
            f"[{v['source']}] {v['timestamp']}: id={v['vehicle_id']} "
            f"{v['lat']:.6f}, {v['lon']:.6f} (bearing {v['bearing']:.0f}°) — {v['headsign']}"
        )


if __name__ == "__main__":
    main()
