"""RET Citadis‑only KV6 subscriber

Listens to the NDOV ``/RIG`` PUB/SUB endpoint and prints **only** those
messages that

1. come from RET (<tmi8:dataownercode> == "RET"), **and**
2. concern a Rotterdam Citadis tram – i.e. a 4‑digit ``<vehiclenumber>``
   in the ranges 2001‑2060 (Citadis I) or 2101‑2153 (Citadis II).

No conversion of internal ``lineplanningnumber`` codes is done yet – that
will follow in a later step.
"""

from __future__ import annotations

import re
import time
from gzip import GzipFile
from io import BytesIO
from typing import Final, List

import zmq

# ---------------------------------------------------------------------------
# ZeroMQ setup
# ---------------------------------------------------------------------------

def create_subscriber(endpoint: str = "tcp://pubsub.besteffort.ndovloket.nl:7658",
                      topic: str = "/RIG") -> zmq.Socket:
    """Return a SUB socket that is already connected and subscribed."""
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    return sock


# ---------------------------------------------------------------------------
# Pre‑compiled regexes & helper functions
# ---------------------------------------------------------------------------

OWNER_RE: Final[re.Pattern[str]] = re.compile(r"<tmi8:dataownercode>([^<]+)</tmi8:dataownercode>")
VEHICLE_RE: Final[re.Pattern[str]] = re.compile(r"<tmi8:vehiclenumber>(\d+)</tmi8:vehiclenumber>")

# Hard‑coded Citadis ranges (inclusive)
CITADIS_RANGES: Final[List[tuple[int, int]]] = [
    (2001, 2060),  # Citadis I (2035 is scrapped, but we keep the range simple)
    (2101, 2153),  # Citadis II
]


def extract_payload(frames: list[bytes]) -> str:
    """Concatenate, gunzip and decode the payload part of a multipart message."""
    raw = b"".join(frames)
    with GzipFile(fileobj=BytesIO(raw)) as gz:
        return gz.read().decode("utf‑8", errors="replace")


def is_ret_message(xml: str) -> bool:
    m = OWNER_RE.search(xml)
    return bool(m and m.group(1) == "RET")


def is_citadis_tram(xml: str) -> bool:
    """Return *True* when <vehiclenumber> is within the Citadis ranges."""
    m = VEHICLE_RE.search(xml)
    if not m:
        return False
    num = int(m.group(1))
    return any(start <= num <= end for start, end in CITADIS_RANGES)


# ---------------------------------------------------------------------------
# Main receive loop
# ---------------------------------------------------------------------------

def main() -> None:
    subscriber = create_subscriber()
    print("🚋  Listening for RET **Citadis** updates …  (press Ctrl‑C to stop)")

    try:
        while True:
            frames = subscriber.recv_multipart()
            address, payload_frames = frames[0], frames[1:]
            try:
                xml = extract_payload(payload_frames)
                if is_ret_message(xml) and is_citadis_tram(xml):
                    print(xml)
            except Exception as exc:
                ts = int(time.time())
                path = f"/tmp/ndov_error_{ts}.xml.gz"
                with open(path, "wb") as fh:
                    fh.write(b"".join(payload_frames))
                print(f"⚠️  {ts}: failed to process message on {address!r} – saved to {path} ({exc})")
    except KeyboardInterrupt:
        print("Interrupted by user – shutting down …")
    finally:
        subscriber.close()
        subscriber.context.term()


if __name__ == "__main__":
    main()
