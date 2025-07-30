"""RET-only KV6 subscriber

Listens to the NDOV ``/RIG`` PUB/SUB endpoint and prints ONLY those
messages where <tmi8:dataownercode> == "RET" (Rotterdamse Elektrische
Tram).  Messages from HTM or any other operator are silently ignored.

No further filtering (routes, vehicles, etc.) is done yet – that will
come in the next steps of the project.
"""

from __future__ import annotations

import re
import time
from gzip import GzipFile
from io import BytesIO

import zmq

# ---------------------------------------------------------------------------
# ZeroMQ setup – same as the example, but wrapped in a context‑manager‑style
# function so that the socket is always closed even on Ctrl‑C.
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
# Simple helper utilities
# ---------------------------------------------------------------------------

# Pre‑compiled regex to find the first <tmi8:dataownercode>...</tmi8:dataownercode>
OWNER_RE = re.compile(r"<tmi8:dataownercode>([^<]+)</tmi8:dataownercode>")


def extract_payload(frames: list[bytes]) -> str:
    """Concatenate, gunzip and decode the payload part of a multipart message."""
    raw = b"".join(frames)
    with GzipFile(fileobj=BytesIO(raw)) as gz:
        return gz.read().decode("utf‑8", errors="replace")


def is_ret_message(xml: str) -> bool:
    """Return *True* when the <dataownercode> is 'RET'."""
    m = OWNER_RE.search(xml)
    return bool(m and m.group(1) == "RET")


# ---------------------------------------------------------------------------
# Main receive loop
# ---------------------------------------------------------------------------

def main() -> None:
    subscriber = create_subscriber()
    print("🚋  Listening for RET vehicle updates …  (press Ctrl‑C to stop)")

    try:
        while True:
            frames = subscriber.recv_multipart()
            address, payload_frames = frames[0], frames[1:]
            try:
                xml = extract_payload(payload_frames)
                if is_ret_message(xml):
                    print(xml)
            except Exception as exc:
                # Dump the problematic payload for later debugging
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
