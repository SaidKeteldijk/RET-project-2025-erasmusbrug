"""kv6_ret_subscriber.py – RET‑only KV6 stream logger

Combine :class:`NetexMapper` and :class:`RDConverter` to subscribe to the
NDOV KV6 vehicle feed and log nicely formatted blocks to console and
``output.txt``.
"""

from __future__ import annotations

import datetime as dt
import re
import tempfile
import time
from gzip import GzipFile
from io import BytesIO
from pathlib import Path
from typing import Dict, Final, List, Tuple

import zmq

from netex_mapper import NetexMapper
from rd_converter import RDConverter

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR: Final[Path] = Path(__file__).resolve().parent
NETEX_FILE: Final[Path] = SCRIPT_DIR / "netex-ret-28-7-ndov.gz"
OUTPUT_FILE: Final[Path] = SCRIPT_DIR / "output.txt"

ZMQ_ENDPOINT: Final[str] = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ZMQ_TOPIC: Final[str] = "/RIG"

# ---------------------------------------------------------------------------
# KV6 helpers
# ---------------------------------------------------------------------------
MSG_RE: Final[re.Pattern[str]] = re.compile(r"<tmi8:(ONROUTE|ONSTOP).*?</tmi8:\1>", re.DOTALL)

_TAG_CACHE: Dict[str, re.Pattern[str]] = {}


def tag_value(xml: str, tag: str) -> str:
    patt = _TAG_CACHE.setdefault(tag, re.compile(rf"<{tag}>([^<]*)</{tag}>", re.DOTALL))
    m = patt.search(xml)
    return m.group(1).strip() if m else ""

TAGS: Tuple[str, ...] = (
    "tmi8:dataownercode",
    "tmi8:timestamp",
    "tmi8:operatingday",
    "tmi8:lineplanningnumber",
    "tmi8:vehiclenumber",
    "tmi8:journeynumber",
    "tmi8:rd-x",
    "tmi8:rd-y",
)

# ---------------------------------------------------------------------------
# Subscriber class
# ---------------------------------------------------------------------------
class KV6Subscriber:
    """RET‑only subscriber that logs formatted messages."""

    def __init__(self, mapper: NetexMapper, converter: RDConverter, *,
                 endpoint: str = ZMQ_ENDPOINT, topic: str = ZMQ_TOPIC,
                 output_file: Path = OUTPUT_FILE):
        self.mapper = mapper
        self.converter = converter
        self.output_file = output_file

        ctx = zmq.Context.instance()
        self.sock = ctx.socket(zmq.SUB)
        self.sock.connect(endpoint)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, topic)

    # ------------------------
    # Main loop
    # ------------------------
    def run(self) -> None:
        print(f"📡  Listening on {ZMQ_TOPIC} (RET only)…  Ctrl‑C to stop.")
        tmpdir = Path(tempfile.gettempdir())
        try:
            while True:
                frames = self.sock.recv_multipart()
                xml_payload = self._extract_payload(frames[1:])
                try:
                    for m in MSG_RE.finditer(xml_payload):
                        self._handle(m.group(0))
                except Exception as exc:
                    ts = int(time.time())
                    err_path = tmpdir / f"kv6_error_{ts}.xml.gz"
                    with err_path.open("wb") as fh:
                        fh.write(b"".join(frames[1:]))
                    print(f"⚠️  parse error {exc} – raw saved to {err_path}")
        except KeyboardInterrupt:
            print("Stopping…")
        finally:
            self.sock.close(); self.sock.context.term()

    # ------------------------
    # Internals
    # ------------------------
    @staticmethod
    def _extract_payload(frames: List[bytes]) -> str:
        raw = b"".join(frames)
        with GzipFile(fileobj=BytesIO(raw)) as gz:
            return gz.read().decode("utf-8", errors="replace")

    def _formatted_block(self, data: Dict[str, str]) -> str:
        return (
            f"Vervoerder: {data['tmi8:dataownercode']}\n"
            f"tijd: {data['tijd']}\n"
            f"dag: {data['tmi8:operatingday']}\n"
            f"Lineplanningsnummer: {data['publiek_line']}\n"
            f"voertuignummer: {data['tmi8:vehiclenumber']}\n"
            f"ritnummer: {data['tmi8:journeynumber']}\n"
            f"rd-x: {data['tmi8:rd-x']}\n"
            f"rd-y: {data['tmi8:rd-y']}\n"
            f"lat: {data['lat']}\n"
            f"lon: {data['lon']}\n"
        )

    def _log(self, text: str) -> None:
        with self.output_file.open("a", encoding="utf-8") as fh:
            fh.write(text + "\n")

    def _handle(self, xml_msg: str) -> None:
        if tag_value(xml_msg, "tmi8:dataownercode") != "RET":
            return

        data = {tag: tag_value(xml_msg, tag) for tag in TAGS}

        # timestamp → tijd
        t_iso = data.get("tmi8:timestamp", "")
        try:
            tijd = dt.datetime.fromisoformat(t_iso).time().isoformat(timespec="seconds") if t_iso else ""
        except ValueError:
            tijd = t_iso
        data["tijd"] = tijd

        # publieke lijn
        internal = data.get("tmi8:lineplanningnumber", "")
        data["publiek_line"] = self.mapper.public_code(internal)

        # RD → WGS
        try:
            x_rd = float(data.get("tmi8:rd-x", ""))
            y_rd = float(data.get("tmi8:rd-y", ""))
            lat, lon = self.converter.rd_to_wgs(x_rd, y_rd)
            data["lat"] = f"{lat:.6f}"
            data["lon"] = f"{lon:.6f}"
        except ValueError:
            data["lat"] = data["lon"] = ""

        block = self._formatted_block(data)
        print(block)
        self._log(block)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mapper = NetexMapper(NETEX_FILE)
    converter = RDConverter()
    sub = KV6Subscriber(mapper, converter)
    print(f"🔄  Loaded {len(mapper._map)} line‑codes from NetEx.  Output ➜ {OUTPUT_FILE}\n")
    sub.run()
