"""kv6_ret_subscriber.py – Brug‑monitor (RET, lijnen 3 & 5)

Luistert op NDOV `/RIG` en **meldt alleen** wanneer een RET‑tram van
lijn 3 of 5 de Willemsbrug (coördinaat‑box hieronder) oprijdt of verlaat.

Bericht‑logica
──────────────
* **Eerste** tram in de box →  "Begin met meten – tram <nr> op brug" + tijd.
* Als de teller terug op 0 gaat →  "Stop met meten – 0 trams op brug" + tijd.

Er wordt verder *niets* geprint; `output.txt` is niet meer nodig.
"""

from __future__ import annotations

import datetime as dt
import re
import tempfile
import time
from gzip import GzipFile
from io import BytesIO
from pathlib import Path
from typing import Dict, Final, List, Tuple, Set

import zmq

from netex_mapper import NetexMapper
from rd_converter import RDConverter

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR: Final[Path] = Path(__file__).resolve().parent
NETEX_FILE: Final[Path] = SCRIPT_DIR / "netex-ret-8-9-ndov.gz"

ZMQ_ENDPOINT: Final[str] = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ZMQ_TOPIC: Final[str] = "/RIG"

ALLOWED_LINES: Final[Set[str]] = {"3", "5"}

# GPS‑box rond de Willemsbrug
LAT_MIN, LON_MIN = 51.908765, 4.482431  # zuid‑west (lat, lon)
LAT_MAX, LON_MAX = 51.909434, 4.490950  # noord‑oost

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
# Subscriber
# ---------------------------------------------------------------------------
class BridgeMonitor:
    """Print start/stop messages when trams enter/leave the bridge box."""

    def __init__(self, mapper: NetexMapper, converter: RDConverter,
                 *, allowed_lines: Set[str]):
        self.mapper = mapper
        self.converter = converter
        self.allowed = {l.strip() for l in allowed_lines}
        self.on_bridge: Set[str] = set()  # vehiclenumbers currently in box

        ctx = zmq.Context.instance()
        self.sock = ctx.socket(zmq.SUB)
        self.sock.connect(ZMQ_ENDPOINT)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, ZMQ_TOPIC)

    # ------------------------
    def run(self) -> None:
        print(" Monitoring Willemsbrug (RET lijnen 3 & 5). Ctrl‑C to stop…")
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
                    err_path = tmpdir / f"kv6_err_{ts}.xml.gz"
                    with err_path.open("wb") as fh:
                        fh.write(b"".join(frames[1:]))
                    print(f" parse error {exc} – raw saved to {err_path}")
        except KeyboardInterrupt:
            print("Stopping…")
        finally:
            self.sock.close(); self.sock.context.term()

    # ------------------------
    @staticmethod
    def _extract_payload(frames: List[bytes]) -> str:
        raw = b"".join(frames)
        with GzipFile(fileobj=BytesIO(raw)) as gz:
            return gz.read().decode("utf-8", errors="replace")

    def _in_bridge_box(self, lat: float, lon: float) -> bool:
        return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX

    def _handle(self, xml_msg: str) -> None:
        if tag_value(xml_msg, "tmi8:dataownercode") != "RET":
            return

        # Publieke lijn filter
        internal = tag_value(xml_msg, "tmi8:lineplanningnumber")
        public_line = self.mapper.public_code(internal)
        if public_line not in self.allowed:
            return

        veh = tag_value(xml_msg, "tmi8:vehiclenumber")
        x_rd = tag_value(xml_msg, "tmi8:rd-x")
        y_rd = tag_value(xml_msg, "tmi8:rd-y")
        try:
            lat, lon = self.converter.rd_to_wgs(float(x_rd), float(y_rd))
        except ValueError:
            return  # invalid coords

        now_str = dt.datetime.now().strftime("%H:%M:%S")

        inside = self._in_bridge_box(lat, lon)
        before = len(self.on_bridge)

        if inside:
            if veh not in self.on_bridge:
                self.on_bridge.add(veh)
                if before == 0:
                    print(f"Begin met meten – tram {veh} op brug – tijd: {now_str}")
        else:
            if veh in self.on_bridge:
                self.on_bridge.remove(veh)
                if len(self.on_bridge) == 0:
                    print(f"Stop met meten – 0 trams op brug – tijd: {now_str}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mapper = NetexMapper(NETEX_FILE)
    conv = RDConverter()
    bm = BridgeMonitor(mapper, conv, allowed_lines=ALLOWED_LINES)
    bm.run()
