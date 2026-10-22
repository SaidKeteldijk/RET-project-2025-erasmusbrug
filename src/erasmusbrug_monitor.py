"""erasmusbrug_monitor.py – Meldt wanneer er een tram over de Erasmusbrug rijdt

Luistert op de NDOV KV6‑feed (`/RIG`) en meldt wanneer RET‑trams de
Erasmusbrug oprijden of verlaten.

Uitvoer
───────
    Begin met meten – tram 2148 (lijn 5, richting noord) op brug – tijd: 11:43:46
      + tram 2136 (lijn 5, richting zuid) op brug – 11:44:02 (2 trams)
      - tram 2148 van brug – 11:44:51 (1 tram)
    Stop met meten – 0 trams op brug – tijd: 11:45:20

* "Begin met meten" = de eerste tram rijdt de lege brug op.
* "Stop met meten"  = de laatste tram heeft de brug verlaten.
* Elke tram die op/af rijdt komt ook als regel in `metingen.csv`.

Wanneer staat een tram "op de brug"?
────────────────────────────────────
* Zijn RD‑positie ligt tussen de brugeinden en binnen `BRIDGE_BUFFER_M` meter
  van het tramspoor (middenlijn van OSM‑ways 45662401/64942350, 726 m).
* Alleen RET‑voertuigen met TransportMode "tram" volgens de NetEx; bussen en
  metro (die onder de rivier door rijdt) tellen niet mee.
* Trams sturen ongeveer elke 8 s een positie. Het moment waarop een tram het
  brugeinde passeert wordt geïnterpoleerd tussen de posities ervoor en
  erna; alle tijden komen uit het KV6‑bericht, niet van de pc‑klok.
* rd‑x/rd‑y = -1 (positie onbekend) verandert niets aan de status.
* Een tram op de brug die `STALE_AFTER_S` seconden niets stuurt, telt als weg.

De NetEx‑dienstregeling wordt bij het starten en daarna elke
`NETEX_REFRESH_S` seconden bijgewerkt (zie netex_updater.py).
"""

from __future__ import annotations

import csv
import datetime as dt
import math
import sys
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
import zlib
from dataclasses import dataclass
from gzip import GzipFile
from io import BytesIO
from pathlib import Path
from typing import Dict, Final, Iterator, List, Optional, Set, Tuple

import zmq

from netex_mapper import NetexMapper
from netex_updater import ensure_latest_netex

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR: Final[Path] = Path(__file__).resolve().parent
NETEX_CACHE_DIR: Final[Path] = SCRIPT_DIR / "netex_cache"
NETEX_REFRESH_S: Final[int] = 6 * 3600
EVENT_LOG: Final[Optional[Path]] = SCRIPT_DIR / "metingen.csv"  # None = geen bestand

ZMQ_ENDPOINT: Final[str] = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ZMQ_TOPIC: Final[str] = "/RIG"
STALL_TIMEOUT_S: Final[int] = 60   # geen enkel bericht → opnieuw verbinden

TRAM_MODE: Final[str] = "tram"     # NetEx TransportMode
STALE_AFTER_S: Final[int] = 90     # tram op brug zonder nieuwe positie → weg
MAX_INTERP_S: Final[int] = 60      # alleen interpoleren tussen posities ≤ 60 s uit elkaar

# Tramspoor op de Erasmusbrug (RD, meter), van de zuidkant (Wilhelminaplein)
# naar de noordkant (Leuvehaven). Middenlijn van OSM‑ways 45662401 en
# 64942350 (bridge=yes), stand 2026‑10‑22.
BRIDGE_TRACK_RD: Final[Tuple[Tuple[float, float], ...]] = (
    (93286, 435845),
    (93214, 435890),
    (92795, 436167),
    (92777, 436185),
    (92767, 436199),
    (92761, 436211),
    (92754, 436234),
    (92742, 436291),
)
BRIDGE_BUFFER_M: Final[float] = 20.0  # marge naast het spoor (GPS‑onnauwkeurigheid)

MAX_ERROR_DUMPS: Final[int] = 20

# ---------------------------------------------------------------------------
# Geometrie
# ---------------------------------------------------------------------------
_SEG_START: Final[List[float]] = [0.0]  # afstand langs het spoor bij elk knooppunt
for (_ax, _ay), (_bx, _by) in zip(BRIDGE_TRACK_RD, BRIDGE_TRACK_RD[1:]):
    _SEG_START.append(_SEG_START[-1] + math.hypot(_bx - _ax, _by - _ay))
BRIDGE_LENGTH_M: Final[float] = _SEG_START[-1]


def track_position(x: float, y: float) -> Tuple[float, float]:
    """Return ``(positie langs het spoor, afstand tot het spoor)`` in meter.

    De positie is 0 aan de zuidkant en `BRIDGE_LENGTH_M` aan de noordkant;
    vóór/na de brug loopt hij door (negatief of groter dan de lengte).
    """
    best = (math.inf, 0.0)
    last = len(BRIDGE_TRACK_RD) - 2
    for i, ((ax, ay), (bx, by)) in enumerate(zip(BRIDGE_TRACK_RD, BRIDGE_TRACK_RD[1:])):
        dx, dy = bx - ax, by - ay
        seg_len = math.hypot(dx, dy)
        t_raw = ((x - ax) * dx + (y - ay) * dy) / (seg_len * seg_len)
        t = max(0.0, min(1.0, t_raw))
        dist = math.hypot(x - (ax + t * dx), y - (ay + t * dy))
        if dist < best[0]:
            if (i == 0 and t_raw < 0) or (i == last and t_raw > 1):
                t = t_raw  # voorbij een brugeinde: doortellen
            best = (dist, _SEG_START[i] + t * seg_len)
    return best[1], best[0]


def is_on_bridge(chainage: float, dist: float) -> bool:
    """Tussen de twee brugeinden en hooguit `BRIDGE_BUFFER_M` naast het spoor."""
    return 0.0 <= chainage <= BRIDGE_LENGTH_M and dist <= BRIDGE_BUFFER_M

# ---------------------------------------------------------------------------
# KV6 helpers
# ---------------------------------------------------------------------------
KV6_TYPES: Final[Set[str]] = {
    "ARRIVAL", "ONSTOP", "DEPARTURE", "ONROUTE", "OFFROUTE", "INIT", "END", "DELAY",
}


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]  # strip namespace


def iter_kv6(payload: bytes) -> Iterator[Tuple[str, Dict[str, str]]]:
    """Yield ``(berichttype, velden)`` per KV6‑bericht, met of zonder ``tmi8:``‑prefix."""
    for el in ET.fromstring(payload).iter():
        kind = _local(el.tag)
        if kind in KV6_TYPES:
            yield kind, {_local(c.tag): (c.text or "").strip() for c in el}


def parse_rd(x_rd: str, y_rd: str) -> Optional[Tuple[float, float]]:
    """Return RD‑coördinaten, of ``None`` als ze ontbreken of ongeldig zijn (KV6: -1)."""
    try:
        x, y = float(x_rd), float(y_rd)
    except ValueError:
        return None
    if not (0 < x < 300_000 and 289_000 < y < 629_000):  # buiten Nederland / NaN
        return None
    return x, y


def parse_timestamp(value: str) -> Optional[dt.datetime]:
    try:
        ts = dt.datetime.fromisoformat(value) if value else None
    except ValueError:
        return None
    if ts is not None and ts.tzinfo is None:
        ts = ts.astimezone()
    return ts


def now_local() -> dt.datetime:
    return dt.datetime.now().astimezone()


def _trams(n: int) -> str:
    return f"{n} tram" if n == 1 else f"{n} trams"

# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------
@dataclass
class TramOnBridge:
    line: str
    direction: str  # "noord" (naar Leuvehaven) of "zuid" (naar Wilhelminaplein)
    seen: float     # time.monotonic() van de laatste positie op de brug


class BridgeMonitor:
    """Meldt wanneer trams de Erasmusbrug oprijden en verlaten."""

    def __init__(self, mapper: NetexMapper, *, event_log: Optional[Path] = EVENT_LOG):
        self.mapper = mapper
        self.event_log = event_log
        self.dump_dir = Path(tempfile.gettempdir())  # ruwe berichten bij parse‑fouten
        self.on_bridge: Dict[str, TramOnBridge] = {}
        self.last_ts: Dict[str, dt.datetime] = {}                 # voertuig → laatste KV6‑tijd
        self.last_pos: Dict[str, Tuple[dt.datetime, float]] = {}  # voertuig → (tijd, positie)
        self.unknown_codes: Set[str] = set()
        self.sock: Optional[zmq.Socket] = None
        self._stop = threading.Event()
        self._dumps = 0

    # ------------------------
    def run(self) -> None:
        print("Monitoring Erasmusbrug (RET-trams). Ctrl-C to stop...")
        self._connect()
        threading.Thread(target=self._refresh_netex_loop, daemon=True).start()
        last_msg = time.monotonic()
        try:
            while True:
                try:
                    if self.sock.poll(1000):  # timeout houdt Ctrl-C en time-outs werkend
                        frames = self.sock.recv_multipart()
                        last_msg = time.monotonic()
                        self._process(frames)
                    elif time.monotonic() - last_msg > STALL_TIMEOUT_S:
                        print(f"Geen KV6-berichten in {STALL_TIMEOUT_S} s - opnieuw verbinden")
                        self._connect()
                        last_msg = time.monotonic()
                except zmq.ZMQError as exc:
                    print(f"ZMQ-fout ({exc}) - opnieuw verbinden over 5 s")
                    time.sleep(5)
                    self._connect()
                    last_msg = time.monotonic()
                self._expire_stale()
        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            self._stop.set()
            if self.sock is not None:
                self.sock.close(linger=0)
            zmq.Context.instance().term()

    def _connect(self) -> None:
        if self.sock is not None:
            self.sock.close(linger=0)
        self.sock = zmq.Context.instance().socket(zmq.SUB)
        self.sock.connect(ZMQ_ENDPOINT)
        self.sock.setsockopt_string(zmq.SUBSCRIBE, ZMQ_TOPIC)

    # ------------------------
    def _process(self, frames: List[bytes]) -> None:
        if len(frames) < 2 or not frames[0].endswith(b"KV6posinfo"):
            return  # KV15/KV17 e.d. bevatten geen posities
        try:
            messages = list(iter_kv6(self._extract_payload(frames[1:])))
        except (OSError, EOFError, zlib.error, ET.ParseError) as exc:
            self._dump_error(frames, exc)
            return
        for kind, fields in messages:
            try:
                self._handle(kind, fields)
            except Exception as exc:  # één fout bericht mag de rest niet tegenhouden
                self._dump_error(frames, exc)

    @staticmethod
    def _extract_payload(frames: List[bytes]) -> bytes:
        with GzipFile(fileobj=BytesIO(b"".join(frames))) as gz:
            return gz.read()

    def _dump_error(self, frames: List[bytes], exc: BaseException) -> None:
        self._dumps += 1
        if self._dumps > MAX_ERROR_DUMPS:
            print(f"parse error {exc!r} (niet opgeslagen, al {MAX_ERROR_DUMPS} dumps)")
            return
        err_path = self.dump_dir / f"kv6_err_{int(time.time())}_{self._dumps}.xml.gz"
        with err_path.open("wb") as fh:
            fh.write(b"".join(frames[1:]))
        print(f"parse error {exc!r} - raw saved to {err_path}")

    def _refresh_netex_loop(self) -> None:
        while not self._stop.wait(NETEX_REFRESH_S):
            try:
                path = ensure_latest_netex(NETEX_CACHE_DIR)
                if path != self.mapper.path:
                    self.mapper = NetexMapper(path)  # atomair omwisselen
                    print(f"NetEx bijgewerkt: {path.name} ({len(self.mapper)} lijncodes)")
                    self.unknown_codes.clear()
            except Exception as exc:
                print(f"NetEx-update mislukt: {exc}")

    # ------------------------
    def _handle(self, kind: str, f: Dict[str, str]) -> None:
        if f.get("dataownercode") != "RET":
            return
        veh = f.get("vehiclenumber", "")
        if not veh:
            return

        # Oudere berichten (buiten volgorde binnengekomen) negeren
        ts = parse_timestamp(f.get("timestamp", ""))
        if ts is not None:
            prev = self.last_ts.get(veh)
            if prev is not None and ts < prev:
                return
            self.last_ts[veh] = ts
        when = ts or now_local()

        code = f.get("lineplanningnumber", "")
        hit = self.mapper.lookup(code)
        if hit is None and code not in self.unknown_codes:
            self.unknown_codes.add(code)
            print(f"Onbekende RET-lijncode {code!r} (niet in {self.mapper.path.name})")

        # Rit afgelopen, of voertuig rijdt nu geen tramrit → niet (meer) meetellen
        if kind == "END" or hit is None or hit[1] != TRAM_MODE:
            self._leave(veh, when, "rit beëindigd" if kind == "END" else "geen tramrit meer")
            return

        pos = parse_rd(f.get("rd-x", ""), f.get("rd-y", ""))
        if pos is None:
            return  # positie onbekend: niets aan de status veranderen

        chainage, dist = track_position(*pos)
        prev_pos = self.last_pos.get(veh)
        self.last_pos[veh] = (when, chainage)

        if is_on_bridge(chainage, dist):
            tram = self.on_bridge.get(veh)
            if tram is not None:
                tram.seen = time.monotonic()
                return
            if self._recent(prev_pos, when) and chainage != prev_pos[1]:
                northbound = chainage > prev_pos[1]  # rijrichting uit twee posities
            else:
                northbound = chainage < BRIDGE_LENGTH_M / 2  # dichtstbijzijnde brugeinde
            entry_end = 0.0 if northbound else BRIDGE_LENGTH_M
            self._enter(veh, hit[0], "noord" if northbound else "zuid",
                        self._passing_time(prev_pos, when, chainage, entry_end))
        elif veh in self.on_bridge:
            exit_end = BRIDGE_LENGTH_M if chainage > BRIDGE_LENGTH_M / 2 else 0.0
            self._leave(veh, self._passing_time(prev_pos, when, chainage, exit_end))

    @staticmethod
    def _recent(prev: Optional[Tuple[dt.datetime, float]], when: dt.datetime) -> bool:
        return prev is not None and 0 < (when - prev[0]).total_seconds() <= MAX_INTERP_S

    @classmethod
    def _passing_time(cls, prev: Optional[Tuple[dt.datetime, float]], when: dt.datetime,
                      chainage: float, end: float) -> dt.datetime:
        """Schat wanneer de tram brugeinde *end* passeerde (lineair tussen twee posities)."""
        if not cls._recent(prev, when) or chainage == prev[1]:
            return when
        t0, c0 = prev
        frac = min(1.0, max(0.0, (end - c0) / (chainage - c0)))
        return t0 + (when - t0) * frac

    def _enter(self, veh: str, line: str, direction: str, when: dt.datetime) -> None:
        self.on_bridge[veh] = TramOnBridge(line, direction, time.monotonic())
        n = len(self.on_bridge)
        if n == 1:
            print(f"Begin met meten – tram {veh} (lijn {line}, richting {direction}) "
                  f"op brug – tijd: {when:%H:%M:%S}")
        else:
            print(f"  + tram {veh} (lijn {line}, richting {direction}) op brug – "
                  f"{when:%H:%M:%S} ({_trams(n)})")
        self._log_event(when, veh, line, "op", direction, n, "begin" if n == 1 else "")

    def _leave(self, veh: str, when: dt.datetime, note: str = "") -> None:
        tram = self.on_bridge.pop(veh, None)
        if tram is None:
            return
        n = len(self.on_bridge)
        extra = f" ({note})" if note else ""
        if n == 0:
            print(f"Stop met meten – 0 trams op brug – tijd: {when:%H:%M:%S}{extra}")
        else:
            print(f"  - tram {veh} van brug – {when:%H:%M:%S} ({_trams(n)}){extra}")
        self._log_event(when, veh, tram.line, "af", tram.direction, n, "stop" if n == 0 else "", note)

    def _expire_stale(self) -> None:
        now = time.monotonic()
        for veh, tram in list(self.on_bridge.items()):
            if now - tram.seen > STALE_AFTER_S:
                self._leave(veh, now_local(), "geen signaal meer")

    def _log_event(self, when: dt.datetime, veh: str, line: str, event: str,
                   direction: str, count: int, measurement: str, note: str = "") -> None:
        if self.event_log is None:
            return
        new = not self.event_log.exists()
        with self.event_log.open("a", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["tijd", "voertuig", "lijn", "gebeurtenis", "richting",
                            "trams_op_brug", "meting", "opmerking"])
            w.writerow([when.isoformat(timespec="seconds"), veh, line, event, direction,
                        count, measurement, note])

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # UTF‑8 + direct wegschrijven, ook als de uitvoer naar een bestand gaat
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    try:
        netex = ensure_latest_netex(NETEX_CACHE_DIR)
    except Exception as exc:
        sys.exit(f"Kan de NetEx-dienstregeling niet downloaden en er is geen lokale kopie: {exc}")
    mapper = NetexMapper(netex)
    print(f"NetEx: {netex.name} ({len(mapper)} lijncodes)")
    if mapper.is_expired():
        print(f"LET OP: deze dienstregeling is verlopen op {mapper.valid_to:%Y-%m-%d}")
    BridgeMonitor(mapper).run()
