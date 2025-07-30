#!/usr/bin/env python3
"""
feedchecker.py
==============

• Leest mapping publiekslijn ↔ internal uit netex‑ret.gz & netex‑htm.gz
  (de bestanden moeten in dezelfde map staan als dit script).
• Luistert op ZeroMQ KV6 best‑effort‑feed (tcp://…:7658) – envelope /RET.
• Verwerpt HTM‑berichten.
• Toont alléén publiekslijnen 3 en 5 (RET‑trams).
• Duplicate‑filter op (timestamp, vehiclenumber, journeynumber).
"""

from dataclasses import dataclass
from gzip import GzipFile
from io import BytesIO
from pathlib import Path
import xml.etree.ElementTree as ET
import zmq

# ───────── instellingen ─────────
ENDPOINT    = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ENVELOPE    = "/RIG"                      # RET‑feed
NETEX_FILES = ["netex-ret-28-7-ndov.gz", "netex-htm.gz"]
SKIP_OWNER  = "HTM"
KEEP_LINES  = {"3", "5"}                  # alleen tramlijnen 3 en 5
# ─────────────────────────────────


# ───────── dataclass ─────────
@dataclass
class Kv6Update:
    owner: str
    line_int: str
    line_pub: str
    timestamp: str
    veh: str
    journey: str
    rd_x: str
    rd_y: str

    def key(self) -> tuple[str, str, str]:
        return self.timestamp, self.veh, self.journey

    def pretty(self) -> str:
        return (
            f"Vervoerder: {self.owner}\n"
            f"Lijn: {self.line_pub}  (intern {self.line_int})\n"
            f"Tijd: {self.timestamp}\n"
            f"Voertuignummer: {self.veh}\n"
            f"Ritnummer: {self.journey}\n"
            f"rd‑x: {self.rd_x}\n"
            f"rd‑y: {self.rd_y}\n"
            "─" * 40
        )


# ───────── helperfuncties ─────────
def gunzip(buf: bytes) -> str:
    """De‑compressie naar UTF‑8‑string."""
    return GzipFile("", "r", 0, BytesIO(buf)).read().decode("utf-8", "replace")


def txt(elem: ET.Element, tag: str, ns: dict) -> str:
    """Return .text of sub‑element (of ''), altijd een str."""
    sub = elem.find(tag, ns)
    return (sub.text or "").strip() if sub is not None else ""


def build_mapping() -> dict[str, str]:
    """Lees NeTEx‑gz’s en maak {internal → publiekslijn}."""
    ns = {"n": "http://www.netex.org.uk/netex"}
    mapping: dict[str, str] = {}

    for fname in NETEX_FILES:
        path = Path(__file__).with_name(fname)
        if not path.exists():
            print(f"[!] {fname} niet gevonden – overgeslagen")
            continue

        with GzipFile(path) as f:
            root = ET.parse(f).getroot()

        for line in root.findall(".//n:Line", ns):
            internal = txt(line, "n:PrivateCode", ns) or txt(line, "n:LineRef", ns)
            public   = (
                txt(line, "n:PublicCode", ns)
                or txt(line, "n:ShortName", ns)
                or txt(line, "n:Name", ns)
            )
            if internal and public:
                mapping[internal] = public

    print(f"[info] mapping geladen – {len(mapping)} lijn‑ID’s.")
    return mapping


def parse_update(xml: str, mapping: dict[str, str]) -> Kv6Update | None:
    root = ET.fromstring(xml)
    for rec in root.iter():
        if rec.tag.split('}')[-1] in {"ONROUTE", "ONSTOP", "ARRIVAL"}:
            d = {c.tag.split('}')[-1]: (c.text or "") for c in rec}
            line_int = d.get("lineplanningnumber", "")
            return Kv6Update(
                owner      = d.get("dataownercode", ""),
                line_int   = line_int,
                line_pub   = mapping.get(line_int, line_int),
                timestamp  = d.get("timestamp", ""),
                veh        = d.get("vehiclenumber", ""),
                journey    = d.get("journeynumber", ""),
                rd_x       = d.get("rd-x", ""),
                rd_y       = d.get("rd-y", ""),
            )
    return None


# ───────── main ─────────
def main() -> None:
    mapping = build_mapping()

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(ENDPOINT)
    sub.setsockopt_string(zmq.SUBSCRIBE, ENVELOPE)

    seen: set[tuple[str, str, str]] = set()
    print(f"▶ Luisteren op {ENDPOINT}{ENVELOPE} – alleen lijnen {', '.join(KEEP_LINES)}")

    try:
        while True:
            _, payload = sub.recv_multipart()
            upd = parse_update(gunzip(payload), mapping)
            if not upd:
                continue
            if upd.owner == SKIP_OWNER:
                continue                      # HTM overslaan
            if upd.line_pub not in KEEP_LINES:
                continue                      # niet lijn 3/5
            if upd.key() in seen:
                continue                      # duplicate
            seen.add(upd.key())

            print(upd.pretty())

    except KeyboardInterrupt:
        print("\n[Stop]")
    finally:
        sub.close()
        ctx.term()


if __name__ == "__main__":
    main()
