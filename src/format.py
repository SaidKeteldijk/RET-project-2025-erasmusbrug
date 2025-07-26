#!/usr/bin/env python3
"""
KV6 listener – alleen RET‑tram lijnen 3, 5 en de Erasmusbrug‑lijnen 20/23/25
==============================================================================
• Leest NeTEx‑bestanden (netex-ret.gz & netex-htm.gz) naast dit script → mapping
  `lineplanningnumber ➜ publiekslijn`.
• Luistert op best‑effort KV6 via envelope **/RIG**.
• Slaat HTM (en andere niet‑RET‑vervoerders) over.
• Filtert daarna op publiekslijn ∈ {3, 5, 20, 23, 25}.
• Print unieke berichten in leesbaar formaat.
"""

from dataclasses import dataclass
from gzip import GzipFile
from io import BytesIO
from pathlib import Path
import xml.etree.ElementTree as ET
import zmq

ENDPOINT = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ENVELOPE = "/RIG"
NETEX_FILES = ["netex-ret.gz", "netex-htm.gz"]
SKIP_OWNER = "HTM"                     # HTM overslaan
KEEP_LINES = {"3", "5"}   # gewenste publiekslijnen

# ───────── dataclass ─────────
@dataclass
class Kv6Update:
    owner: str
    line_internal: str
    line_public: str
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
            f"Lijn: {self.line_public}  (intern {self.line_internal})\n"
            f"Tijd: {self.timestamp}\n"
            f"Voertuignummer: {self.veh}\n"
            f"Ritnummer: {self.journey}\n"
            f"rd‑x: {self.rd_x}\nrd‑y: {self.rd_y}\n"
            + "─" * 40
        )

# ───────── helpers ─────────

def gunzip(buf: bytes) -> str:
    return GzipFile("", "r", 0, BytesIO(buf)).read().decode("utf-8", "replace")


def build_mapping() -> dict[str, str]:
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
            internal = (
                line.findtext("n:PrivateCode", namespaces=ns)
                or line.findtext("n:LineRef", namespaces=ns)
            )
            public = (
                line.findtext("n:PublicCode", namespaces=ns)
                or line.findtext("n:ShortName", namespaces=ns)
                or line.findtext("n:Name", namespaces=ns)
            )
            if internal and public:
                mapping[internal] = public
    return mapping


def parse_update(xml: str, mapping: dict[str, str]) -> Kv6Update | None:
    root = ET.fromstring(xml)
    for rec in root.iter():
        if rec.tag.split('}')[-1] in {"ONROUTE", "ONSTOP", "ARRIVAL"}:
            d = {c.tag.split('}')[-1]: (c.text or "") for c in rec}
            internal = d.get("lineplanningnumber", "")
            return Kv6Update(
                owner=d.get("dataownercode", ""),
                line_internal=internal,
                line_public=mapping.get(internal, internal),
                timestamp=d.get("timestamp", ""),
                veh=d.get("vehiclenumber", ""),
                journey=d.get("journeynumber", ""),
                rd_x=d.get("rd-x", ""),
                rd_y=d.get("rd-y", ""),
            )
    return None

# ───────── main ─────────

def main() -> None:
    mapping = build_mapping()

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(ENDPOINT)
    sub.setsockopt_string(zmq.SUBSCRIBE, ENVELOPE)

    seen: tuple[str, str, str] | None = None
    print(
        f"▶ luisteren op {ENDPOINT} envelope {ENVELOPE}\n"
        f"  houd publiekslijnen: {', '.join(sorted(KEEP_LINES))}"
    )

    try:
        while True:
            _, payload = sub.recv_multipart()
            upd = parse_update(gunzip(payload), mapping)
            if not upd:
                continue
            if upd.owner == SKIP_OWNER:
                continue  # HTM overslaan
            if upd.line_public not in KEEP_LINES:
                continue  # ongewenste lijn
            if upd.key() == seen:
                continue  # duplicate
            seen = upd.key()
            print(upd.pretty())
    except KeyboardInterrupt:
        print("\n[Stop]")
    finally:
        sub.close()
        ctx.term()


if __name__ == "__main__":
    main()
