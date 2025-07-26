#!/usr/bin/env python3
"""
Best‑effort KV6 listener + publiekslijn mapping (RET & HTM)
==========================================================
* Leest **twee** NeTEx‑bestanden in dezelfde map als dit script:
    • `netex-ret.gz`
    • `netex-htm.gz`
  en bouwt één mapping `lineplanningnumber → publiekslijn`.
* Luistert op best‑effort ZeroMQ :7658, envelope **/RIG**.
  (Daar verschijnen zowel RET‑ als HTM‑berichten.)
* **Geen verdere filtering:** elk ontvangen frame wordt uitgepakt,
  publiekslijn lookup gedaan (indien beschikbaar) en weggeschreven.
* Console‑output én logfile **Output/rig_feed_public.txt**.

Afhankelijkheden
----------------
```bash
pip install pyzmq lxml  # lxml alleen nodig voor snelle XML‑parse
```

Gebruik
-------
```bash
python main.py
```
Ctrl‑C stopt de sessie.
"""

from gzip import GzipFile
from io import BytesIO
from pathlib import Path
import time
import xml.etree.ElementTree as ET

import zmq

# ───────────── instellingen ─────────────
# beide NeTEx‑bestanden liggen naast main.py
NETEX_FILES = [
    Path(__file__).with_name("netex-ret.gz"),
    Path(__file__).with_name("netex-htm.gz"),
]

ENDPOINT = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ENVELOPE = "/RIG"          # bevat o.a. RET & HTM
LOG_FILE = (
    Path(__file__).resolve().parent.parent / "Output" / "rig_feed_public.txt"
)
# ─────────────────────────────────────────


def build_mapping(files: list[Path]) -> dict[str, str]:
    """Bouw mapping uit meerdere NeTEx‑gz bestanden."""
    mapping: dict[str, str] = {}
    ns = {"n": "http://www.netex.org.uk/netex"}
    for p in files:
        if not p.exists():
            print(f"[!] Waarschuwing: {p} niet gevonden → overgeslagen")
            continue
        print(f"→ mapping laden uit {p.name}…")
        with GzipFile(p) as f:
            root = ET.parse(f).getroot()
        for line in root.findall(".//n:Line", ns):
            internal = (
                line.findtext("n:PrivateCode", namespaces=ns)
                or line.findtext("n:LineRef", namespaces=ns)
            )
            public = (
                line.findtext("n:PublicCode", namespaces=ns)
                or line.findtext("n:ShortName", namespaces=ns)
            )
            if internal and public:
                mapping[internal] = public
    print(f"→ totaal {len(mapping)} lijn‑ID’s gemapt.\n")
    return mapping


def gunzip(buf: bytes) -> str:
    return GzipFile("", "r", 0, BytesIO(buf)).read().decode("utf-8", "replace")


def main() -> None:  # noqa: CCR001
    mapping = build_mapping(NETEX_FILES)

    LOG_FILE.parent.mkdir(exist_ok=True)
    log = LOG_FILE.open("a", encoding="utf-8")
    log.write(f"\n===== sessie {time.strftime('%F %T')} =====\n")
    log.flush()

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(ENDPOINT)
    sub.setsockopt_string(zmq.SUBSCRIBE, ENVELOPE)

    print(
        f"▶ Luisteren op {ENDPOINT}  envelope={ENVELOPE}  → log {LOG_FILE}\n"
        f"  mapping: {', '.join(p.name for p in NETEX_FILES if p.exists())}"
    )

    try:
        while True:
            topic, payload = sub.recv_multipart()
            xml = gunzip(payload)

            # publiekslijn (optioneel)
            try:
                lp_num = xml.split("<tm18:lineplanningnumber>")[1].split("<")[0]
            except IndexError:
                lp_num = "?"
            public = mapping.get(lp_num, lp_num)

            header = (
                f"\n--- {time.strftime('%F %T')}  topic {topic.decode()}  "
                f"publiekslijn {public} (internal {lp_num}) ---"
            )
            print(header)
            print(xml)

            log.write(header + "\n" + xml + "\n")
            log.flush()

    except KeyboardInterrupt:
        print("\n[Stop]")
    finally:
        log.close()
        sub.close()
        ctx.term()


if __name__ == "__main__":
    main()
