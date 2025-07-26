#!/usr/bin/env python3
"""
line_lookup.py – vertaal lineplanningnumber ➜ publiekslijn

• Leest automatisch de bestanden netex‑ret.gz en netex‑htm.gz
  (in dezelfde map als dit script).
• Bouwt bij start een dictionary {internal_id: public_line}.
• Werkt zowel met een command‑line‑argument als in interactieve modus.
"""

from gzip import GzipFile
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

# ------------------------------------------
NETEX_FILES = [
    Path(__file__).with_name("netex-ret.gz"),
    Path(__file__).with_name("netex-htm.gz"),
]
NS = {"n": "http://www.netex.org.uk/netex"}
# ------------------------------------------


def build_mapping() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for p in NETEX_FILES:
        if not p.exists():
            print(f"[!] Waarschuwing: {p.name} niet gevonden – overgeslagen")
            continue
        with GzipFile(p) as f:
            root = ET.parse(f).getroot()
        for line in root.findall(".//n:Line", NS):
            internal = (
                line.findtext("n:PrivateCode", namespaces=NS)
                or line.findtext("n:LineRef", namespaces=NS)
            )
            public = (
                line.findtext("n:PublicCode", namespaces=NS)
                or line.findtext("n:ShortName", namespaces=NS)
                or line.findtext("n:Name", namespaces=NS)
            )
            if internal and public:
                mapping[internal] = public
    return mapping


def lookup(mapping: dict[str, str], code: str) -> str:
    """Geef publiekslijn of 'onbekend' terug."""
    return mapping.get(code, f"onbekend ({code})")


def main() -> None:
    mapping = build_mapping()
    if len(sys.argv) > 1:  # één argument meegegeven
        print(lookup(mapping, sys.argv[1]))
        return

    # interactieve modus
    print("Interactieve modus – typ internal lineplanningnumber (Ctrl‑C om te stoppen)")
    try:
        while True:
            code = input("internal> ").strip()
            if not code:
                continue
            print(lookup(mapping, code))
    except (EOFError, KeyboardInterrupt):
        print("\n[exit]")


if __name__ == "__main__":
    main()
