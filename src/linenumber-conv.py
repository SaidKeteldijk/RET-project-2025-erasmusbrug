"""kv6_ret_subscriber.py
========================

Streams **RET** messages from NDOV `/RIG`, converts internal
`<lineplanningnumber>` to the public line number (via NetEx
`netex-ret-28-7-ndov.gz` in the same folder) and prints each message in
a clear block **and** appends the same block to `output.txt` (created in
this folder if it doesn’t exist).

Output format example
─────────────────────
    Vervoerder: RET
    tijd: 17:52:49
    dag: 2025-07-30
    Lineplanningsnummer: 76
    voertuignummer: 1208
    ritnummer: 9050
    rd-x: 93953
    rd-y: 433317

Only RET records are processed; HTM and others are ignored.
"""

from __future__ import annotations

import datetime as dt
import re
import tempfile
import time
import xml.etree.ElementTree as ET
from gzip import GzipFile
from io import BytesIO
from pathlib import Path
from typing import Dict, Final, List, Tuple

import zmq

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR: Final[Path] = Path(__file__).resolve().parent
NETEX_FILE: Final[Path] = SCRIPT_DIR / "netex-ret-28-7-ndov.gz"
OUTPUT_FILE: Final[Path] = SCRIPT_DIR / "output.txt"

ZMQ_ENDPOINT: Final[str] = "tcp://pubsub.besteffort.ndovloket.nl:7658"
ZMQ_TOPIC: Final[str] = "/RIG"

# ---------------------------------------------------------------------------
# NetEx helper
# ---------------------------------------------------------------------------

def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def load_line_mapping(path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(path)

    with GzipFile(path, "rb") as gz:
        for _ev, elem in ET.iterparse(gz, events=("end",)):
            if _local(elem.tag) != "Line":
                continue
            priv = elem.find(".//{*}PrivateCode")
            if priv is None or not (code := priv.text.strip()):
                elem.clear(); continue

            # Choose best public label in order ShortName → PublicCode → Name
            candidates = [
                elem.find(".//{*}ShortName"),
                elem.find(".//{*}PublicCode"),
                elem.find(".//{*}Name"),
            ]
            label = next((c.text.strip() for c in candidates if c is not None and c.text and c.text.strip()), code)
            mapping[code] = label
            mapping[code.lstrip("0")] = label  # strip leading zeros variant
            elem.clear()
    return mapping

# ---------------------------------------------------------------------------
# ZeroMQ
# ---------------------------------------------------------------------------

def create_subscriber(endpoint: str = ZMQ_ENDPOINT, topic: str = ZMQ_TOPIC) -> zmq.Socket:
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    return sock

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


def extract_payload(frames: List[bytes]) -> str:
    raw = b"".join(frames)
    with GzipFile(fileobj=BytesIO(raw)) as gz:
        return gz.read().decode("utf‑8", errors="replace")

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def format_block(data: Dict[str, str]) -> str:
    return (
        f"Vervoerder: {data['tmi8:dataownercode']}\n"
        f"tijd: {data['tijd']}\n"
        f"dag: {data['tmi8:operatingday']}\n"
        f"Lineplanningsnummer: {data['publiek_line']}\n"
        f"voertuignummer: {data['tmi8:vehiclenumber']}\n"
        f"ritnummer: {data['tmi8:journeynumber']}\n"
        f"rd-x: {data['tmi8:rd-x']}\n"
        f"rd-y: {data['tmi8:rd-y']}\n"
    )


def append_to_file(text: str) -> None:
    with OUTPUT_FILE.open("a", encoding="utf-8") as fh:
        fh.write(text + "\n")

# ---------------------------------------------------------------------------
# Printer
# ---------------------------------------------------------------------------

def handle_message(msg_xml: str, mapping: Dict[str, str]) -> None:
    if tag_value(msg_xml, "tmi8:dataownercode") != "RET":
        return  # skip non‑RET

    data = {tag: tag_value(msg_xml, tag) for tag in TAGS}

    # tijd
    tijd_iso = data.get("tmi8:timestamp", "")
    try:
        tijd = dt.datetime.fromisoformat(tijd_iso).time().isoformat(timespec="seconds") if tijd_iso else ""
    except ValueError:
        tijd = tijd_iso
    data["tijd"] = tijd

    # publiek lijnnummer
    internal = data.get("tmi8:lineplanningnumber", "")
    public = mapping.get(internal) or mapping.get(internal.lstrip("0")) or internal
    data["publiek_line"] = public

    block = format_block(data)
    print(block)
    append_to_file(block)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("🔄  Loading NetEx mapping …", end=" ")
    mapping = load_line_mapping(NETEX_FILE)
    print(f"{len(mapping)} entries.")
    print(f"📄  Output wordt gelogd naar {OUTPUT_FILE}\n")

    sub = create_subscriber()
    print("📡  Listening on /RIG (RET only).  Ctrl‑C to stop…")

    tmpdir = Path(tempfile.gettempdir())

    try:
        while True:
            frames = sub.recv_multipart()
            payload = extract_payload(frames[1:])
            try:
                for m in MSG_RE.finditer(payload):
                    handle_message(m.group(0), mapping)
            except Exception as exc:
                ts = int(time.time())
                err_path = tmpdir / f"ndov_err_{ts}.xml.gz"
                with err_path.open("wb") as fh:
                    fh.write(b"".join(frames[1:]))
                print(f"⚠️  parse error {exc} – raw saved to {err_path}")
    except KeyboardInterrupt:
        print("Stopping…")
    finally:
        sub.close(); sub.context.term()


if __name__ == "__main__":
    main()
