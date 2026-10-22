"""netex_mapper.py – Internal ⇢ public line‑code mapping for RET

Parses a gzipped NetEx file and exposes :class:`NetexMapper` for easy
lookup of public line numbers (and transport mode) given KV6
``lineplanningnumber`` values.

Let op: in recente NetEx‑bestanden hebben nachtbussen hetzelfde publieke
nummer als trams (bijv. nachtbus 3 en tram 3). Filter daarom op de
transport mode die :meth:`NetexMapper.lookup` teruggeeft.
"""

from __future__ import annotations

import datetime as dt
import xml.etree.ElementTree as ET
from gzip import GzipFile
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class NetexMapper:
    """Load a NetEx file and map internal → (public line code, transport mode)."""

    def __init__(self, netex_path: Path):
        if not netex_path.exists():
            raise FileNotFoundError(netex_path)
        self.path = netex_path
        self.valid_to: Optional[dt.datetime] = None  # einddatum dienstregeling
        self._map: Dict[str, Tuple[str, str]] = {}
        self._load(netex_path)

    def __len__(self) -> int:
        return len(self._map)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def lookup(self, internal: str) -> Optional[Tuple[str, str]]:
        """Return ``(public_code, transport_mode)`` or ``None`` if unknown."""
        internal = internal.strip()
        return self._map.get(internal) or self._map.get(internal.lstrip("0"))

    def is_expired(self, now: Optional[dt.datetime] = None) -> bool:
        if self.valid_to is None:
            return False
        return (now or dt.datetime.now()) > self.valid_to

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]  # strip namespace

    @staticmethod
    def _child_text(elem: ET.Element, tag: str) -> str:
        node = elem.find(f"{{*}}{tag}")  # alleen directe kinderen van <Line>
        return node.text.strip() if node is not None and node.text else ""

    @staticmethod
    def _private_code(elem: ET.Element) -> str:
        codes = elem.findall(".//{*}PrivateCode")
        for node in codes:  # voorkeur voor het LinePlanningNumber
            if node.get("type") == "LinePlanningNumber" and node.text and node.text.strip():
                return node.text.strip()
        for node in codes:
            if node.text and node.text.strip():
                return node.text.strip()
        return ""

    def _add_line(self, elem: ET.Element) -> None:
        key = self._private_code(elem)
        if not key:
            return
        label = (self._child_text(elem, "PublicCode")
                 or self._child_text(elem, "ShortName")
                 or self._child_text(elem, "Name")
                 or key)
        self._map[key] = (label, self._child_text(elem, "TransportMode"))

    def _set_valid_to(self, text: Optional[str]) -> None:
        # Eerste ToDate/EndDate in het bestand = einde van de dienstregeling
        try:
            self.valid_to = dt.datetime.fromisoformat((text or "").strip()[:19])
        except ValueError:
            pass

    def _load(self, path: Path) -> None:
        # Streaming parse: alles buiten een <Line> wordt direct weggegooid,
        # anders blijft de hele (honderden MB grote) XML in het geheugen.
        stack: List[ET.Element] = []
        in_line = 0
        with GzipFile(path, "rb") as gz:
            for ev, el in ET.iterparse(gz, events=("start", "end")):
                tag = self._local(el.tag)
                if ev == "start":
                    stack.append(el)
                    in_line += tag == "Line"
                    continue
                stack.pop()
                if tag == "Line":
                    in_line -= 1
                    self._add_line(el)
                elif tag in ("ToDate", "EndDate") and self.valid_to is None:
                    self._set_valid_to(el.text)
                if not in_line and stack:
                    stack[-1].remove(el)
