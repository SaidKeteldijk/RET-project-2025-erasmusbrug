"""netex_mapper.py – Internal ⇢ public line‑code mapping for RET

Parses a gzipped NetEx file and exposes :class:`NetexMapper` for easy
lookup of public line numbers given KV6 ``lineplanningnumber`` values.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from gzip import GzipFile
from pathlib import Path
from typing import Dict


class NetexMapper:
    """Load a NetEx file and map internal → public line codes."""

    def __init__(self, netex_path: Path):
        if not netex_path.exists():
            raise FileNotFoundError(netex_path)
        self._map: Dict[str, str] = {}
        self._load(netex_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def public_code(self, internal: str) -> str:
        """Return public line number; fall back to *internal* if unknown."""
        return self._map.get(internal) or self._map.get(internal.lstrip("0"), internal)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]  # strip namespace

    def _best_label(self, elem: ET.Element) -> str:
        for tag in ("ShortName", "PublicCode", "Name"):
            node = elem.find(f".//{{*}}{tag}")
            if node is not None and node.text and node.text.strip():
                return node.text.strip()
        return ""  # fallback handled by caller

    def _load(self, path: Path) -> None:
        with GzipFile(path, "rb") as gz:
            for _ev, el in ET.iterparse(gz, events=("end",)):
                if self._local(el.tag) != "Line":
                    continue
                priv = el.find(".//{*}PrivateCode")
                if priv is None or not priv.text:
                    el.clear(); continue
                key = priv.text.strip()
                label = self._best_label(el) or key
                self._map[key] = label
                self._map[key.lstrip("0")] = label  # variant zonder leading zeros
                el.clear()
