"""netex_updater.py – Houdt de RET NetEx‑dienstregeling automatisch actueel

NDOV Loket publiceert de RET‑dienstregeling als één bestand
``NeTEx_RET_<…>_version<N>_<…>.xml.gz`` in https://data.ndovloket.nl/netex/ret/.
Dit module zoekt daar het nieuwste bestand, downloadt het (alleen als die
versie nog niet lokaal staat) naar een cache‑map en controleert of het een
complete gzip is.

Zonder internet valt het terug op het nieuwste bestand in de cache.
"""

from __future__ import annotations

import gzip
import re
import shutil
import urllib.request
import zlib
from pathlib import Path
from typing import Final, Optional, Tuple
from urllib.parse import urljoin

NETEX_INDEX_URL: Final[str] = "https://data.ndovloket.nl/netex/ret/"
HTTP_TIMEOUT_S: Final[int] = 60
KEEP_FILES: Final[int] = 2  # nieuwste + één vorige versie als reserve

_LINK_RE: Final[re.Pattern[str]] = re.compile(r'href="(NeTEx_RET_[^"/?#]+\.xml\.gz)"')
_PATTERN: Final[str] = "NeTEx_RET_*.xml.gz"


def _sort_key(name: str) -> Tuple[int, ...]:
    """Sorteer op alle getallen in de naam: (datum, datum, versie, …)."""
    return tuple(int(n) for n in re.findall(r"\d+", name))


def latest_remote_name(index_url: str = NETEX_INDEX_URL) -> str:
    """Return de bestandsnaam van de nieuwste NetEx op NDOV Loket."""
    with urllib.request.urlopen(index_url, timeout=HTTP_TIMEOUT_S) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    names = set(_LINK_RE.findall(html))
    if not names:
        raise RuntimeError(f"geen NeTEx_RET-bestand gevonden op {index_url}")
    return max(names, key=_sort_key)


def newest_local(cache_dir: Path) -> Optional[Path]:
    files = sorted(cache_dir.glob(_PATTERN), key=lambda p: _sort_key(p.name))
    return files[-1] if files else None


def _verify_gzip(path: Path) -> None:
    """Lees het hele bestand uit; gooit een fout bij een kapotte download."""
    with gzip.open(path, "rb") as gz:
        while gz.read(1 << 20):
            pass


def _download(name: str, cache_dir: Path, index_url: str) -> Path:
    dest = cache_dir / name
    if dest.exists():
        return dest  # deze versie hebben we al

    tmp = dest.with_name(dest.name + ".part")
    url = urljoin(index_url, name)
    print(f"NetEx downloaden: {url}")
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_S) as resp, tmp.open("wb") as fh:
        shutil.copyfileobj(resp, fh)
        expected = resp.headers.get("Content-Length")
    try:
        if expected is not None and tmp.stat().st_size != int(expected):
            raise OSError(f"download onvolledig: {tmp.stat().st_size} van {expected} bytes")
        _verify_gzip(tmp)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    tmp.replace(dest)
    return dest


def _prune(cache_dir: Path, keep: int = KEEP_FILES) -> None:
    files = sorted(cache_dir.glob(_PATTERN), key=lambda p: _sort_key(p.name))
    for old in files[:-keep]:
        old.unlink(missing_ok=True)


def ensure_latest_netex(cache_dir: Path, index_url: str = NETEX_INDEX_URL) -> Path:
    """Return het pad naar de nieuwste beschikbare NetEx.

    Downloadt een nieuwe versie als die er is. Lukt dat niet, dan wordt het
    nieuwste bestand uit de cache gebruikt; is die leeg, dan volgt de fout.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        path = _download(latest_remote_name(index_url), cache_dir, index_url)
        _prune(cache_dir)
        return path
    except (OSError, EOFError, ValueError, RuntimeError, zlib.error) as exc:
        local = newest_local(cache_dir)
        if local is None:
            raise
        print(f"NetEx-update mislukt ({exc}); gebruik lokaal bestand {local.name}")
        return local
