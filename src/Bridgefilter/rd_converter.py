"""rd_converter.py – Rijksdriehoek ▸ WGS‑84

Provides :class:`RDConverter` that converts Dutch Rijksdriehoek
coordinates (EPSG:28992) to latitude/longitude in WGS‑84 (EPSG:4326)
using the official RDNAPTRANS™ 2008 polynomial (accuracy < 1 m).
"""

from __future__ import annotations

from typing import Tuple


class RDConverter:
    """Utility class for RD → WGS coordinate conversion."""

    # Polynomial coefficients (arc‑seconds)
    _PHI_COEF = [
        (0, 1, 3235.65389),
        (2, 0, -32.58297),
        (0, 2, -0.2475),
        (2, 1, -0.84978),
        (0, 3, -0.0655),
        (2, 2, -0.01709),
        (1, 0, -0.00738),
        (4, 0, 0.00067),
        (2, 3, -0.00039),
        (4, 1, 0.00033),
    ]

    _LAM_COEF = [
        (1, 0, 5260.52916),
        (1, 1, 105.94684),
        (1, 2, 2.45656),
        (3, 0, -0.81885),
        (1, 3, 0.05594),
        (3, 1, -0.05607),
        (0, 1, 0.01199),
        (3, 2, -0.00256),
        (1, 4, 0.00128),
    ]

    _X0 = 155_000.0
    _Y0 = 463_000.0
    _PHI_0 = 52.15517440  # deg
    _LAM_0 = 5.38720621   # deg

    @classmethod
    def rd_to_wgs(cls, x_rd: float, y_rd: float) -> Tuple[float, float]:
        """Return ``(lat, lon)`` in decimal degrees."""
        dx = (x_rd - cls._X0) / 100_000.0
        dy = (y_rd - cls._Y0) / 100_000.0

        d_phi = sum(c * dx ** p * dy ** q for p, q, c in cls._PHI_COEF)
        d_lam = sum(c * dx ** p * dy ** q for p, q, c in cls._LAM_COEF)

        lat = cls._PHI_0 + d_phi / 3600.0
        lon = cls._LAM_0 + d_lam / 3600.0
        return lat, lon
