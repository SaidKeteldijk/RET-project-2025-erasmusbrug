"""Tests voor de Erasmusbrug‑monitor (geen netwerk nodig).

Draaien vanuit de hoofdmap van de repo:  python -m unittest discover -s tests
"""

from __future__ import annotations

import contextlib
import gzip
import io
import math
import sys
import tempfile
import unittest
from pathlib import Path
from typing import List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import erasmusbrug_monitor as mon  # noqa: E402
import netex_updater  # noqa: E402
from netex_mapper import NetexMapper  # noqa: E402

L = mon.BRIDGE_LENGTH_M
CENTRAAL = (92500, 436800)          # ver van de brug
WILHELMINAPLEIN = (93330, 435800)   # tramhalte net voorbij de zuidkant


def at(chainage: float, offset: float = 0.0) -> Tuple[float, float]:
    """RD‑punt op *chainage* meter langs het spoor (doorgetrokken voorbij de einden),
    *offset* meter er haaks naast."""
    track = mon.BRIDGE_TRACK_RD
    seg_start = 0.0
    for i, ((ax, ay), (bx, by)) in enumerate(zip(track, track[1:])):
        seg_len = math.hypot(bx - ax, by - ay)
        if chainage <= seg_start + seg_len or i == len(track) - 2:
            t = (chainage - seg_start) / seg_len if chainage >= 0 else chainage / seg_len
            ux, uy = (bx - ax) / seg_len, (by - ay) / seg_len
            return ax + t * (bx - ax) - uy * offset, ay + t * (by - ay) + ux * offset
        seg_start += seg_len
    raise AssertionError("unreachable")


class FakeMapper:
    path = Path("fake.xml.gz")
    _lines = {"1682": ("3", "tram"), "1683": ("5", "tram"), "64": ("12", "tram"),
              "65": ("3", "bus"), "M010": ("E", "metro")}

    def lookup(self, code: str) -> Optional[Tuple[str, str]]:
        return self._lines.get(code.strip())


def kv6(kind: str, veh: str, rd=None, sec: int = 0, line: str = "1682",
        prefix: str = "tmi8:", owner: str = "RET") -> str:
    p = prefix
    x, y = rd if rd is not None else at(300)  # zuidelijke helft van de brug
    ts = f"2026-10-22T10:{sec // 60:02d}:{sec % 60:02d}+02:00"
    return (f"<{p}{kind}><{p}dataownercode>{owner}</{p}dataownercode>"
            f"<{p}lineplanningnumber>{line}</{p}lineplanningnumber>"
            f"<{p}timestamp>{ts}</{p}timestamp><{p}vehiclenumber>{veh}</{p}vehiclenumber>"
            f"<{p}rd-x>{x:.0f}</{p}rd-x><{p}rd-y>{y:.0f}</{p}rd-y></{p}{kind}>")


def frames(*messages: str, prefixed: bool = True) -> List[bytes]:
    if prefixed:
        body = ('<tmi8:VV_TM_PUSH xmlns:tmi8="http://bison.connekt.nl/tmi8/kv6/msg">'
                f"<tmi8:KV6posinfo>{''.join(messages)}</tmi8:KV6posinfo></tmi8:VV_TM_PUSH>")
    else:
        body = ('<VV_TM_PUSH xmlns="http://bison.connekt.nl/tmi8/kv6/msg">'
                f"<KV6posinfo>{''.join(messages)}</KV6posinfo></VV_TM_PUSH>")
    return [b"/RIG/KV6posinfo", gzip.compress(body.encode())]


class GeometryTest(unittest.TestCase):
    def test_track_position(self):
        chainage, dist = mon.track_position(*at(300, offset=5))
        self.assertAlmostEqual(chainage, 300, delta=1)
        self.assertAlmostEqual(dist, 5, delta=1)
        self.assertAlmostEqual(mon.track_position(*at(-40))[0], -40, delta=1)
        self.assertAlmostEqual(mon.track_position(*at(L + 40))[0], L + 40, delta=1)

    def test_bridge_corridor(self):
        def on(point):
            return mon.is_on_bridge(*mon.track_position(*point))
        self.assertAlmostEqual(L, 726, delta=1)
        self.assertTrue(on(at(5)))
        self.assertTrue(on(at(L - 5)))
        self.assertTrue(on(at(L / 2, offset=15)))
        self.assertFalse(on(at(L / 2, offset=30)))
        self.assertFalse(on(at(-10)))           # net voor het zuideinde
        self.assertFalse(on(at(L + 10)))        # net na het noordeinde
        self.assertFalse(on(CENTRAAL))
        self.assertFalse(on(WILHELMINAPLEIN))

    def test_parse_rd_rejects_unknown_positions(self):
        self.assertIsNone(mon.parse_rd("-1", "-1"))
        self.assertIsNone(mon.parse_rd("", ""))
        self.assertIsNone(mon.parse_rd("nan", "nan"))
        self.assertEqual(mon.parse_rd("93000", "436030"), (93000.0, 436030.0))


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.log = Path(self.tmp.name) / "metingen.csv"
        self.bm = mon.BridgeMonitor(FakeMapper(), event_log=self.log)
        self.bm.dump_dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def feed(self, *frame_list: List[bytes]) -> List[str]:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            for fr in frame_list:
                self.bm._process(fr)
        return buf.getvalue().splitlines()

    def assert_events(self, out: List[str], *expected: str):
        self.assertEqual([line.split(" – ")[0].strip() for line in out if " – " in line],
                         list(expected))

    def test_crossing_northbound_with_interpolated_times(self):
        out = self.feed(frames(
            kv6("ONROUTE", "2001", at(-60), sec=0),        # voor de brug
            kv6("ONROUTE", "2001", at(30), sec=10),        # op de brug
            kv6("ONROUTE", "2001", at(L - 30), sec=69),
            kv6("ONROUTE", "2001", at(L + 60), sec=79)))   # voorbij de brug
        self.assert_events(out, "Begin met meten", "Stop met meten")
        self.assertIn("tram 2001 (lijn 3, richting noord)", out[0])
        self.assertIn("tijd: 10:00:06", out[0])   # zuideinde gepasseerd na 60/90 van 10 s
        self.assertIn("tijd: 10:01:12", out[1])   # noordeinde gepasseerd na 30/90 van 10 s

    def test_crossing_southbound(self):
        out = self.feed(frames(kv6("ONROUTE", "2001", at(L + 50), sec=0),
                               kv6("ONROUTE", "2001", at(L - 50), sec=9)))
        self.assertIn("richting zuid", out[0])
        self.assertIn("tijd: 10:00:04", out[0])   # halverwege 9 s

    def test_two_trams_one_measurement(self):
        out = self.feed(frames(kv6("ONROUTE", "2001", sec=0),
                               kv6("ONROUTE", "2002", line="1683", sec=5),
                               kv6("ONROUTE", "2001", CENTRAAL, sec=70),
                               kv6("ONROUTE", "2002", CENTRAAL, sec=80)))
        self.assert_events(out, "Begin met meten", "+ tram 2002 (lijn 5, richting noord) op brug",
                           "- tram 2001 van brug", "Stop met meten")
        self.assertIn("(2 trams)", out[1])
        self.assertIn("(1 tram)", out[2])

    def test_every_tram_line_counts(self):
        out = self.feed(frames(kv6("ONROUTE", "2050", line="64")))
        self.assertIn("lijn 12", out[0])

    def test_buses_metro_and_other_operators_ignored(self):
        out = self.feed(frames(kv6("ONROUTE", "900", line="65"),          # nachtbus 3
                               kv6("ONROUTE", "5501", line="M010"),       # metro
                               kv6("ONROUTE", "3001", owner="HTM")))
        self.assertEqual(out, [])

    def test_unknown_position_keeps_state(self):
        out = self.feed(frames(kv6("ONROUTE", "2001"), kv6("ONROUTE", "2001", (-1, -1), sec=8)))
        self.assert_events(out, "Begin met meten")
        self.assertIn("2001", self.bm.on_bridge)

    def test_arrival_and_departure_positions_used(self):
        out = self.feed(frames(kv6("DEPARTURE", "2001"), kv6("ARRIVAL", "2001", CENTRAAL, sec=70)))
        self.assert_events(out, "Begin met meten", "Stop met meten")

    def test_end_of_journey_on_bridge_stops(self):
        out = self.feed(frames(kv6("ONROUTE", "2001"), kv6("END", "2001", (-1, -1), sec=30)))
        self.assert_events(out, "Begin met meten", "Stop met meten")
        self.assertIn("rit beëindigd", out[1])

    def test_vehicle_switching_to_bus_line_stops(self):
        out = self.feed(frames(kv6("ONROUTE", "2001"), kv6("INIT", "2001", line="65", sec=30)))
        self.assert_events(out, "Begin met meten", "Stop met meten")

    def test_out_of_order_message_ignored(self):
        out = self.feed(frames(kv6("ONROUTE", "2001", sec=20), kv6("ONROUTE", "2001", CENTRAAL, sec=10)))
        self.assert_events(out, "Begin met meten")

    def test_stale_tram_expires(self):
        self.feed(frames(kv6("ONROUTE", "2001")))
        self.bm.on_bridge["2001"].seen -= mon.STALE_AFTER_S + 1
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.bm._expire_stale()
        self.assertIn("geen signaal meer", buf.getvalue())
        self.assertEqual(self.bm.on_bridge, {})

    def test_messages_without_namespace_prefix(self):
        out = self.feed(frames(kv6("ONROUTE", "2001", prefix=""),
                               kv6("ONROUTE", "2001", CENTRAAL, sec=70, prefix=""), prefixed=False))
        self.assert_events(out, "Begin met meten", "Stop met meten")

    def test_corrupt_payload_does_not_stop_monitor(self):
        out = self.feed([b"/RIG/KV6posinfo", b"geen gzip"], frames(kv6("ONROUTE", "2001")))
        self.assertTrue(out[0].startswith("parse error"))
        self.assert_events(out, "Begin met meten")
        self.assertEqual(len(list(Path(self.tmp.name).glob("kv6_err_*.xml.gz"))), 1)

    def test_non_kv6_topics_skipped(self):
        self.assertEqual(self.feed([b"/RIG/KV17cvlinfo", b"..."]), [])

    def test_event_log(self):
        self.feed(frames(kv6("ONROUTE", "2001", at(-60), sec=0),
                         kv6("ONROUTE", "2001", at(30), sec=10),
                         kv6("ONROUTE", "2002", line="1683", sec=20),
                         kv6("ONROUTE", "2001", at(L + 60), sec=78),
                         kv6("ONROUTE", "2002", CENTRAAL, sec=90)))
        self.assertEqual(self.log.read_text(encoding="utf-8").splitlines(), [
            "tijd,voertuig,lijn,gebeurtenis,richting,trams_op_brug,meting,opmerking",
            "2026-10-22T10:00:06+02:00,2001,3,op,noord,1,begin,",
            "2026-10-22T10:00:20+02:00,2002,5,op,noord,2,,",
            "2026-10-22T10:01:18+02:00,2001,3,af,noord,1,,",
            "2026-10-22T10:01:30+02:00,2002,5,af,noord,0,stop,",
        ])


NETEX_NEW = """<?xml version="1.0" encoding="UTF-8"?>
<PublicationDelivery xmlns="http://www.netex.org.uk/netex"><dataObjects>
<CompositeFrame><ValidBetween><FromDate>2026-10-20T00:00:00</FromDate>
<ToDate>2027-01-03T00:00:00</ToDate></ValidBetween><frames><ServiceFrame><lines>
<Line id="NL:RET:Line:1682"><privateCodes><PrivateCode type="LinePlanningNumber">1682</PrivateCode>
</privateCodes><Name>Beverwaard - Rotterdam Centraal</Name><TransportMode>tram</TransportMode>
<PublicCode>3</PublicCode></Line>
<Line id="NL:RET:Line:65"><privateCodes><PrivateCode type="LinePlanningNumber">65</PrivateCode>
</privateCodes><Name>Rotterdam Centraal - Schiedam Woudhoek</Name><TransportMode>bus</TransportMode>
<PublicCode>3</PublicCode></Line>
</lines></ServiceFrame></frames></CompositeFrame></dataObjects></PublicationDelivery>
"""

NETEX_OLD = """<?xml version="1.0" encoding="UTF-8"?>
<PublicationDelivery xmlns="http://www.netex.org.uk/netex"><dataObjects>
<CompositeFrame><versions><Version><StartDate>2025-09-09T00:00:00.000+02:00</StartDate>
<EndDate>2025-12-17T00:00:00.000+01:00</EndDate></Version></versions><frames><ServiceFrame><lines>
<Line id="RET:Line:1683"><Name>Carnisselande - Rotterdam Centraal</Name><TransportMode>tram</TransportMode>
<PublicCode>5</PublicCode><PrivateCode type="LinePlanningNumber">1683</PrivateCode></Line>
</lines></ServiceFrame></frames></CompositeFrame></dataObjects></PublicationDelivery>
"""


class NetexMapperTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def mapper(self, xml: str) -> NetexMapper:
        path = Path(self.tmp.name) / "netex.xml.gz"
        path.write_bytes(gzip.compress(xml.encode()))
        return NetexMapper(path)

    def test_current_format_separates_tram_and_night_bus(self):
        m = self.mapper(NETEX_NEW)
        self.assertEqual(m.lookup("1682"), ("3", "tram"))
        self.assertEqual(m.lookup("65"), ("3", "bus"))
        self.assertIsNone(m.lookup("9999"))
        self.assertEqual(str(m.valid_to), "2027-01-03 00:00:00")

    def test_old_format_and_expiry(self):
        m = self.mapper(NETEX_OLD)
        self.assertEqual(m.lookup("1683"), ("5", "tram"))
        self.assertTrue(m.is_expired())


class NetexUpdaterTest(unittest.TestCase):
    OLD = "NeTEx_RET_ALL_20260801_20260801_version400_v930.xml.gz"
    NEW = "NeTEx_RET_ALL_20261020_20261020_version419_v930.xml.gz"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.site = Path(self.tmp.name) / "site"
        self.site.mkdir()
        for name in (self.OLD, self.NEW):
            (self.site / name).write_bytes(gzip.compress(NETEX_NEW.encode()))
        (self.site / "index.html").write_text(
            "".join(f'<a class="link" href="{n}">{n}</a>' for n in (self.OLD, self.NEW)))
        self.index_url = self.site.as_uri() + "/index.html"
        self.cache = Path(self.tmp.name) / "cache"

    def tearDown(self):
        self.tmp.cleanup()

    def ensure(self, index_url: Optional[str] = None) -> Path:
        with contextlib.redirect_stdout(io.StringIO()):
            return netex_updater.ensure_latest_netex(self.cache, index_url or self.index_url)

    def test_downloads_newest_once(self):
        self.assertEqual(self.ensure().name, self.NEW)
        (self.site / self.NEW).unlink()          # tweede keer: uit de cache
        self.assertEqual(self.ensure().name, self.NEW)

    def test_offline_uses_cache_or_raises(self):
        offline = (Path(self.tmp.name) / "bestaat-niet").as_uri() + "/"
        with self.assertRaises(OSError):
            self.ensure(offline)                 # lege cache en geen verbinding
        self.ensure()
        self.assertEqual(self.ensure(offline).name, self.NEW)

    def test_keeps_two_newest_versions(self):
        self.cache.mkdir()
        for v in (100, 200, 300):
            (self.cache / f"NeTEx_RET_ALL_20250101_20250101_version{v}_v1.xml.gz").write_bytes(b"x")
        self.ensure()
        self.assertEqual(sorted(p.name for p in self.cache.iterdir()),
                         ["NeTEx_RET_ALL_20250101_20250101_version300_v1.xml.gz", self.NEW])


if __name__ == "__main__":
    unittest.main()
