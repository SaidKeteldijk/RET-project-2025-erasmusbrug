# RET-project-2025-erasmusbrug

Detecteert in realtime wanneer er een tram over de **Erasmusbrug** in Rotterdam
rijdt, zodat een meting precies tijdens een tramoversteek kan starten en stoppen.

De posities van de trams komen uit de open KV6-feed van NDOV Loket
(`tcp://pubsub.besteffort.ndovloket.nl:7658`, topic `/RIG`). Welke voertuigen
trams zijn, volgt uit de NetEx-dienstregeling van RET.

## Starten

```bash
pip install -r requirements.txt
python src/erasmusbrug_monitor.py
```

Stoppen met Ctrl-C. Voorbeeld van de uitvoer:

```
Begin met meten – tram 2148 (lijn 3, richting noord) op brug – tijd: 11:21:04
  + tram 2037 (lijn 5, richting noord) op brug – 11:21:31 (2 trams)
  + tram 2004 (lijn 5, richting zuid) op brug – 11:21:37 (3 trams)
  - tram 2148 van brug – 11:22:23 (2 trams)
  - tram 2037 van brug – 11:22:36 (1 tram)
Stop met meten – 0 trams op brug – tijd: 11:22:44
```

- **Begin met meten**: de eerste tram rijdt de lege brug op.
- **Stop met meten**: de laatste tram heeft de brug verlaten.
- `richting noord` = naar Leuvehaven/centrum, `richting zuid` = naar Wilhelminaplein.

Elke tram die de brug op- of afrijdt, komt ook als regel in `src/metingen.csv`:

| tijd | voertuig | lijn | gebeurtenis | richting | trams_op_brug | meting | opmerking |
|---|---|---|---|---|---|---|---|
| 2026-10-22T11:21:04+02:00 | 2148 | 3 | op | noord | 1 | begin | |
| 2026-10-22T11:22:44+02:00 | 2004 | 5 | af | zuid | 0 | stop | |

## Hoe het werkt

- **Brug**: een tram staat op de brug als zijn positie tussen de twee
  brugeinden ligt en binnen 20 m van het tramspoor. Het spoor (726 m) is de
  middenlijn van de OpenStreetMap-tramsporen over de brug. Die staat als
  `BRIDGE_TRACK_RD` in RD-coördinaten in `src/erasmusbrug_monitor.py`.
- **Welke voertuigen**: alle RET-trams (alle lijnen). Bussen en metro tellen
  niet mee; de metro rijdt onder de rivier door.
- **Tijden**: trams sturen ongeveer elke 8 seconden hun positie. Het moment
  waarop een tram een brugeinde passeert, wordt geïnterpoleerd tussen de
  positie ervoor en erna. Alle tijden komen uit het KV6-bericht en niet van de
  klok van de pc. Een regel verschijnt pas zodra het volgende meetpunt binnen
  is, dus enkele seconden na de tijd die erin staat.
- **Storingen**: een onbekende positie (`rd-x = -1`) verandert niets. Een tram
  die 90 s niets meer stuurt, telt als vertrokken. Als de feed 60 s stil is,
  verbindt de monitor opnieuw.
- **Slaapstand**: in slaapstand of moderne stand-by ontvangt de pc niets en
  worden trams gemist. Zet slaapstand uit tijdens het meten. Na het ontwaken
  verbindt de monitor vanzelf opnieuw.
- **Dienstregeling**: bij het starten en daarna elke 6 uur wordt de nieuwste
  NetEx van <https://data.ndovloket.nl/netex/ret/> opgehaald naar
  `src/netex_cache/`. Een versie die er al staat, wordt niet opnieuw
  gedownload. Zonder internet gebruikt de monitor het laatst gedownloade
  bestand.

## Bestanden

| Bestand | Wat |
|---|---|
| `src/erasmusbrug_monitor.py` | De monitor (start hier) |
| `src/netex_mapper.py` | Leest de NetEx: interne lijncode → publiek lijnnummer + tram/bus/metro |
| `src/netex_updater.py` | Downloadt de nieuwste NetEx van NDOV Loket |
| `tests/` | Tests, zonder netwerk: `python -m unittest discover -s tests` |
