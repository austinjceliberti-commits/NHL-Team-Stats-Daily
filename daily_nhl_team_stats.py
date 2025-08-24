# daily_nhl_team_stats.py
# Loads season-to-date team stats and UPSERTs into your nhl_team_stats table.
# Requires: pip install requests psycopg2-binary python-dateutil

import os, sys, time, json, requests
from datetime import datetime, timezone
from dateutil import tz
import psycopg2, psycopg2.extras

DB_URL = os.environ["postgres://tsdbadmin:<TIMESCALE_DB_PASSWORD>@jbzime1eq9.rj87e4urof.tsdb.cloud.timescale.com:34545/tsdb?sslmode=require"]  # e.g. postgres://user:pass@host:port/dbname
SEASON = os.environ.get("NHL_SEASON")  # e.g. 20252026; if None, we'll infer

# --- Helpers ---------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "nhl-stats-loader/1.0 (+automation)"
})

def nhl_get(url, params=None, timeout=20):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def infer_current_season(today=None):
    """Return season as YYYYYYYY string (e.g., '20252026')."""
    tz_et = tz.gettz("America/New_York")
    today = today or datetime.now(tz=tz_et).date()
    year = today.year
    # NHL season rolls around Oct; if before July, current season starts previous calendar year
    start_year = year - 1 if today.month < 7 else year
    return f"{start_year}{start_year+1}"

def fetch_active_teams():
    # New web API: all teams + triCode for mapping, works reliably
    # Example: https://api-web.nhle.com/v1/teams
    data = nhl_get("https://api-web.nhle.com/v1/teams")
    # Normalize to {triCode, fullName}
    teams = []
    for t in data:
        tri = t.get("triCode")
        name = t.get("fullName") or t.get("name") or t.get("teamName")
        if tri and name:
            teams.append({"code": tri.lower(), "name": name})
    return teams

def fetch_team_season_summary(team_code, season):
    """
    Use the new 'club-stats' endpoint (season-to-date summary for a team).
    Example (per community refs): /v1/club-stats/{team}/{season}/{gameType}
      - team: three-letter code (e.g., BOS)
      - season: YYYYYYYY (e.g., 20252026)
      - gameType: 2 = regular season, 3 = playoffs
    We’ll pull gameType=2 for your table.
    """
    url = f"https://api-web.nhle.com/v1/club-stats/{team_code.upper()}/{season}/2"
    try:
        data = nhl_get(url)
    except requests.HTTPError as e:
        # Some teams before the season may 404; return empty baseline
        return None

    # The structure typically includes totals: gp, w, l, ot, gf, ga, ppPct, pkPct, etc.
    # We guard with .get(...) to be resilient.
    out = {
        "gp": data.get("gamesPlayed"),
        "w": data.get("wins"),
        "l": data.get("losses"),
        "ties": None,  # modern NHL has no ties; keep for legacy compatibility
        "ot": data.get("otLosses") or data.get("overtimeLosses"),
        "points": data.get("points"),
        "points_pct": data.get("pointsPct"),
        "rw": data.get("regulationWins"),
        "row": data.get("regulationPlusOtWins") or data.get("row"),
        "so_wins": data.get("shootoutWins"),
        "gf": data.get("goalsFor"),
        "ga": data.get("goalsAgainst"),
        "gf_per_gp": data.get("goalsForPerGame"),
        "ga_per_gp": data.get("goalsAgainstPerGame"),
        "pp_pct": data.get("powerPlayPct"),
        "pk_pct": data.get("penaltyKillPct"),
        "net_pp_pct": data.get("netPowerPlayPct"),
        "net_pk_pct": data.get("netPenaltyKillPct"),
        "shots_per_gp": data.get("shotsForPerGame"),
        "sa_per_gp": data.get("shotsAgainstPerGame"),
        "fow_pct": data.get("faceoffWinPct"),
    }
    return out

def slug_from_name(name):
    import unicodedata, re
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

def team_id_from_db_or_slug(cur, team_name):
    cur.execute("""
        SELECT team_id
        FROM nhl_teams
        WHERE lower(unaccent(team_name)) = lower(unaccent(%s))
        LIMIT 1
    """, (team_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    return slug_from_name(team_name)

# --- Main UPSERT -----------------------------------------------------------

UPSERT_SQL = """
INSERT INTO nhl_team_stats (
  team_id, season, gp, w, l, ties, ot, points, points_pct,
  rw, row, so_wins, gf, ga, gf_per_gp, ga_per_gp,
  pp_pct, pk_pct, net_pp_pct, net_pk_pct, shots_per_gp, sa_per_gp, fow_pct, source
) VALUES (
  %(team_id)s, %(season)s, %(gp)s, %(w)s, %(l)s, %(ties)s, %(ot)s, %(points)s, %(points_pct)s,
  %(rw)s, %(row)s, %(so_wins)s, %(gf)s, %(ga)s, %(gf_per_gp)s, %(ga_per_gp)s,
  %(pp_pct)s, %(pk_pct)s, %(net_pp_pct)s, %(net_pk_pct)s, %(shots_per_gp)s, %(sa_per_gp)s, %(fow_pct)s, %(source)s
)
ON CONFLICT (team_id, season) DO UPDATE SET
  gp = EXCLUDED.gp, w = EXCLUDED.w, l = EXCLUDED.l, ties = EXCLUDED.ties, ot = EXCLUDED.ot,
  points = EXCLUDED.points, points_pct = EXCLUDED.points_pct,
  rw = EXCLUDED.rw, row = EXCLUDED.row, so_wins = EXCLUDED.so_wins,
  gf = EXCLUDED.gf, ga = EXCLUDED.ga, gf_per_gp = EXCLUDED.gf_per_gp, ga_per_gp = EXCLUDED.ga_per_gp,
  pp_pct = EXCLUDED.pp_pct, pk_pct = EXCLUDED.pk_pct,
  net_pp_pct = EXCLUDED.net_pp_pct, net_pk_pct = EXCLUDED.net_pk_pct,
  shots_per_gp = EXCLUDED.shots_per_gp, sa_per_gp = EXCLUDED.sa_per_gp, fow_pct = EXCLUDED.fow_pct,
  source = EXCLUDED.source, ingested_at = now();
"""

def main():
    season = SEASON or infer_current_season()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # Ensure unaccent is available for matching names
    cur.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")

    teams = fetch_active_teams()  # [{code:'bos', name:'Boston Bruins'}, ...]
    upserts = 0
    for t in teams:
        summary = fetch_team_season_summary(t["code"], season)
        if not summary:
            continue
        team_id = team_id_from_db_or_slug(cur, t["name"])
        row = {
            "team_id": team_id,
            "season": season,
            **summary,
            "source": f"api-web.nhle.com/{t['code']}/{season}"
        }
        # psycopg2 named params UPSERT
        cur.execute(UPSERT_SQL, row)
        upserts += 1
        time.sleep(0.15)  # be polite

    conn.commit()
    cur.close(); conn.close()
    print(f"Upserted stats for {upserts} teams for season {season}")

if __name__ == "__main__":
    main()
