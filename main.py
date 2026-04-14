import copy
import datetime as dt
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
import io
import zipfile


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "fantasy_data.json")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8000"))
API_BASE = "https://api.cricapi.com/v1"
CRICBUZZ_LIVE_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
CRICBUZZ_RECENT_URL = "https://www.cricbuzz.com/cricket-match/live-scores/recent-matches"
CRICSHEET_IPL_JSON_ZIP = "https://cricsheet.org/downloads/ipl_json.zip"

DEFAULT_SETTINGS = {
    "cricketdata_api_key": os.environ.get("CRICKETDATA_API_KEY", "").strip(),
    "series_filter": "Indian Premier League",
    "selected_match_ids": [],
}

DEFAULT_OWNER_FLAGS = {
    "captain_change_used": False,
    "captain_change_from_match_id": "",
    "changed_captain": "",
    "changed_vice_captain": "",
}

WICKETKEEPERS = {
    "A Porel",
    "Anuj Rawat",
    "D C Jurel",
    "F H Allen",
    "H Klaasen",
    "Ishan Kishan",
    "J C Buttler",
    "J M Sharma",
    "K L Rahul",
    "M S Dhoni",
    "N Pooran",
    "P D Salt",
    "Prabhsimran Singh",
    "Q de Kock",
    "R D Rickelton",
    "R R Pant",
    "S V Samson",
    "T L Seifert",
}

ALIAS_SIGNATURES = {
    "C V Varun": ["varunchakravarthy", "chakravarthy", "chakravarty", "varun"],
    "S A Yadav": ["suryakumaryadav", "suryakumar", "sky", "skyyadav"],
    "R G Sharma": ["rohitsharma", "rohit"],
    "A K Markram": ["aidenmarkram", "markram"],
    "B Kumar": ["bhuvneshwarkumar", "bhuvneshwar"],
    "K R Sen": ["kuldeepsen", "sen"],
    "C Bosch": ["corbinbosch", "bosch"],
    "R D Rickelton": ["ryanrickelton", "rickelton"],
    "Q de Kock": ["quintondekock", "dekock", "dekock"],
    "L S Livingstone": ["liamlivingstone", "livingstone"],
    "A F Milne": ["adammilne", "milne"],
    "A J Hosein": ["akealhosein", "hosein"],
    "J C Buttler": ["josbuttler", "buttler"],
    "J J Bumrah": ["jaspritbumrah", "bumrah"],
    "M R Marsh": ["mitchellmarsh", "marsh"],
    "J O Holder": ["jasonholder", "holder"],
    "V R Iyer": ["venkateshiyer", "venkatesh"],
    "V G Arora": ["vaibhavarora"],
    "P P Shaw": ["prithvishaw"],
    "T M Head": ["travishead"],
    "V Kohli": ["viratkohli", "virat"],
    "J Overton": ["jamieoverton"],
    "N Wadhera": ["nehalwadhera"],
    "R D Gaikwad": ["ruturajgaikwad", "ruturaj"],
    "S S Iyer": ["shreyasiyer", "shreyas"],
    "P J Cummins": ["patcummins", "pat"],
    "N Burger": ["nandreburger"],
    "J D Unadkat": ["jaydevunadkat"],
    "R A Jadeja": ["ravindrajadeja"],
    "K H Pandya": ["krunalpandya", "krunal"],
    "H H Pandya": ["hardikpandya", "hardik"],
    "M J Henry": ["matthenry", "matthewhenry"],
}

HOWSTAT_POINT_RULES = {
    "batting": "1 run = 1 point, plus +25 at 50, +35 at 75, +50 at 100",
    "bowling": "1 wicket = 25 points, plus +25 at 3 wickets, +35 at 4 wickets, +50 at 5 wickets",
    "fielding": "Only wicketkeepers score fielding points: catch = 3, stumping = 5",
    "captain": "Captain gets 1.5x points",
    "vice_captain": "Vice-captain gets 1.25x points",
}

DEFAULT_OWNERS = [
    {
        "owner_name": "Tejas",
        "captain": "A K Markram",
        "vice_captain": "A R Patel",
        "players": [
            "S P Narine",
            "Ravi Bishnoi",
            "A R Patel",
            "Rashid Khan",
            "N Pooran",
            "A K Markram",
            "N K Reddy",
            "Washington Sundar",
            "D A Miller",
            "N Burger",
            "R D Chahar",
        ],
    },
    {
        "owner_name": "Vivek",
        "captain": "F H Allen",
        "vice_captain": "S A Yadav",
        "players": [
            "F H Allen",
            "S A Yadav",
            "C Green",
            "J M Sharma",
            "Kartik Sharma",
            "K Rabada",
            "M A Starc",
            "Arshdeep Singh",
            "T Natarajan",
            "Mayank Yadav",
            "Ashok Sharma",
        ],
    },
    {
        "owner_name": "Nikku",
        "captain": "Abhishek Sharma",
        "vice_captain": "C V Varun",
        "players": [
            "Abhishek Sharma",
            "C V Varun",
            "P D Salt",
            "T L Seifert",
            "P J Cummins",
            "Prabhsimran Singh",
            "R D Rickelton",
            "Shashank Singh",
            "Vijaykumar Vyshak",
            "Anuj Rawat",
            "S N Thakur",
            "Prashant Veer",
        ],
    },
    {
        "owner_name": "Mihir",
        "captain": "S V Samson",
        "vice_captain": "Kuldeep Yadav",
        "players": [
            "S V Samson",
            "Mohammed Shami",
            "Kuldeep Yadav",
            "B Kumar",
            "L Ngidi",
            "Rinku Singh",
            "Avesh Khan",
            "R A Jadeja",
            "Sandeep Sharma",
            "R Parag",
            "Q de Kock",
            "K R Sen",
        ],
    },
    {
        "owner_name": "Jogani",
        "captain": "Sai Sudharsan",
        "vice_captain": "Tilak Varma",
        "players": [
            "Sai Sudharsan",
            "P Nissanka",
            "L S Livingstone",
            "H V Patel",
            "A F Milne",
            "A Porel",
            "S N Khan",
            "Naman Dhir",
            "Tilak Varma",
            "K H Pandya",
            "M P Stoinis",
            "Priyansh Arya",
        ],
    },
    {
        "owner_name": "Suken",
        "captain": "K L Rahul",
        "vice_captain": "Ishan Kishan",
        "players": [
            "K L Rahul",
            "Ishan Kishan",
            "V Suryavanshi",
            "D C Jurel",
            "R M Patidar",
            "J G Bethell",
            "J D Unadkat",
            "Azmatullah Omarzai",
            "A Badoni",
            "Ramandeep Singh",
            "R S Kishore",
            "M S Dhoni",
        ],
    },
    {
        "owner_name": "Vinit",
        "captain": "Y B K Jaiswal",
        "vice_captain": "R R Pant",
        "players": [
            "T A Boult",
            "D L Chahar",
            "J R Hazlewood",
            "Y S Chahal",
            "Y B K Jaiswal",
            "Shahbaz Ahamad",
            "Abdul Samad",
            "M Prasidh Krishna",
            "M J Henry",
            "R R Pant",
            "V G Arora",
            "M Pathirana",
        ],
    },
    {
        "owner_name": "Biggie",
        "captain": "H H Pandya",
        "vice_captain": "A M Rahane",
        "players": [
            "S Dube",
            "T H David",
            "A J Hosein",
            "H H Pandya",
            "R Shepherd",
            "M J Owen",
            "Mohammed Siraj",
            "M J Santner",
            "S O Hetmeyer",
            "A M Rahane",
            "Harpreet Brar",
            "M Markande",
        ],
    },
    {
        "owner_name": "Mitul",
        "captain": "R G Sharma",
        "vice_captain": "M Jansen",
        "players": [
            "Z Ansari",
            "Ayush Mhatre",
            "J C Archer",
            "A Kumar",
            "V Nigam",
            "W G Jacks",
            "C Bosch",
            "M Jansen",
            "H Klaasen",
            "R G Sharma",
            "K K Nair",
            "Shahrukh Khan",
        ],
    },
    {
        "owner_name": "Kacchu",
        "captain": "R D Gaikwad",
        "vice_captain": "S S Iyer",
        "players": [
            "R D Gaikwad",
            "S S Iyer",
            "N Rana",
            "D Padikkal",
            "G D Phillips",
            "T Stubbs",
            "S E Rutherford",
            "Suyash Sharma",
            "R Powell",
            "B Muzarabani",
            "B A Carse",
            "A Verma",
        ],
    },
    {
        "owner_name": "VishRu",
        "captain": "J J Bumrah",
        "vice_captain": "M R Marsh",
        "players": [
            "D Brevis",
            "J C Buttler",
            "M R Marsh",
            "Auqib Nabi",
            "J J Bumrah",
            "Angkrish Raghuvanshi",
            "R Tewatia",
            "T U Deshpande",
            "Mangesh Yadav",
            "K K Ahmed",
            "J O Holder",
            "V R Iyer",
        ],
    },
    {
        "owner_name": "JinGu",
        "captain": "V Kohli",
        "vice_captain": "Shubman Gill",
        "players": [
            "Noor Ahmad",
            "T M Head",
            "Shivang Kumar",
            "V Kohli",
            "L H Ferguson",
            "Ashutosh Sharma",
            "Shubman Gill",
            "J Overton",
            "P P Shaw",
            "Prince Yadav",
            "Digvesh Rathi",
            "N Wadhera",
        ],
    },
]


def normalize_text(value):
    return re.sub(r"[^a-z0-9]", "", value.lower())


def tokenize_name(value):
    cleaned = re.sub(r"[^A-Za-z ]", " ", value)
    tokens = [token.lower() for token in cleaned.split() if token]
    return tokens


def signatures_for_name(name):
    tokens = tokenize_name(name)
    signatures = set()
    if not tokens:
        return signatures
    signatures.add(normalize_text(name))
    last = tokens[-1]
    initials = "".join(token[0] for token in tokens[:-1])
    signatures.add(last)
    signatures.add(tokens[0] + last)
    signatures.add(tokens[0][0] + last)
    if initials:
        signatures.add(initials + last)
        signatures.add(initials)
    if len(tokens) >= 2:
        signatures.add(tokens[0] + tokens[-1])
        signatures.add(tokens[0])
    signatures.update(ALIAS_SIGNATURES.get(name, []))
    return {signature for signature in signatures if signature}


def calculate_batting_points(runs):
    points = runs
    if runs >= 100:
        points += 50
    elif runs >= 75:
        points += 35
    elif runs >= 50:
        points += 25
    return points


def calculate_bowling_points(wickets):
    points = wickets * 25
    if wickets >= 5:
        points += 50
    elif wickets == 4:
        points += 35
    elif wickets == 3:
        points += 25
    return points


def calculate_fielding_points(catches, stumpings, is_wicketkeeper):
    points = 0
    if is_wicketkeeper:
        points += catches * 3
        points += stumpings * 5
    return points


def calculate_player_points(runs, wickets, catches, stumpings, is_wicketkeeper, is_captain, is_vice_captain):
    points = 0
    points += calculate_batting_points(runs)
    points += calculate_bowling_points(wickets)
    points += calculate_fielding_points(catches, stumpings, is_wicketkeeper)
    if is_captain:
        points = points * 1.5
    elif is_vice_captain:
        points = points * 1.25
    return round(points, 2)


def default_player(player_name, captain, vice_captain):
    return {
        "player_name": player_name,
        "is_wicketkeeper": player_name in WICKETKEEPERS,
        "is_captain": player_name == captain,
        "is_vice_captain": player_name == vice_captain,
        "matches": {},
    }


def build_default_state():
    owners = []
    for owner in DEFAULT_OWNERS:
        owners.append(
            {
                "owner_name": owner["owner_name"],
                "captain": owner["captain"],
                "vice_captain": owner["vice_captain"],
                "original_captain": owner["captain"],
                "original_vice_captain": owner["vice_captain"],
                "players": [
                    default_player(player_name, owner["captain"], owner["vice_captain"])
                    for player_name in owner["players"]
                ],
                **copy.deepcopy(DEFAULT_OWNER_FLAGS),
            }
        )
    return {
        "settings": copy.deepcopy(DEFAULT_SETTINGS),
        "owners": owners,
        "live_matches": [],
        "match_catalog": {},
        "last_sync_at": None,
        "last_sync_message": "No live sync has been run yet.",
    }


def save_state(state):
    with open(DATA_FILE, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2)


def load_state():
    if not os.path.exists(DATA_FILE):
        state = build_default_state()
        save_state(state)
        return state
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        state = json.load(file)
    return migrate_state(state)


def migrate_state(state):
    default_state = build_default_state()
    state.setdefault("settings", {})
    for key, value in DEFAULT_SETTINGS.items():
        if not state["settings"].get(key):
            state["settings"][key] = value
    state.setdefault("live_matches", [])
    state.setdefault("match_catalog", {})
    state.setdefault("last_sync_at", None)
    state.setdefault("last_sync_message", "No live sync has been run yet.")

    existing_owners = {owner["owner_name"]: owner for owner in state.get("owners", [])}
    merged_owners = []
    for default_owner in default_state["owners"]:
        existing_owner = existing_owners.get(default_owner["owner_name"], {})
        existing_players = {
            player["player_name"]: player for player in existing_owner.get("players", [])
        }
        players = []
        for default_player_state in default_owner["players"]:
            current = existing_players.get(default_player_state["player_name"], {})
            merged = copy.deepcopy(default_player_state)
            merged["matches"] = current.get("matches", {})
            players.append(merged)
        merged_owners.append(
            {
                "owner_name": default_owner["owner_name"],
                "captain": existing_owner.get("captain", default_owner["captain"]),
                "vice_captain": existing_owner.get("vice_captain", default_owner["vice_captain"]),
                "original_captain": existing_owner.get("original_captain", default_owner["captain"]),
                "original_vice_captain": existing_owner.get("original_vice_captain", default_owner["vice_captain"]),
                "players": players,
                "captain_change_used": bool(existing_owner.get("captain_change_used", False)),
                "captain_change_from_match_id": str(existing_owner.get("captain_change_from_match_id", "")),
                "changed_captain": str(existing_owner.get("changed_captain", "")),
                "changed_vice_captain": str(existing_owner.get("changed_vice_captain", "")),
            }
        )
    state["owners"] = merged_owners
    save_state(state)
    return state


def get_signature_lookup(state):
    lookup = {}
    for owner in state["owners"]:
        for player in owner["players"]:
            for signature in signatures_for_name(player["player_name"]):
                lookup.setdefault(signature, set()).add(player["player_name"])
    return lookup


def resolve_player_name(external_name, state):
    lookup = get_signature_lookup(state)
    candidates = set()
    for signature in signatures_for_name(external_name):
        candidates.update(lookup.get(signature, set()))
    if len(candidates) == 1:
        return next(iter(candidates))

    external_tokens = tokenize_name(external_name)
    if not external_tokens:
        return None
    external_last = external_tokens[-1]
    external_first_initial = external_tokens[0][0]

    for owner in state["owners"]:
        for player in owner["players"]:
            player_tokens = tokenize_name(player["player_name"])
            if not player_tokens:
                continue
            if player_tokens[-1] == external_last and player_tokens[0][0] == external_first_initial:
                return player["player_name"]
    return None


def owner_assignment_for_match(owner, match_id, state):
    current_captain = owner.get("original_captain") or owner.get("captain")
    current_vice = owner.get("original_vice_captain") or owner.get("vice_captain")
    if owner.get("captain_change_used") and owner.get("captain_change_from_match_id"):
        match_catalog = state.get("match_catalog", {})
        current_info = match_catalog.get(str(match_id), {})
        change_info = match_catalog.get(str(owner.get("captain_change_from_match_id")), {})
        current_key = match_sort_key(
            {
                "id": match_id,
                "dateTimeGMT": current_info.get("date_time_gmt", ""),
            }
        )
        change_key = match_sort_key(
            {
                "id": owner.get("captain_change_from_match_id"),
                "dateTimeGMT": change_info.get("date_time_gmt", ""),
            }
        )
        if current_key >= change_key:
            current_captain = owner.get("changed_captain") or current_captain
            current_vice = owner.get("changed_vice_captain") or current_vice
    return current_captain, current_vice


def aggregate_player_totals(player, owner, state):
    totals = {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0}
    total_points = 0
    for match_id, stats in player.get("matches", {}).items():
        totals["runs"] += int(stats.get("runs", 0))
        totals["wickets"] += int(stats.get("wickets", 0))
        totals["catches"] += int(stats.get("catches", 0))
        totals["stumpings"] += int(stats.get("stumpings", 0))
        match_captain, match_vice = owner_assignment_for_match(owner, match_id, state)
        total_points += calculate_player_points(
            runs=int(stats.get("runs", 0)),
            wickets=int(stats.get("wickets", 0)),
            catches=int(stats.get("catches", 0)),
            stumpings=int(stats.get("stumpings", 0)),
            is_wicketkeeper=player.get("is_wicketkeeper", False),
            is_captain=player.get("player_name") == match_captain,
            is_vice_captain=player.get("player_name") == match_vice,
        )
    totals["points"] = round(total_points, 2)
    return totals


def leaderboard_state():
    state = maybe_refresh_state()
    owners = []
    for owner in state["owners"]:
        enriched_players = []
        owner_total = 0
        for player in owner["players"]:
            player_copy = copy.deepcopy(player)
            player_copy.update(aggregate_player_totals(player, owner, state))
            owner_total += player_copy["points"]
            enriched_players.append(player_copy)
        enriched_players.sort(key=lambda item: item["points"], reverse=True)
        owner_copy = {
            "owner_name": owner["owner_name"],
            "captain": owner["captain"],
            "vice_captain": owner["vice_captain"],
            "original_captain": owner.get("original_captain", owner["captain"]),
            "original_vice_captain": owner.get("original_vice_captain", owner["vice_captain"]),
            "captain_change_used": owner.get("captain_change_used", False),
            "captain_change_from_match_id": owner.get("captain_change_from_match_id", ""),
            "changed_captain": owner.get("changed_captain", ""),
            "changed_vice_captain": owner.get("changed_vice_captain", ""),
            "players": enriched_players,
            "total_points": round(owner_total, 2),
            "player_count": len(enriched_players),
        }
        owners.append(owner_copy)
    owners.sort(key=lambda item: item["total_points"], reverse=True)
    return {
        "owners": owners,
        "point_rules": HOWSTAT_POINT_RULES,
        "live_matches": state.get("live_matches", []),
        "match_catalog": state.get("match_catalog", {}),
        "matches_for_changes": ordered_match_catalog(state),
        "last_sync_at": state.get("last_sync_at"),
        "last_sync_message": state.get("last_sync_message"),
        "settings": {
            "series_filter": state["settings"].get("series_filter", DEFAULT_SETTINGS["series_filter"]),
            "selected_match_ids": state["settings"].get("selected_match_ids", []),
            "has_api_key": bool(state["settings"].get("cricketdata_api_key")),
        },
    }


def ordered_match_catalog(state):
    catalog = list((state.get("match_catalog") or {}).values())
    catalog.sort(
        key=lambda item: (
            str(item.get("date_time_gmt") or ""),
            int(item.get("match_number") or 9999) if str(item.get("match_number") or "").isdigit() else 9999,
            str(item.get("name") or ""),
        )
    )
    return catalog


def get_setting_api_key(state):
    return (state.get("settings", {}).get("cricketdata_api_key") or "").strip()


def fetch_json(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=25) as response:
        return json.loads(response.read().decode("utf-8"))


def make_api_url(endpoint, **params):
    query = urllib.parse.urlencode(params)
    return f"{API_BASE}/{endpoint}?{query}"


def pick_first(mapping, keys, default=None):
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return default


def int_value(value):
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def flatten_matches_payload(payload):
    results = []

    def walk(node):
        if isinstance(node, dict):
            match_id = pick_first(node, ["id", "matchId", "unique_id"])
            name = pick_first(node, ["name", "matchTitle", "title"])
            if match_id and name:
                results.append(node)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    unique = {}
    for item in results:
        match_id = str(pick_first(item, ["id", "matchId", "unique_id"]))
        unique[match_id] = item
    return list(unique.values())


def score_summary(match):
    score = match.get("score")
    if isinstance(score, list):
        parts = []
        for entry in score:
            if isinstance(entry, dict):
                inning = pick_first(entry, ["inning", "innings"], "")
                runs = pick_first(entry, ["r", "runs"], "")
                wickets = pick_first(entry, ["w", "wickets"], "")
                overs = pick_first(entry, ["o", "overs"], "")
                parts.append(f"{inning} {runs}/{wickets} ({overs})".strip())
        return " | ".join(parts)
    if isinstance(score, str):
        return score
    return match.get("status", "No score yet")


def match_sort_key(match_info):
    raw = (
        match_info.get("dateTimeGMT")
        or match_info.get("date")
        or match_info.get("matchStarted")
        or ""
    )
    if raw:
        return str(raw)
    return str(match_info.get("id", ""))


def filter_ipl_matches(matches, series_filter):
    wanted = series_filter.lower()
    filtered = []
    catalog = {}
    for match in matches:
        raw_text = " ".join(
            [
                str(match.get("name", "")),
                str(match.get("series_id", "")),
                str(match.get("series", "")),
                str(match.get("matchType", "")),
                str(match.get("status", "")),
            ]
        ).lower()
        if wanted in raw_text or "ipl" in raw_text:
            match_id = str(pick_first(match, ["id", "matchId", "unique_id"]))
            item = {
                "id": match_id,
                "name": str(pick_first(match, ["name", "title"], "Match")),
                "status": str(match.get("status", "Unknown")),
                "match_type": str(match.get("matchType", "")),
                "score_summary": score_summary(match),
                "date_time_gmt": str(pick_first(match, ["dateTimeGMT", "date"], "")),
            }
            filtered.append(item)
            catalog[match_id] = {
                "id": match_id,
                "name": item["name"],
                "date_time_gmt": item["date_time_gmt"],
                "status": item["status"],
            }
    filtered.sort(key=lambda item: item["name"])
    return filtered, catalog


def refresh_live_matches(state):
    api_key = get_setting_api_key(state)
    if not api_key:
        raise ValueError("Add your CricketData API key first.")
    payload = fetch_json(make_api_url("currentMatches", apikey=api_key, offset=0))
    matches = flatten_matches_payload(payload)
    filtered, catalog = filter_ipl_matches(matches, state["settings"].get("series_filter", DEFAULT_SETTINGS["series_filter"]))
    state["live_matches"] = filtered
    state["match_catalog"].update(catalog)
    save_state(state)
    return filtered


def extract_text(value):
    if value is None:
        return ""
    return str(value).strip()


def maybe_record_batting(entry, stats_by_player, fingerprints, state):
    name = extract_text(pick_first(entry, ["batsman", "batter", "name", "playerName", "fullName"]))
    runs = int_value(pick_first(entry, ["r", "runs"]))
    balls = pick_first(entry, ["b", "balls", "bf"])
    dismissal = extract_text(pick_first(entry, ["dismissal", "dismissalText", "howOut"]))
    if not name or ("runs" not in entry and "r" not in entry):
        return
    if balls in (None, "") and not dismissal and "fours" not in entry and "4s" not in entry:
        return
    player_name = resolve_player_name(name, state)
    if not player_name:
        return
    fingerprint = ("bat", player_name, runs, extract_text(balls), dismissal)
    if fingerprint in fingerprints:
        return
    fingerprints.add(fingerprint)
    stats_by_player.setdefault(player_name, {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0})
    stats_by_player[player_name]["runs"] += runs

    catch_match = re.search(r"\bc\s+([A-Za-z .'-]+?)\s+b\b", dismissal, re.IGNORECASE)
    if catch_match and "&" not in catch_match.group(1):
        catcher = resolve_player_name(catch_match.group(1), state)
        if catcher:
            stats_by_player.setdefault(catcher, {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0})
            stats_by_player[catcher]["catches"] += 1
    stumping_match = re.search(r"\bst\s+([A-Za-z .'-]+?)\s+b\b", dismissal, re.IGNORECASE)
    if stumping_match:
        keeper = resolve_player_name(stumping_match.group(1), state)
        if keeper:
            stats_by_player.setdefault(keeper, {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0})
            stats_by_player[keeper]["stumpings"] += 1


def maybe_record_bowling(entry, stats_by_player, fingerprints, state):
    name = extract_text(pick_first(entry, ["bowler", "name", "playerName", "fullName"]))
    wickets = int_value(pick_first(entry, ["w", "wkts", "wickets"]))
    overs = pick_first(entry, ["o", "overs"])
    economy = pick_first(entry, ["econ", "economy"])
    if not name or ("wickets" not in entry and "wkts" not in entry and "w" not in entry):
        return
    if overs in (None, "") and economy in (None, ""):
        return
    player_name = resolve_player_name(name, state)
    if not player_name:
        return
    fingerprint = ("bowl", player_name, wickets, extract_text(overs), extract_text(economy))
    if fingerprint in fingerprints:
        return
    fingerprints.add(fingerprint)
    stats_by_player.setdefault(player_name, {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0})
    stats_by_player[player_name]["wickets"] += wickets


def parse_scorecard_payload(payload, state):
    stats_by_player = {}
    fingerprints = set()

    def walk(node):
        if isinstance(node, dict):
            maybe_record_batting(node, stats_by_player, fingerprints, state)
            maybe_record_bowling(node, stats_by_player, fingerprints, state)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return stats_by_player


def find_match_name(match_id, state):
    for match in state.get("live_matches", []):
        if str(match.get("id")) == str(match_id):
            return match.get("name", f"Match {match_id}")
    return f"Match {match_id}"


def find_owner(state, owner_name):
    for owner in state["owners"]:
        if owner["owner_name"] == owner_name:
            return owner
    return None


def apply_captain_change(state, owner_name, new_captain, new_vice_captain, from_match_id):
    owner = find_owner(state, owner_name)
    if not owner:
        raise ValueError("Owner not found.")
    if owner.get("captain_change_used"):
        raise ValueError(f"{owner_name} has already used the one-time captain change.")
    valid_names = {player["player_name"] for player in owner["players"]}
    if new_captain not in valid_names or new_vice_captain not in valid_names:
        raise ValueError("Captain and vice-captain must belong to that owner's squad.")
    if new_captain == new_vice_captain:
        raise ValueError("Captain and vice-captain must be different players.")
    if not from_match_id:
        raise ValueError("Choose the match from which this change should apply.")

    owner["captain_change_used"] = True
    owner["captain_change_from_match_id"] = str(from_match_id)
    owner["changed_captain"] = new_captain
    owner["changed_vice_captain"] = new_vice_captain
    owner["captain"] = new_captain
    owner["vice_captain"] = new_vice_captain

    for player in owner["players"]:
        player["is_captain"] = player["player_name"] == new_captain
        player["is_vice_captain"] = player["player_name"] == new_vice_captain
    save_state(state)


def fetch_text(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


def fetch_bytes(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def extract_escaped_json(html, token):
    start = html.find(token)
    if start == -1:
        return None
    start += len(token)
    data = html[start:]
    opening = data[0]
    closing = "]" if opening == "[" else "}"
    depth = 0
    in_string = False
    escape = False
    end = None
    for index, char in enumerate(data):
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if not in_string:
            if char == opening:
                depth += 1
            elif char == closing:
                depth -= 1
                if depth == 0:
                    end = index + 1
                    break
    if end is None:
        return None
    snippet = data[:end]
    return json.loads(snippet.encode("utf-8").decode("unicode_escape"))


def parse_match_number(text):
    if text is None:
        return None
    match = re.search(r"(\d+)", str(text))
    return int(match.group(1)) if match else None


def canonical_match_key(match_number, fallback_id):
    if match_number is not None:
        return f"IPL2026-M{int(match_number):02d}"
    return str(fallback_id)


def derive_player_stats_from_scorecards(scorecards):
    stats_by_player = {}

    def ensure(name):
        stats_by_player.setdefault(name, {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0})

    for innings in scorecards or []:
        batters = ((innings.get("batTeamDetails") or {}).get("batsmenData") or {}).values()
        for batter in batters:
            name = batter.get("batName")
            if not name:
                continue
            ensure(name)
            stats_by_player[name]["runs"] += int_value(batter.get("runs"))

            out_desc = str(batter.get("outDesc") or "")
            st_match = re.search(r"\bst\s+([A-Za-z .'-]+?)\s+b\b", out_desc, re.IGNORECASE)
            if st_match:
                ensure(st_match.group(1))
                stats_by_player[st_match.group(1)]["stumpings"] += 1
            catch_match = re.search(r"\bc\s+([A-Za-z .'-]+?)\s+b\b", out_desc, re.IGNORECASE)
            if catch_match and "&" not in catch_match.group(1):
                ensure(catch_match.group(1))
                stats_by_player[catch_match.group(1)]["catches"] += 1

        bowlers = ((innings.get("bowlTeamDetails") or {}).get("bowlersData") or {}).values()
        for bowler in bowlers:
            name = bowler.get("bowlName")
            if not name:
                continue
            ensure(name)
            stats_by_player[name]["wickets"] += int_value(bowler.get("wickets"))

    return stats_by_player


def discover_cricbuzz_ipl_matches():
    discovered = {}
    for url in [CRICBUZZ_LIVE_URL, CRICBUZZ_RECENT_URL]:
        html = fetch_text(url)
        for match_id, slug in re.findall(r"/live-cricket-scores/(\d+)/([^\"\\'\s<]+)", html):
            if "indian-premier-league-2026" not in slug and "ipl-2026" not in slug:
                continue
            discovered[match_id] = slug
    return discovered


def scrape_cricbuzz_match(match_id, slug):
    url = f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}/{slug}"
    html = fetch_text(url)
    scorecards = extract_escaped_json(html, 'scoreCard\\":') or []
    match_header = extract_escaped_json(html, 'matchHeader\\":') or {}
    match_info = extract_escaped_json(html, 'matchInfo\\":') or {}
    status = str(match_header.get("status") or match_info.get("status") or "")
    match_desc = str(match_header.get("matchDescription") or match_info.get("matchDesc") or "")
    match_number = parse_match_number(match_desc)
    match_key = canonical_match_key(match_number, match_id)
    teams = []
    team1 = ((match_header.get("team1") or {}).get("name") or (match_info.get("team1") or {}).get("teamName") or "")
    team2 = ((match_header.get("team2") or {}).get("name") or (match_info.get("team2") or {}).get("teamName") or "")
    if team1 and team2:
        teams = [team1, team2]
    stats = derive_player_stats_from_scorecards(scorecards)
    return {
        "match_id": str(match_id),
        "match_key": match_key,
        "match_number": match_number,
        "name": f"{team1} vs {team2}".strip(" vs ") if teams else slug.replace("-", " "),
        "status": status,
        "slug": slug,
        "teams": teams,
        "scorecards": scorecards,
        "stats": stats,
        "date_time_gmt": "",
        "is_complete": str(match_header.get("state") or "").lower() == "complete" or "won by" in status.lower(),
    }


def cricsheet_match_stats(match_data):
    stats_by_player = {}

    def ensure(name):
        stats_by_player.setdefault(name, {"runs": 0, "wickets": 0, "catches": 0, "stumpings": 0})

    for innings in match_data.get("innings", []):
        for over in innings.get("overs", []):
            for delivery in over.get("deliveries", []):
                batter = delivery.get("batter")
                if batter:
                    ensure(batter)
                    stats_by_player[batter]["runs"] += int_value((delivery.get("runs") or {}).get("batter"))

                wicket_entries = delivery.get("wickets") or []
                for wicket in wicket_entries:
                    kind = str(wicket.get("kind") or "").lower()
                    bowler = delivery.get("bowler")
                    if bowler and kind not in {"run out", "retired hurt", "retired out", "obstructing the field"}:
                        ensure(bowler)
                        stats_by_player[bowler]["wickets"] += 1
                    for fielder in wicket.get("fielders") or []:
                        name = fielder.get("name")
                        if not name:
                            continue
                        ensure(name)
                        if kind == "stumped":
                            stats_by_player[name]["stumpings"] += 1
                        elif kind == "caught":
                            stats_by_player[name]["catches"] += 1

    return stats_by_player


def apply_match_stats(state, match_key, match_name, match_number, match_date, stats_by_external_name, source):
    mapped_count = 0
    player_lookup = {}
    for owner in state["owners"]:
        for player in owner["players"]:
            player_lookup[player["player_name"]] = player

    for external_name, stats in stats_by_external_name.items():
        resolved = resolve_player_name(external_name, state)
        if not resolved:
            continue
        player_lookup[resolved]["matches"][match_key] = {
            "match_name": match_name,
            "match_number": match_number,
            "match_date": match_date,
            "runs": int_value(stats.get("runs")),
            "wickets": int_value(stats.get("wickets")),
            "catches": int_value(stats.get("catches")),
            "stumpings": int_value(stats.get("stumpings")),
            "source": source,
            "last_updated": dt.datetime.now().isoformat(timespec="seconds"),
        }
        mapped_count += 1

    state.setdefault("match_catalog", {})
    state["match_catalog"][match_key] = {
        "id": match_key,
        "name": match_name,
        "match_number": match_number,
        "date_time_gmt": match_date,
        "status": source,
    }
    return mapped_count


def backfill_from_cricsheet(state):
    raw = fetch_bytes(CRICSHEET_IPL_JSON_ZIP)
    archive = zipfile.ZipFile(io.BytesIO(raw))
    processed = 0
    for file_name in archive.namelist():
        if not file_name.endswith(".json"):
            continue
        match = json.loads(archive.read(file_name).decode("utf-8"))
        info = match.get("info", {})
        event = info.get("event", {})
        if event.get("name") != "Indian Premier League":
            continue
        dates = info.get("dates", [])
        if not dates or not str(dates[0]).startswith("2026-"):
            continue
        match_number = parse_match_number(event.get("match_number"))
        match_key = canonical_match_key(match_number, file_name.replace(".json", ""))
        match_name = " vs ".join(info.get("teams", []))
        stats = cricsheet_match_stats(match)
        apply_match_stats(
            state=state,
            match_key=match_key,
            match_name=match_name,
            match_number=match_number,
            match_date=str(dates[0]),
            stats_by_external_name=stats,
            source="cricsheet",
        )
        processed += 1
    state["historical_backfill_at"] = dt.datetime.now().isoformat(timespec="seconds")
    return processed


def refresh_live_from_cricbuzz(state):
    discovered = discover_cricbuzz_ipl_matches()
    live_matches = []
    for match_id, slug in discovered.items():
        try:
            match = scrape_cricbuzz_match(match_id, slug)
        except Exception:
            continue
        apply_match_stats(
            state=state,
            match_key=match["match_key"],
            match_name=match["name"],
            match_number=match["match_number"],
            match_date=match["date_time_gmt"],
            stats_by_external_name=match["stats"],
            source="cricbuzz",
        )
        live_matches.append(
            {
                "id": match["match_key"],
                "name": match["name"],
                "status": match["status"],
                "match_number": match["match_number"],
                "score_summary": match["status"],
            }
        )
    state["live_matches"] = sorted(live_matches, key=lambda item: (item.get("match_number") is None, item.get("match_number") or 999))
    state["last_live_sync_at"] = dt.datetime.now().isoformat(timespec="seconds")
    return len(live_matches)


def maybe_refresh_state(force=False):
    state = load_state()
    now = dt.datetime.now()
    hist_due = force or not state.get("historical_backfill_at")
    live_due = force or not state.get("last_live_sync_at")
    if not hist_due and state.get("historical_backfill_at"):
        try:
            hist_due = (now - dt.datetime.fromisoformat(state["historical_backfill_at"])).total_seconds() > 21600
        except Exception:
            hist_due = True
    if not live_due and state.get("last_live_sync_at"):
        try:
            live_due = (now - dt.datetime.fromisoformat(state["last_live_sync_at"])).total_seconds() > 20
        except Exception:
            live_due = True

    notes = []
    if hist_due:
        try:
            processed = backfill_from_cricsheet(state)
            notes.append(f"historical backfill ok ({processed} matches)")
        except Exception as exc:
            notes.append(f"historical backfill failed: {exc}")
    if live_due:
        try:
            current = refresh_live_from_cricbuzz(state)
            notes.append(f"live refresh ok ({current} tracked matches)")
        except Exception as exc:
            notes.append(f"live refresh failed: {exc}")
    if notes:
        state["last_sync_message"] = " | ".join(notes)
        state["last_sync_at"] = now.isoformat(timespec="seconds")
        save_state(state)
    return state


def sync_selected_matches(state):
    api_key = get_setting_api_key(state)
    if not api_key:
        raise ValueError("Add your CricketData API key first.")

    refresh_live_matches(state)
    selected_ids = state["settings"].get("selected_match_ids", [])
    if not selected_ids:
        live_ids = [
            match["id"]
            for match in state.get("live_matches", [])
            if "live" in match.get("status", "").lower() or "innings" in match.get("status", "").lower()
        ]
        selected_ids = live_ids
        state["settings"]["selected_match_ids"] = selected_ids

    if not selected_ids:
        raise ValueError("No IPL live matches were selected or detected.")

    player_index = {}
    for owner in state["owners"]:
        for player in owner["players"]:
            player_index[player["player_name"]] = player

    synced = []
    for match_id in selected_ids:
        payload = fetch_json(make_api_url("match_scorecard", apikey=api_key, id=match_id))
        parsed = parse_scorecard_payload(payload, state)
        match_name = find_match_name(match_id, state)
        match_info = state.get("match_catalog", {}).get(str(match_id), {})
        for player_name, stats in parsed.items():
            player = player_index.get(player_name)
            if not player:
                continue
            player["matches"][str(match_id)] = {
                "match_name": match_name,
                "match_date": match_info.get("date_time_gmt", ""),
                "runs": stats["runs"],
                "wickets": stats["wickets"],
                "catches": stats["catches"],
                "stumpings": stats["stumpings"],
                "last_updated": dt.datetime.now().isoformat(timespec="seconds"),
            }
        synced.append(match_name)

    state["last_sync_at"] = dt.datetime.now().isoformat(timespec="seconds")
    state["last_sync_message"] = f"Synced {len(synced)} match(es): " + ", ".join(synced)
    save_state(state)
    return synced


LEADERBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>IPL 2026 Fantasy Leaderboard</title>
  <style>
    :root {
      --bg1: #07121d; --bg2: #10243a; --panel: rgba(10,22,38,.85); --line: rgba(255,255,255,.09);
      --text: #f4f7fb; --muted: #96abc2; --gold: #ffbe0b; --green: #34d399;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; color: var(--text); font-family: "Segoe UI", sans-serif;
      background: radial-gradient(circle at top left, rgba(255,190,11,.17), transparent 24%),
                  linear-gradient(160deg, var(--bg1), var(--bg2));
      min-height: 100vh;
    }
    .shell { width: min(760px, calc(100% - 20px)); margin: 14px auto 24px; }
    .hero, .card {
      background: var(--panel); border: 1px solid var(--line); border-radius: 22px; backdrop-filter: blur(16px);
    }
    .hero { padding: 22px 18px; }
    h1 { margin: 0; font-size: clamp(28px, 7vw, 42px); text-transform: uppercase; letter-spacing: .04em; }
    .sub { margin-top: 10px; color: var(--muted); line-height: 1.5; font-size: 14px; }
    .meta { margin-top: 12px; display: inline-block; background: rgba(52,211,153,.12); color: var(--green); padding: 8px 12px; border-radius: 999px; font-size: 12px; }
    .list { margin-top: 16px; display: grid; gap: 12px; }
    .card { padding: 14px; display: flex; justify-content: space-between; gap: 12px; align-items: center; }
    .left { display: flex; gap: 12px; align-items: center; min-width: 0; }
    .rank { width: 40px; height: 40px; border-radius: 14px; display: grid; place-items: center; font-weight: 700;
      background: linear-gradient(180deg, rgba(255,190,11,.22), rgba(255,190,11,.06)); color: var(--gold); flex: 0 0 auto; }
    .name { font-size: 19px; font-weight: 700; }
    .tiny { color: var(--muted); font-size: 12px; line-height: 1.4; }
    .pts { color: var(--green); font-size: 26px; font-weight: 800; white-space: nowrap; }
    .matches { margin-top: 18px; padding: 14px; }
    .match { padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,.06); }
    .match:last-child { border-bottom: 0; }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>IPL 2026</h1>
      <div class="sub">Fantasy leaderboard with automatic historical backfill and live score refresh.</div>
      <div class="meta" id="syncText">Loading latest points...</div>
    </section>
    <section class="list" id="leaderboard"></section>
    <section class="hero matches">
      <div style="font-weight:700;font-size:18px;">Tracked Matches</div>
      <div id="matches" class="sub">Loading matches...</div>
    </section>
  </div>
  <script>
    async function refreshBoard() {
      const response = await fetch('/api/state');
      const state = await response.json();
      document.getElementById('syncText').textContent = state.last_sync_message || 'Live refresh running';
      document.getElementById('leaderboard').innerHTML = state.owners.map((owner, index) => `
        <div class="card">
          <div class="left">
            <div class="rank">#${index + 1}</div>
            <div>
              <div class="name">${owner.owner_name}</div>
              <div class="tiny">Captain: ${owner.captain} | VC: ${owner.vice_captain}</div>
            </div>
          </div>
          <div class="pts">${owner.total_points}</div>
        </div>
      `).join('');
      document.getElementById('matches').innerHTML = state.live_matches.length
        ? state.live_matches.map(match => `<div class="match"><div style="font-weight:700">${match.name}</div><div class="tiny">${match.status}</div></div>`).join('')
        : 'No current IPL live matches detected right now.';
    }
    refreshBoard();
    setInterval(refreshBoard, 15000);
  </script>
</body>
</html>
"""


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>IPL 2026 Fantasy League</title>
  <style>
    :root {
      --bg: #08131f;
      --panel: rgba(9, 24, 40, 0.84);
      --panel-2: rgba(16, 37, 58, 0.96);
      --line: rgba(255,255,255,0.10);
      --text: #f6f7fb;
      --muted: #9cb3c9;
      --accent: #ffb703;
      --accent-2: #34d399;
      --danger: #fb7185;
      --shadow: 0 24px 80px rgba(0,0,0,0.35);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      font-family: "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(255,183,3,0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(52,211,153,0.15), transparent 24%),
        linear-gradient(160deg, #08131f 0%, #0b1a2b 40%, #10243a 100%);
      min-height: 100vh;
    }
    .shell { width: min(1480px, calc(100% - 28px)); margin: 20px auto 40px; }
    .hero, .panel {
      border: 1px solid var(--line);
      border-radius: 24px;
      background: var(--panel);
      backdrop-filter: blur(18px);
      box-shadow: var(--shadow);
    }
    .hero { padding: 28px; }
    .hero h1 { margin: 0; font-size: clamp(30px, 4vw, 54px); letter-spacing: 0.04em; text-transform: uppercase; }
    .hero p { color: var(--muted); max-width: 960px; line-height: 1.6; margin: 12px 0 0; }
    .status-bar {
      margin-top: 14px; display: inline-block; padding: 10px 14px; border-radius: 999px;
      background: rgba(52,211,153,0.12); color: var(--accent-2); font-size: 13px;
    }
    .grid { display: grid; grid-template-columns: 1.05fr 0.95fr; gap: 18px; margin-top: 18px; }
    .panel { padding: 20px; }
    .panel h2 { margin: 0 0 8px; font-size: 22px; }
    .sub { color: var(--muted); font-size: 14px; line-height: 1.5; margin-bottom: 16px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 14px; }
    button, input {
      border: 1px solid rgba(255,255,255,0.12); border-radius: 14px; background: rgba(255,255,255,0.06);
      color: var(--text); padding: 12px 14px; font-size: 14px;
    }
    input { width: min(480px, 100%); }
    button { cursor: pointer; background: linear-gradient(180deg, rgba(255,183,3,0.24), rgba(255,183,3,0.08)); }
    .ghost { background: rgba(255,255,255,0.05); }
    .leaderboard-item, .match-item, .player-line {
      border: 1px solid rgba(255,255,255,0.06); border-radius: 18px; background: var(--panel-2); padding: 14px 16px; margin-bottom: 12px;
    }
    .leaderboard-item { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
    .rank {
      width: 42px; height: 42px; border-radius: 14px; display: grid; place-items: center;
      background: linear-gradient(180deg, rgba(255,183,3,0.22), rgba(255,183,3,0.06)); color: var(--accent); font-weight: 700; flex: 0 0 auto;
    }
    .points { font-size: 24px; color: var(--accent-2); font-weight: 700; white-space: nowrap; }
    .rules { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
    .rule { padding: 12px; border-radius: 16px; background: rgba(255,255,255,0.05); line-height: 1.5; }
    .match-item label { display: flex; gap: 12px; align-items: flex-start; cursor: pointer; }
    .match-item input[type="checkbox"] { width: auto; margin-top: 2px; }
    .owners { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    .owner-card { border: 1px solid rgba(255,255,255,0.06); border-radius: 18px; background: var(--panel-2); padding: 16px; }
    .player-line { display: flex; justify-content: space-between; gap: 12px; margin-bottom: 10px; background: rgba(255,255,255,0.04); }
    .small { color: var(--muted); font-size: 12px; line-height: 1.4; }
    .message { min-height: 24px; font-size: 14px; margin-top: 10px; color: var(--accent-2); }
    .warning { color: #ffd166; font-size: 13px; line-height: 1.5; margin-top: 8px; }
    .change-box { margin: 14px 0 10px; padding: 12px; border-radius: 16px; background: rgba(255,255,255,0.05); }
    .change-grid { display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 8px; }
    select {
      border: 1px solid rgba(255,255,255,0.12); border-radius: 14px; background: rgba(255,255,255,0.06);
      color: var(--text); padding: 10px 12px; font-size: 14px; width: 100%;
    }
    @media (max-width: 980px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>IPL 2026 Fantasy League</h1>
      <p>Admin view for the fantasy league. Public users should only use the main leaderboard page. This panel is only for checking tracked matches and using one-time captain and vice-captain changes.</p>
      <div class="status-bar" id="syncBadge">Waiting for first sync</div>
    </section>

    <div class="grid">
      <section class="panel">
        <h2>Admin Tools</h2>
        <div class="sub">The app backfills old IPL 2026 matches automatically from Cricsheet and refreshes live scorecards automatically from Cricbuzz. Use this page only when you want to force a refresh or apply one-time captain changes.</div>
        <div class="toolbar">
          <button onclick="forceRefresh()">Force Refresh Now</button>
          <button class="ghost" onclick="refreshState()">Refresh Screen</button>
        </div>
        <div class="warning">No API key is needed. Automatic updates run in the background whenever people open the site, and the public leaderboard refreshes itself every 15 seconds.</div>
        <div id="message" class="message"></div>
      </section>

      <section class="panel">
        <h2>How Points Work</h2>
        <div class="sub">These rules were copied from your old project. Fielding points apply only to wicketkeepers because that is how the original rules were coded.</div>
        <div id="rules" class="rules"></div>
      </section>
    </div>

    <div class="grid">
      <section class="panel">
        <h2>Leaderboard</h2>
        <div class="sub">Owner totals are cumulative across all backfilled and live-refreshed matches.</div>
        <div id="leaderboard"></div>
      </section>

      <section class="panel">
        <h2>Detected IPL Matches</h2>
        <div class="sub">These are the live matches currently being tracked automatically.</div>
        <div id="matches"></div>
      </section>
    </div>

    <section class="panel" style="margin-top:18px;">
      <h2>Owner Squads</h2>
      <div class="sub">These squads were copied into this new app and are read-only here. Tejas currently has 11 players because that is what the source data contained.</div>
      <div id="owners" class="owners"></div>
    </section>
  </div>

  <script>
    let state = null;
    function setMessage(text, isError=false) {
      const el = document.getElementById("message");
      el.style.color = isError ? "#fb7185" : "#34d399";
      el.textContent = text;
    }
    function renderRules() {
      const container = document.getElementById("rules");
      container.innerHTML = Object.entries(state.point_rules).map(([key, value]) => `
        <div class="rule"><strong>${key.replaceAll("_", " ")}</strong><br>${value}</div>
      `).join("");
    }
    function renderLeaderboard() {
      const container = document.getElementById("leaderboard");
      container.innerHTML = state.owners.map((owner, index) => `
        <div class="leaderboard-item">
          <div style="display:flex;gap:12px;align-items:center;">
            <div class="rank">#${index + 1}</div>
            <div>
              <div style="font-weight:700;font-size:18px;">${owner.owner_name}</div>
              <div class="small">Captain: ${owner.captain} | VC: ${owner.vice_captain} | Players: ${owner.player_count}</div>
            </div>
          </div>
          <div class="points">${owner.total_points} pts</div>
        </div>
      `).join("");
    }
    function renderMatches() {
      const container = document.getElementById("matches");
      if (!state.live_matches.length) {
        container.innerHTML = `<div class="small">No live IPL matches detected right now. Historical matches are still counted from automatic backfill.</div>`;
        return;
      }
      container.innerHTML = state.live_matches.map(match => `
        <div class="match-item">
          <div>
            <div style="font-weight:700;">${match.name}</div>
            <div class="small">${match.status}</div>
            <div class="small">${match.score_summary}</div>
          </div>
        </div>
      `).join("");
    }
    function renderOwners() {
      const container = document.getElementById("owners");
      container.innerHTML = state.owners.map(owner => `
        <div class="owner-card">
          <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">
            <div>
              <div style="font-size:20px;font-weight:700;">${owner.owner_name}</div>
              <div class="small">Captain: ${owner.captain}</div>
              <div class="small">Vice-captain: ${owner.vice_captain}</div>
              <div class="small">${owner.captain_change_used ? `One-time change used from ${matchName(owner.captain_change_from_match_id)}` : "One-time captain/VC change available"}</div>
            </div>
            <div class="points" style="font-size:18px;">${owner.total_points}</div>
          </div>
          <div class="change-box">
            ${owner.captain_change_used ? `
              <div class="small">Changed captain: ${owner.changed_captain}</div>
              <div class="small">Changed vice-captain: ${owner.changed_vice_captain}</div>
            ` : `
              <div style="font-weight:700;">One-Time Captain / VC Change</div>
              <div class="change-grid">
                <select id="captain_${safeId(owner.owner_name)}">${owner.players.map(player => `<option value="${player.player_name}" ${player.player_name === owner.captain ? "selected" : ""}>Captain: ${player.player_name}</option>`).join("")}</select>
                <select id="vice_${safeId(owner.owner_name)}">${owner.players.map(player => `<option value="${player.player_name}" ${player.player_name === owner.vice_captain ? "selected" : ""}>Vice-captain: ${player.player_name}</option>`).join("")}</select>
                <select id="match_${safeId(owner.owner_name)}">
                  <option value="">Apply from match...</option>
                  ${matchOptions()}
                </select>
                <button onclick="applyCaptainChange('${owner.owner_name}')">Use One-Time Change</button>
              </div>
            `}
          </div>
          <div style="margin-top:14px;">
            ${owner.players.map(player => `
              <div class="player-line">
                <div>
                  <div style="font-weight:700;">${player.player_name}</div>
                  <div class="small">Runs ${player.runs} | Wickets ${player.wickets} | Catches ${player.catches} | Stumpings ${player.stumpings}</div>
                </div>
                <div style="text-align:right;">
                  <div style="font-weight:700;color:#34d399;">${player.points} pts</div>
                  <div class="small">${player.is_captain ? "Captain" : player.is_vice_captain ? "Vice-captain" : player.is_wicketkeeper ? "Wicketkeeper" : ""}</div>
                </div>
              </div>
            `).join("")}
          </div>
        </div>
      `).join("");
    }
    function safeId(value) {
      return value.replace(/[^a-zA-Z0-9]/g, "_");
    }
    function matchOptions() {
      const matches = state.matches_for_changes || [];
      return matches.map(match => `<option value="${match.id}">${match.match_number ? `Match ${match.match_number}: ` : ""}${match.name}</option>`).join("");
    }
    function matchName(matchId) {
      const found = state.live_matches.find(match => String(match.id) === String(matchId));
      if (found) return found.name;
      const catalog = state.match_catalog || {};
      return (catalog[String(matchId)] && catalog[String(matchId)].name) || matchId || "selected match";
    }
    function renderMeta() {
      const badge = document.getElementById("syncBadge");
      badge.textContent = state.last_sync_message || "Waiting for first sync";
    }
    async function draw(payload) {
      state = payload;
      renderRules();
      renderLeaderboard();
      renderMatches();
      renderOwners();
      renderMeta();
    }
    async function refreshState() {
      const response = await fetch("/api/state");
      await draw(await response.json());
    }
    async function forceRefresh() {
      setMessage("Refreshing historical and live data...");
      const response = await fetch("/api/refresh-now", { method: "POST" });
      const payload = await response.json();
      if (payload.error) return setMessage(payload.error, true);
      await draw(payload);
      setMessage(payload.last_sync_message);
    }
    async function applyCaptainChange(ownerName) {
      const id = safeId(ownerName);
      const captain = document.getElementById(`captain_${id}`).value;
      const vice = document.getElementById(`vice_${id}`).value;
      const fromMatchId = document.getElementById(`match_${id}`).value;
      const response = await fetch("/api/captain-change", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ owner_name: ownerName, new_captain: captain, new_vice_captain: vice, from_match_id: fromMatchId })
      });
      const payload = await response.json();
      if (payload.error) return setMessage(payload.error, true);
      await draw(payload);
      setMessage(`Captain/VC change locked for ${ownerName}.`);
    }
    refreshState();
    setInterval(refreshState, 15000);
  </script>
</body>
</html>
"""


class FantasyCricketHandler(BaseHTTPRequestHandler):
    def json_response(self, payload, status=200):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def html_response(self, payload):
        encoded = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def read_json(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else b"{}"
        return json.loads(body.decode("utf-8"))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self.html_response(LEADERBOARD_HTML)
            return
        if path == "/admin":
            self.html_response(HTML)
            return
        if path == "/api/state":
            self.json_response(leaderboard_state())
            return
        self.send_error(404, "Not Found")

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/refresh-now":
                maybe_refresh_state(force=True)
                self.json_response(leaderboard_state())
                return

            if path == "/api/captain-change":
                payload = self.read_json()
                state = load_state()
                apply_captain_change(
                    state=state,
                    owner_name=str(payload.get("owner_name", "")),
                    new_captain=str(payload.get("new_captain", "")),
                    new_vice_captain=str(payload.get("new_vice_captain", "")),
                    from_match_id=str(payload.get("from_match_id", "")),
                )
                self.json_response(leaderboard_state())
                return

        except ValueError as exc:
            self.json_response({"error": str(exc)}, status=400)
            return
        except urllib.error.HTTPError as exc:
            self.json_response({"error": f"Live data source returned HTTP {exc.code}."}, status=502)
            return
        except urllib.error.URLError:
            self.json_response({"error": "Could not reach the live cricket source right now."}, status=502)
            return
        except Exception as exc:
            self.json_response({"error": f"Unexpected error: {exc}"}, status=500)
            return

        self.send_error(404, "Not Found")

    def log_message(self, format_string, *args):
        return


if __name__ == "__main__":
    load_state()
    server = ThreadingHTTPServer((HOST, PORT), FantasyCricketHandler)
    print(f"Fantasy cricket app running at http://127.0.0.1:{PORT}")
    print("Open the URL in your browser. Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.server_close()
