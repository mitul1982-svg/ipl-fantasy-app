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
from http import cookies


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "fantasy_data.json")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8000"))
API_BASE = "https://api.cricapi.com/v1"
CRICBUZZ_LIVE_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
CRICBUZZ_RECENT_URL = "https://www.cricbuzz.com/cricket-match/live-scores/recent-matches"
CRICSHEET_IPL_JSON_ZIP = "https://cricsheet.org/downloads/ipl_json.zip"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "ipl2026")
ADMIN_COOKIE_NAME = "ipl_admin_auth"
ADMIN_COOKIE_VALUE = "ok"

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

PLAYER_TEAMS = {
    "S P Narine": "KKR", "Ravi Bishnoi": "RR", "A R Patel": "DC",
    "Rashid Khan": "GT", "N Pooran": "LSG", "A K Markram": "LSG",
    "N K Reddy": "SRH", "Washington Sundar": "GT", "D A Miller": "DC",
    "N Burger": "RR", "R D Chahar": "CSK",
    "F H Allen": "KKR", "S A Yadav": "MI", "C Green": "KKR",
    "J M Sharma": "RCB", "Kartik Sharma": "CSK", "K Rabada": "GT",
    "M A Starc": "DC", "Arshdeep Singh": "PBKS", "T Natarajan": "DC",
    "Mayank Yadav": "LSG", "Ashok Sharma": "GT",
    "Abhishek Sharma": "SRH", "C V Varun": "KKR", "P D Salt": "RCB",
    "T L Seifert": "KKR", "P J Cummins": "SRH", "Prabhsimran Singh": "PBKS",
    "R D Rickelton": "MI", "Shashank Singh": "PBKS", "Vijaykumar Vyshak": "PBKS",
    "Anuj Rawat": "GT", "S N Thakur": "MI", "Prashant Veer": "CSK",
    "S V Samson": "CSK", "Mohammed Shami": "LSG", "Kuldeep Yadav": "DC",
    "B Kumar": "RCB", "L Ngidi": "DC", "Rinku Singh": "KKR",
    "Avesh Khan": "LSG", "R A Jadeja": "CSK", "Sandeep Sharma": "RR",
    "R Parag": "RR", "Q de Kock": "MI", "K R Sen": "RR",
    "Sai Sudharsan": "GT", "P Nissanka": "DC", "L S Livingstone": "SRH",
    "H V Patel": "SRH", "A F Milne": "RR", "A Porel": "DC",
    "S N Khan": "CSK", "Naman Dhir": "MI", "Tilak Varma": "MI",
    "K H Pandya": "RCB", "M P Stoinis": "PBKS", "Priyansh Arya": "PBKS",
    "K L Rahul": "DC", "Ishan Kishan": "SRH", "V Suryavanshi": "RR",
    "D C Jurel": "RR", "R M Patidar": "RCB", "J G Bethell": "RCB",
    "J D Unadkat": "SRH", "Azmatullah Omarzai": "PBKS", "A Badoni": "LSG",
    "Ramandeep Singh": "KKR", "R S Kishore": "GT", "M S Dhoni": "CSK",
    "T A Boult": "MI", "D L Chahar": "MI", "J R Hazlewood": "RCB",
    "Y S Chahal": "PBKS", "Y B K Jaiswal": "RR", "Shahbaz Ahamad": "LSG",
    "Abdul Samad": "LSG", "M Prasidh Krishna": "GT", "M J Henry": "CSK",
    "R R Pant": "LSG", "V G Arora": "KKR", "M Pathirana": "KKR",
    "S Dube": "CSK", "T H David": "RCB", "A J Hosein": "CSK",
    "H H Pandya": "MI", "R Shepherd": "RCB", "M J Owen": "PBKS",
    "Mohammed Siraj": "GT", "M J Santner": "MI", "S O Hetmeyer": "RR",
    "A M Rahane": "KKR", "Harpreet Brar": "PBKS", "M Markande": "MI",
    "Z Ansari": "SRH", "Ayush Mhatre": "CSK", "J C Archer": "RR",
    "A Kumar": "MI", "V Nigam": "DC", "W G Jacks": "MI",
    "C Bosch": "MI", "M Jansen": "PBKS", "H Klaasen": "SRH",
    "R G Sharma": "MI", "K K Nair": "DC", "Shahrukh Khan": "PBKS",
    "R D Gaikwad": "CSK", "S S Iyer": "PBKS", "N Rana": "DC",
    "D Padikkal": "RCB", "G D Phillips": "GT", "T Stubbs": "DC",
    "S E Rutherford": "MI", "Suyash Sharma": "RCB", "R Powell": "KKR",
    "B Muzarabani": "KKR", "B A Carse": "SRH", "A Verma": "SRH",
    "D Brevis": "CSK", "J C Buttler": "GT", "M R Marsh": "LSG",
    "Auqib Nabi": "DC", "J J Bumrah": "MI", "Angkrish Raghuvanshi": "KKR",
    "R Tewatia": "GT", "T U Deshpande": "RR", "Mangesh Yadav": "RCB",
    "K K Ahmed": "CSK", "J O Holder": "GT", "V R Iyer": "RCB",
    "Noor Ahmad": "CSK", "T M Head": "SRH", "Shivang Kumar": "SRH",
    "V Kohli": "RCB", "L H Ferguson": "PBKS", "Ashutosh Sharma": "DC",
    "Shubman Gill": "GT", "J Overton": "CSK", "P P Shaw": "DC",
    "Prince Yadav": "LSG", "Digvesh Rathi": "LSG", "N Wadhera": "PBKS",
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

PLAYER_ALIASES = {
    "Virat Kohli": "V Kohli",
    "Rohit Sharma": "R G Sharma",
    "Jasprit Bumrah": "J J Bumrah",
    "Hardik Pandya": "H H Pandya",
    "Trent Boult": "T A Boult",
    "Suryakumar Yadav": "S A Yadav",
    "Ryan Rickelton": "R D Rickelton",
    "Sunil Narine": "S P Narine",
    "Varun Chakravarthy": "C V Varun",
    "Travis Head": "T M Head",
    "TM Head": "T M Head",
    "Shardul Thakur": "S N Thakur",
    "Nitish Reddy": "N K Reddy",
    "Nithish Kumar Reddy": "N K Reddy",
    "Heinrich Klaasen": "H Klaasen",
    "Harshal Patel": "H V Patel",
    "Jitesh Sharma": "J M Sharma",
    "Mayank Markande": "M Markande",
    "Phil Salt": "P D Salt",
    "Finn Allen": "F H Allen",
    "Cameron Green": "C Green",
    "Blessing Muzarabani": "B Muzarabani",
    "Jaydev Unadkat": "J D Unadkat",
    "Devdutt Padikkal": "D Padikkal",
    "Devdutt  Padikkal": "D Padikkal",
    "Tim David": "T H David",
    "Aniket Verma": "A Verma",
    "Vaibhav Arora": "V G Arora",
    "Ajinkya Rahane": "A M Rahane",
    "Sanju Samson": "S V Samson",
    "Ruturaj Gaikwad": "R D Gaikwad",
    "Yashasvi Jaiswal": "Y B K Jaiswal",
    "YBK Jaiswal": "Y B K Jaiswal",
    "Vaibhav Suryavanshi": "V Suryavanshi",
    "Dhruv Jurel": "D C Jurel",
    "Riyan Parag": "R Parag",
    "Riyan  Parag": "R Parag",
    "Jofra Archer": "J C Archer",
    "JC Archer": "J C Archer",
    "Nandre Burger": "N Burger",
    "Ravindra Jadeja": "R A Jadeja",
    "RA Jadeja": "R A Jadeja",
    "Khaleel Ahmed": "K K Ahmed",
    "Matt Henry": "M J Henry",
    "Jamie Overton": "J Overton",
    "Rajat Patidar": "R M Patidar",
    "Shivam Dube": "S Dube",
    "Sarfaraz Khan": "S N Khan",
    "Jos Buttler": "J C Buttler",
    "Glenn Phillips": "G D Phillips",
    "Rahul Tewatia": "R Tewatia",
    "Marco Jansen": "M Jansen",
    "Yuzvendra Chahal": "Y S Chahal",
    "Shreyas Iyer": "S S Iyer",
    "Nehal Wadhera": "N Wadhera",
    "Marcus Stoinis": "M P Stoinis",
    "Kagiso Rabada": "K Rabada",
    "Prasidh Krishna": "M Prasidh Krishna",
    "Mitchell Marsh": "M R Marsh",
    "Rishabh Pant": "R R Pant",
    "Aiden Markram": "A K Markram",
    "Ayush Badoni": "A Badoni",
    "KL Rahul": "K L Rahul",
    "Nicholas Pooran": "N Pooran",
    "Shahbaz Ahmed": "Shahbaz Ahamad",
    "Axar Patel": "A R Patel",
    "Thangarasu Natarajan": "T Natarajan",
    "Vipraj Nigam": "V Nigam",
    "Pathum Nissanka": "P Nissanka",
    "Nitish Rana": "N Rana",
    "Tristan Stubbs": "T Stubbs",
    "Sherfane Rutherford": "S E Rutherford",
    "Mitchell Santner": "M J Santner",
    "Corbin Bosch": "C Bosch",
    "David Miller": "D A Miller",
    "Deepak Chahar": "D L Chahar",
    "Shimron Hetmeyer": "S O Hetmeyer",
    "Tushar Deshpande": "T U Deshpande",
    "TU Deshpande": "T U Deshpande",
    "Liam Livingstone": "L S Livingstone",
    "Rahul Chahar": "R D Chahar",
    "Venkatesh Iyer": "V R Iyer",
    "Romario Shepherd": "R Shepherd",
    "Krunal Pandya": "K H Pandya",
    "Bhuvneshwar Kumar": "B Kumar",
    "Dewald Brevis": "D Brevis",
    "Akeal Hosein": "A J Hosein",
    "Abhishek Sharma": "Abhishek Sharma",
    "Ishan Kishan": "Ishan Kishan",
    "Shivang Kumar": "Shivang Kumar",
    "Ravi Bishnoi": "Ravi Bishnoi",
    "Sandeep Sharma": "Sandeep Sharma",
    "Sakib Hussain": "Sakib Hussain",
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
    alias_direct = PLAYER_ALIASES.get(str(external_name).strip())
    if alias_direct:
        return alias_direct

    exact_normalized = normalize_text(external_name)
    for owner in state["owners"]:
        for player in owner["players"]:
            if normalize_text(player["player_name"]) == exact_normalized:
                return player["player_name"]

    lookup = get_signature_lookup(state)
    candidates = set()
    for signature in signatures_for_name(external_name):
        candidates.update(lookup.get(signature, set()))
    if len(candidates) == 1:
        return next(iter(candidates))
    if len(candidates) > 1:
        external_tokens = tokenize_name(external_name)
        exact_full = [
            candidate
            for candidate in candidates
            if tokenize_name(candidate) == external_tokens
        ]
        if len(exact_full) == 1:
            return exact_full[0]

        if external_tokens:
            same_last = [
                candidate
                for candidate in candidates
                if tokenize_name(candidate)
                and tokenize_name(candidate)[-1] == external_tokens[-1]
            ]
            if len(same_last) == 1:
                return same_last[0]

            same_first_token = [
                candidate
                for candidate in same_last
                if tokenize_name(candidate)[0] == external_tokens[0]
            ]
            if len(same_first_token) == 1:
                return same_first_token[0]

        return None

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
            if player_tokens[-1] == external_last and player_tokens[0] == external_tokens[0]:
                return player["player_name"]
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
            "owner_slug": slugify(owner["owner_name"]),
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


def slugify(value):
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    return slug or "item"


def owner_slug_map(state):
    return {slugify(owner["owner_name"]): owner for owner in state["owners"]}


def selection_percentage(player_name, state):
    owner_count = len(state["owners"]) or 1
    selected = 0
    for owner in state["owners"]:
        if any(player["player_name"] == player_name for player in owner["players"]):
            selected += 1
    return round((selected / owner_count) * 100, 2)


def player_leaderboard_data(state):
    players = []
    for owner in state["owners"]:
        for player in owner["players"]:
            totals = aggregate_player_totals(player, owner, state)
            players.append(
                {
                    "player_name": player["player_name"],
                    "owner_name": owner["owner_name"],
                    "owner_slug": slugify(owner["owner_name"]),
                    "points": totals["points"],
                    "runs": totals["runs"],
                    "wickets": totals["wickets"],
                    "catches": totals["catches"],
                    "stumpings": totals["stumpings"],
                    "selection_pct": selection_percentage(player["player_name"], state),
                }
            )
    players.sort(key=lambda item: (-item["points"], item["player_name"]))
    return players


def owner_detail_payload(owner, state):
    players = []
    total = 0
    for player in owner["players"]:
        totals = aggregate_player_totals(player, owner, state)
        fifty_plus_count = 0
        seventy_five_plus_count = 0
        hundred_plus_count = 0
        three_wickets_plus_count = 0
        four_wickets_plus_count = 0
        five_wickets_plus_count = 0
        batting_points_total = 0
        bowling_points_total = 0
        fielding_points_total = 0
        final_points_total = 0

        for stats in (player.get("matches") or {}).values():
            runs = int(stats.get("runs", 0))
            wickets = int(stats.get("wickets", 0))
            catches = int(stats.get("catches", 0))
            stumpings = int(stats.get("stumpings", 0))
            if runs >= 50:
                fifty_plus_count += 1
            if runs >= 75:
                seventy_five_plus_count += 1
            if runs >= 100:
                hundred_plus_count += 1
            if wickets >= 3:
                three_wickets_plus_count += 1
            if wickets >= 4:
                four_wickets_plus_count += 1
            if wickets >= 5:
                five_wickets_plus_count += 1
            batting_points_total += calculate_batting_points(runs)
            bowling_points_total += calculate_bowling_points(wickets)
            fielding_points_total += calculate_fielding_points(
                catches=catches,
                stumpings=stumpings,
                is_wicketkeeper=player.get("is_wicketkeeper", False),
            )

        for match_id, stats in (player.get("matches") or {}).items():
            runs = int(stats.get("runs", 0))
            wickets = int(stats.get("wickets", 0))
            catches = int(stats.get("catches", 0))
            stumpings = int(stats.get("stumpings", 0))
            match_captain, match_vice = owner_assignment_for_match(owner, match_id, state)
            final_points_total += calculate_player_points(
                runs=runs,
                wickets=wickets,
                catches=catches,
                stumpings=stumpings,
                is_wicketkeeper=player.get("is_wicketkeeper", False),
                is_captain=player["player_name"] == match_captain,
                is_vice_captain=player["player_name"] == match_vice,
            )

        base_points_total = round(batting_points_total + bowling_points_total + fielding_points_total, 2)
        captain_vice_bonus_points = round(final_points_total - base_points_total, 2)

        total += totals["points"]
        players.append(
            {
                "player_name": player["player_name"],
                "ipl_team": PLAYER_TEAMS.get(player["player_name"], "-"),
                "points": totals["points"],
                "runs": totals["runs"],
                "wickets": totals["wickets"],
                "catches": totals["catches"],
                "stumpings": totals["stumpings"],
                "fifty_plus_count": fifty_plus_count,
                "seventy_five_plus_count": seventy_five_plus_count,
                "hundred_plus_count": hundred_plus_count,
                "three_wickets_plus_count": three_wickets_plus_count,
                "four_wickets_plus_count": four_wickets_plus_count,
                "five_wickets_plus_count": five_wickets_plus_count,
                "batting_points_total": round(batting_points_total, 2),
                "bowling_points_total": round(bowling_points_total, 2),
                "fielding_points_total": round(fielding_points_total, 2),
                "base_points_total": base_points_total,
                "captain_vice_bonus_points": captain_vice_bonus_points,
                "final_points_total": round(final_points_total, 2),
                "is_captain": player["player_name"] == owner["captain"],
                "is_vice_captain": player["player_name"] == owner["vice_captain"],
                "is_wicketkeeper": player.get("is_wicketkeeper", False),
            }
        )
    players.sort(key=lambda item: (-item["points"], item["player_name"]))
    return {
        "owner_name": owner["owner_name"],
        "owner_slug": slugify(owner["owner_name"]),
        "captain": owner["captain"],
        "vice_captain": owner["vice_captain"],
        "captain_change_used": owner.get("captain_change_used", False),
        "captain_change_from_match_id": owner.get("captain_change_from_match_id", ""),
        "changed_captain": owner.get("changed_captain", ""),
        "changed_vice_captain": owner.get("changed_vice_captain", ""),
        "total_points": round(total, 2),
        "players": players,
    }


def search_results(state, query):
    query = query.strip().lower()
    if len(query) < 2:
        return []
    results = []
    for row in player_leaderboard_data(state):
        if query in row["player_name"].lower():
            results.append(row)
    return results[:12]


def analytics_payload(state):
    ordered_matches = ordered_match_catalog(state)
    match_labels = []
    for match in ordered_matches:
        label = f"M{match.get('match_number')}" if match.get("match_number") else match.get("id")
        match_labels.append(label)

    owner_progression = []
    for owner in state["owners"]:
        running = 0
        points_line = []
        for match in ordered_matches:
            match_points = 0
            for player in owner["players"]:
                stats = (player.get("matches") or {}).get(match["id"])
                if not stats:
                    continue
                current_captain, current_vice = owner_assignment_for_match(owner, match["id"], state)
                match_points += calculate_player_points(
                    runs=int(stats.get("runs", 0)),
                    wickets=int(stats.get("wickets", 0)),
                    catches=int(stats.get("catches", 0)),
                    stumpings=int(stats.get("stumpings", 0)),
                    is_wicketkeeper=player.get("is_wicketkeeper", False),
                    is_captain=player["player_name"] == current_captain,
                    is_vice_captain=player["player_name"] == current_vice,
                )
            running += round(match_points, 2)
            points_line.append(round(running, 2))
        owner_progression.append({"name": owner["owner_name"], "points": points_line})

    best_match_rows = []
    consistent_rows = []
    for owner in state["owners"]:
        for player in owner["players"]:
            consistent_count = 0
            total_matches = 0
            for match_id, stats in (player.get("matches") or {}).items():
                current_captain, current_vice = owner_assignment_for_match(owner, match_id, state)
                match_points = calculate_player_points(
                    runs=int(stats.get("runs", 0)),
                    wickets=int(stats.get("wickets", 0)),
                    catches=int(stats.get("catches", 0)),
                    stumpings=int(stats.get("stumpings", 0)),
                    is_wicketkeeper=player.get("is_wicketkeeper", False),
                    is_captain=player["player_name"] == current_captain,
                    is_vice_captain=player["player_name"] == current_vice,
                )
                bare_points = calculate_player_points(
                    runs=int(stats.get("runs", 0)),
                    wickets=int(stats.get("wickets", 0)),
                    catches=int(stats.get("catches", 0)),
                    stumpings=int(stats.get("stumpings", 0)),
                    is_wicketkeeper=player.get("is_wicketkeeper", False),
                    is_captain=False,
                    is_vice_captain=False,
                )
                best_match_rows.append(
                    {
                        "player_name": player["player_name"],
                        "owner_name": owner["owner_name"],
                        "match_code": match_id,
                        "points": round(match_points, 2),
                    }
                )
                total_matches += 1
                if bare_points >= 25:
                    consistent_count += 1
            if total_matches:
                consistent_rows.append(
                    {
                        "player_name": player["player_name"],
                        "owner_name": owner["owner_name"],
                        "consistent_matches": consistent_count,
                        "total_matches": total_matches,
                    }
                )

    best_match_rows.sort(key=lambda item: (-item["points"], item["player_name"]))
    consistent_rows.sort(
        key=lambda item: (-item["consistent_matches"], -item["total_matches"], item["player_name"])
    )
    return {
        "match_labels": match_labels,
        "owner_progression": owner_progression,
        "best_match": best_match_rows[:20],
        "consistent": consistent_rows[:20],
    }


def public_state_payload():
    state = maybe_refresh_state()
    owners = []
    for owner in state["owners"]:
        owner_total = 0
        for player in owner["players"]:
            owner_total += aggregate_player_totals(player, owner, state)["points"]
        owners.append({"owner_name": owner["owner_name"], "owner_slug": slugify(owner["owner_name"]), "total_points": round(owner_total, 2)})
    owners.sort(key=lambda item: (-item["total_points"], item["owner_name"]))
    return {
        "owners": owners,
        "live_matches": state.get("live_matches", []),
        "last_sync_message": state.get("last_sync_message"),
        "last_sync_at": state.get("last_sync_at"),
    }


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


def clear_all_match_stats(state):
    for owner in state["owners"]:
        for player in owner["players"]:
            player["matches"] = {}
    state["live_matches"] = []
    state["match_catalog"] = {}
    state["historical_backfill_at"] = None
    state["last_live_sync_at"] = None
    state["last_sync_at"] = None


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
    if force or state.get("mapping_version") != 2:
        clear_all_match_stats(state)
        state["mapping_version"] = 2
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
              <div class="name"><a href="/owner/${owner.owner_slug}" style="color:inherit;text-decoration:none;">${owner.owner_name}</a></div>
              <div class="tiny">Tap owner name to view squad and player points</div>
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


PUBLIC_OWNER_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Owner Squad</title>
  <style>
    body { margin:0; background:linear-gradient(160deg,#08131f,#10243a); color:#f6f7fb; font-family:"Segoe UI",sans-serif; padding:16px; }
    .shell { width:min(860px,100%); margin:0 auto; }
    .top { display:flex; gap:12px; align-items:center; margin-bottom:16px; flex-wrap:wrap; }
    a.back { color:#08131f; text-decoration:none; background:#ffb703; padding:10px 14px; border-radius:14px; font-weight:700; }
    h1 { margin:0; font-size:32px; }
    .hero, .row { background:rgba(9,24,40,.84); border:1px solid rgba(255,255,255,.08); border-radius:18px; }
    .hero { padding:18px; margin-bottom:16px; }
    .row { display:flex; justify-content:space-between; gap:12px; padding:12px 14px; margin-bottom:10px; }
    .badge { display:inline-block; padding:3px 8px; border-radius:999px; font-size:11px; font-weight:700; margin-right:6px; background:#ffb703; color:#08131f; }
    .badge.vc { background:#d1d5db; }
    .badge.team { background:#34d399; }
    .pts { color:#34d399; font-weight:800; }
  </style>
</head>
<body>
  <div class="shell">
    <div class="top">
      <a class="back" href="/">Back</a>
      <h1 id="ownerName">Owner</h1>
    </div>
    <div class="hero">
      <div id="summary"></div>
    </div>
    <div id="players"></div>
  </div>
  <script>
    async function loadOwner() {
      const slug = location.pathname.split('/').pop();
      const response = await fetch(`/api/owner/${slug}`);
      if (response.status === 404) return document.body.innerHTML = '<div style="padding:20px;color:#fff;">Owner not found.</div>';
      const owner = await response.json();
      document.getElementById('ownerName').textContent = owner.owner_name;
      document.getElementById('summary').innerHTML = `<div style="font-size:22px;font-weight:700;">${owner.total_points} pts</div><div style="color:#9cb3c9;margin-top:6px;">Captain: ${owner.captain} | Vice-captain: ${owner.vice_captain}</div>`;
      document.getElementById('players').innerHTML = owner.players.map(player => `
        <div class="row">
          <div>
            <div style="font-weight:700">${player.player_name}</div>
            <div style="color:#9cb3c9;font-size:12px;margin-top:4px;">
              <span class="badge team">${player.ipl_team}</span>
              ${player.is_captain ? '<span class="badge">C</span>' : ''}
              ${player.is_vice_captain ? '<span class="badge vc">VC</span>' : ''}
              ${player.is_wicketkeeper ? '<span class="badge vc">WK</span>' : ''}
            </div>
            <div style="color:#9cb3c9;font-size:12px;margin-top:6px;">Runs ${player.runs} | 50+ ${player.fifty_plus_count} | 75+ ${player.seventy_five_plus_count} | 100+ ${player.hundred_plus_count}</div>
            <div style="color:#9cb3c9;font-size:12px;margin-top:4px;">Wkts ${player.wickets} | 3+W ${player.three_wickets_plus_count} | 4+W ${player.four_wickets_plus_count} | 5+W ${player.five_wickets_plus_count}</div>
            <div style="color:#9cb3c9;font-size:12px;margin-top:4px;">Catches ${player.catches} | Stumpings ${player.stumpings}</div>
            <div style="color:#9cb3c9;font-size:12px;margin-top:4px;">Points: Bat ${player.batting_points_total} + Bowl ${player.bowling_points_total} + Field ${player.fielding_points_total} + C/VC ${player.captain_vice_bonus_points} = ${player.final_points_total}</div>
          </div>
          <div class="pts">${player.points}</div>
        </div>
      `).join('');
    }
    loadOwner();
  </script>
</body>
</html>
"""


ADMIN_LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Admin Login</title>
  <style>
    body { margin:0; min-height:100vh; display:grid; place-items:center; background:linear-gradient(160deg,#08131f,#10243a); color:#f6f7fb; font-family:"Segoe UI",sans-serif; }
    .box { width:min(360px,calc(100% - 28px)); background:rgba(9,24,40,.9); border:1px solid rgba(255,255,255,.1); border-radius:22px; padding:26px; }
    h1 { margin:0 0 10px; font-size:28px; }
    p { color:#9cb3c9; line-height:1.5; }
    input, button { width:100%; box-sizing:border-box; border-radius:14px; border:1px solid rgba(255,255,255,.12); padding:12px 14px; font-size:15px; }
    input { background:#fff; color:#08131f; margin:14px 0 10px; }
    button { background:linear-gradient(180deg, rgba(255,183,3,.24), rgba(255,183,3,.08)); color:#f6f7fb; cursor:pointer; }
    .error { color:#fb7185; min-height:22px; margin-top:8px; }
  </style>
</head>
<body>
  <form class="box" method="POST" action="/admin/login">
    <h1>Admin Login</h1>
    <p>Only the public leaderboard is open to everyone. Use this page for team controls, player views, and analytics.</p>
    <input type="password" name="password" placeholder="Enter admin password" autofocus>
    <button type="submit">Enter Admin</button>
    <div class="error">{{ERROR}}</div>
  </form>
</body>
</html>
"""


ADMIN_PLAYERS_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Player Leaderboard</title>
  <style>
    body { margin:0; background:linear-gradient(160deg,#08131f,#10243a); color:#f6f7fb; font-family:"Segoe UI",sans-serif; padding:16px; }
    .shell { width:min(980px,100%); margin:0 auto; }
    .top { display:flex; gap:12px; align-items:center; margin-bottom:16px; flex-wrap:wrap; }
    a { color:#08131f; text-decoration:none; background:#ffb703; padding:10px 14px; border-radius:14px; font-weight:700; }
    h1 { margin:0; font-size:32px; }
    .sub { color:#9cb3c9; margin:6px 0 16px; }
    .row { display:grid; grid-template-columns: 48px 1.3fr .9fr .7fr .7fr; gap:10px; align-items:center; padding:12px 14px; border-radius:16px; background:rgba(9,24,40,.84); border:1px solid rgba(255,255,255,.08); margin-bottom:10px; }
    .head { color:#9cb3c9; text-transform:uppercase; font-size:12px; letter-spacing:.08em; }
    .pts { color:#34d399; font-weight:800; }
    @media (max-width:700px) { .row { grid-template-columns: 42px 1fr; } .head { display:none; } .hide-m { display:none; } }
  </style>
</head>
<body>
  <div class="shell">
    <div class="top">
      <a href="/admin">Back</a>
      <h1>Player Leaderboard</h1>
    </div>
    <div class="sub">Top fantasy performers across all owners.</div>
    <div class="row head"><div>#</div><div>Player</div><div class="hide-m">Owner</div><div class="hide-m">Selected</div><div>Points</div></div>
    <div id="rows"></div>
  </div>
  <script>
    async function loadPlayers() {
      const response = await fetch('/api/admin/players');
      if (response.status === 401) return location.href = '/admin/login';
      const players = await response.json();
      document.getElementById('rows').innerHTML = players.map((p, i) => `
        <div class="row">
          <div>${i + 1}</div>
          <div><div style="font-weight:700">${p.player_name}</div><div style="color:#9cb3c9;font-size:12px;">Runs ${p.runs} | Wkts ${p.wickets}</div></div>
          <div class="hide-m"><a href="/admin/owner/${p.owner_slug}" style="background:none;color:#f6f7fb;padding:0;">${p.owner_name}</a></div>
          <div class="hide-m">${p.selection_pct}%</div>
          <div class="pts">${p.points}</div>
        </div>
      `).join('');
    }
    loadPlayers();
  </script>
</body>
</html>
"""


ADMIN_OWNER_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Owner Squad</title>
  <style>
    body { margin:0; background:linear-gradient(160deg,#08131f,#10243a); color:#f6f7fb; font-family:"Segoe UI",sans-serif; padding:16px; }
    .shell { width:min(860px,100%); margin:0 auto; }
    .top { display:flex; gap:12px; align-items:center; margin-bottom:16px; flex-wrap:wrap; }
    a { color:#08131f; text-decoration:none; background:#ffb703; padding:10px 14px; border-radius:14px; font-weight:700; }
    h1 { margin:0; font-size:32px; }
    .hero, .row { background:rgba(9,24,40,.84); border:1px solid rgba(255,255,255,.08); border-radius:18px; }
    .hero { padding:18px; margin-bottom:16px; }
    .row { display:flex; justify-content:space-between; gap:12px; padding:12px 14px; margin-bottom:10px; }
    .badge { display:inline-block; padding:3px 8px; border-radius:999px; font-size:11px; font-weight:700; margin-right:6px; background:#ffb703; color:#08131f; }
    .badge.vc { background:#d1d5db; }
    .pts { color:#34d399; font-weight:800; }
  </style>
</head>
<body>
  <div class="shell">
    <div class="top">
      <a href="/admin">Back</a>
      <h1 id="ownerName">Owner</h1>
    </div>
    <div class="hero">
      <div id="summary"></div>
    </div>
    <div id="players"></div>
  </div>
  <script>
    async function loadOwner() {
      const slug = location.pathname.split('/').pop();
      const response = await fetch(`/api/admin/owner/${slug}`);
      if (response.status === 401) return location.href = '/admin/login';
      if (response.status === 404) return document.body.innerHTML = '<div style="padding:20px;color:#fff;">Owner not found.</div>';
      const owner = await response.json();
      document.getElementById('ownerName').textContent = owner.owner_name;
      document.getElementById('summary').innerHTML = `<div style="font-size:22px;font-weight:700;">${owner.total_points} pts</div><div style="color:#9cb3c9;margin-top:6px;">Captain: ${owner.captain} | Vice-captain: ${owner.vice_captain}</div>`;
      document.getElementById('players').innerHTML = owner.players.map(player => `
        <div class="row">
          <div>
            <div style="font-weight:700">${player.player_name}</div>
            <div style="color:#9cb3c9;font-size:12px;margin-top:4px;">
              ${player.is_captain ? '<span class="badge">C</span>' : ''}
              ${player.is_vice_captain ? '<span class="badge vc">VC</span>' : ''}
              ${player.is_wicketkeeper ? '<span class="badge vc">WK</span>' : ''}
              Runs ${player.runs} | 50+ ${player.fifty_plus_count} | 75+ ${player.seventy_five_plus_count} | 100+ ${player.hundred_plus_count}<br>
              Wkts ${player.wickets} | 3+W ${player.three_wickets_plus_count} | 4+W ${player.four_wickets_plus_count} | 5+W ${player.five_wickets_plus_count}<br>
              C ${player.catches} | St ${player.stumpings}<br>
              Points: Bat ${player.batting_points_total} + Bowl ${player.bowling_points_total} + Field ${player.fielding_points_total} + C/VC ${player.captain_vice_bonus_points} = ${player.final_points_total}
            </div>
          </div>
          <div class="pts">${player.points}</div>
        </div>
      `).join('');
    }
    loadOwner();
  </script>
</body>
</html>
"""


ADMIN_ANALYTICS_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Analytics</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
  <style>
    body { margin:0; background:linear-gradient(160deg,#08131f,#10243a); color:#f6f7fb; font-family:"Segoe UI",sans-serif; padding:16px; }
    .shell { width:min(1100px,100%); margin:0 auto; }
    .top { display:flex; gap:12px; align-items:center; margin-bottom:16px; flex-wrap:wrap; }
    a { color:#08131f; text-decoration:none; background:#ffb703; padding:10px 14px; border-radius:14px; font-weight:700; }
    h1 { margin:0; font-size:32px; }
    .panel { background:rgba(9,24,40,.84); border:1px solid rgba(255,255,255,.08); border-radius:18px; padding:16px; margin-bottom:16px; }
    .table-row { display:grid; grid-template-columns: 52px 1.2fr 1fr .8fr .8fr; gap:10px; padding:10px 0; border-bottom:1px solid rgba(255,255,255,.06); }
    .table-row:last-child { border-bottom:0; }
    .head { color:#9cb3c9; text-transform:uppercase; font-size:12px; letter-spacing:.08em; }
    @media (max-width:760px) { .table-row { grid-template-columns: 44px 1fr; } .hide-m { display:none; } }
  </style>
</head>
<body>
  <div class="shell">
    <div class="top">
      <a href="/admin">Back</a>
      <h1>Analytics</h1>
    </div>
    <div class="panel">
      <div style="font-size:20px;font-weight:700;margin-bottom:12px;">Owner Progression</div>
      <canvas id="progressionChart" height="180"></canvas>
    </div>
    <div class="panel">
      <div style="font-size:20px;font-weight:700;margin-bottom:10px;">Best Single-Match Scores</div>
      <div class="table-row head"><div>#</div><div>Player</div><div class="hide-m">Owner</div><div class="hide-m">Match</div><div>Points</div></div>
      <div id="bestMatch"></div>
    </div>
    <div class="panel">
      <div style="font-size:20px;font-weight:700;margin-bottom:10px;">Most Consistent Players</div>
      <div class="table-row head"><div>#</div><div>Player</div><div class="hide-m">Owner</div><div class="hide-m">25+ Matches</div><div>Total</div></div>
      <div id="consistent"></div>
    </div>
  </div>
  <script>
    async function loadAnalytics() {
      const response = await fetch('/api/admin/analytics');
      if (response.status === 401) return location.href = '/admin/login';
      const data = await response.json();
      const colors = ['#00d4ff','#ff6384','#36a2eb','#ffce56','#4bc0c0','#9966ff','#ff9f40','#7cfc00','#ff4500','#da70d6','#34d399','#d1d5db'];
      new Chart(document.getElementById('progressionChart'), {
        type: 'line',
        data: {
          labels: data.match_labels,
          datasets: data.owner_progression.map((owner, index) => ({
            label: owner.name,
            data: owner.points,
            borderColor: colors[index % colors.length],
            tension: 0.25,
            pointRadius: 2,
            borderWidth: 2
          }))
        },
        options: {
          responsive: true,
          plugins: { legend: { labels: { color: '#f6f7fb' } } },
          scales: {
            x: { ticks: { color: '#9cb3c9' }, grid: { color: 'rgba(255,255,255,.08)' } },
            y: { ticks: { color: '#9cb3c9' }, grid: { color: 'rgba(255,255,255,.08)' } }
          }
        }
      });
      document.getElementById('bestMatch').innerHTML = data.best_match.map((row, i) => `
        <div class="table-row"><div>${i + 1}</div><div>${row.player_name}</div><div class="hide-m">${row.owner_name}</div><div class="hide-m">${row.match_code}</div><div>${row.points}</div></div>
      `).join('');
      document.getElementById('consistent').innerHTML = data.consistent.map((row, i) => `
        <div class="table-row"><div>${i + 1}</div><div>${row.player_name}</div><div class="hide-m">${row.owner_name}</div><div class="hide-m">${row.consistent_matches}</div><div>${row.total_matches}</div></div>
      `).join('');
    }
    loadAnalytics();
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
    .nav-links { display:flex; flex-wrap:wrap; gap:10px; margin-top:14px; }
    .nav-links a { color: var(--text); text-decoration:none; padding: 10px 14px; border-radius: 14px; background: rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.10); }
    .search-results { margin-top:10px; display:grid; gap:8px; }
    .search-item { border:1px solid rgba(255,255,255,0.06); border-radius:14px; background: rgba(255,255,255,0.04); padding:10px 12px; }
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
      <div class="nav-links">
        <a href="/admin/players">Player Leaderboard</a>
        <a href="/admin/analytics">Analytics</a>
        <a href="/" target="_blank" rel="noreferrer">Open Public Leaderboard</a>
        <a href="/admin/logout">Logout</a>
      </div>
    </section>

    <div class="grid">
      <section class="panel">
        <h2>Admin Tools</h2>
        <div class="sub">The app backfills old IPL 2026 matches automatically from Cricsheet and refreshes live scorecards automatically from Cricbuzz. Use this page only when you want to force a refresh or apply one-time captain changes.</div>
        <div class="toolbar">
          <input id="playerSearch" placeholder="Search player..." oninput="searchPlayers()">
          <button onclick="forceRefresh()">Force Refresh Now</button>
          <button class="ghost" onclick="refreshState()">Refresh Screen</button>
        </div>
        <div class="warning">No API key is needed. Automatic updates run in the background whenever people open the site, and the public leaderboard refreshes itself every 15 seconds.</div>
        <div id="searchResults" class="search-results"></div>
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
              <div class="small"><a href="/admin/owner/${owner.owner_slug}" style="color:#ffb703;text-decoration:none;">Open owner page</a></div>
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
      const response = await fetch("/api/admin/state");
      if (response.status === 401) return location.href = "/admin/login";
      await draw(await response.json());
    }
    async function searchPlayers() {
      const query = document.getElementById("playerSearch").value.trim();
      const box = document.getElementById("searchResults");
      if (query.length < 2) {
        box.innerHTML = "";
        return;
      }
      const response = await fetch(`/api/admin/search?q=${encodeURIComponent(query)}`);
      if (response.status === 401) return location.href = "/admin/login";
      const results = await response.json();
      box.innerHTML = results.length ? results.map(player => `
        <div class="search-item">
          <div style="font-weight:700;">${player.player_name}</div>
          <div class="small">Owner: <a href="/admin/owner/${player.owner_slug}" style="color:#ffb703;text-decoration:none;">${player.owner_name}</a> | Points: ${player.points} | Selection: ${player.selection_pct}%</div>
        </div>
      `).join("") : `<div class="small">No player found.</div>`;
    }
    async function forceRefresh() {
      setMessage("Refreshing historical and live data...");
      const response = await fetch("/api/refresh-now", { method: "POST" });
      if (response.status === 401) return location.href = "/admin/login";
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
      if (response.status === 401) return location.href = "/admin/login";
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
    def json_response(self, payload, status=200, headers=None):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(encoded)

    def html_response(self, payload, status=200, headers=None):
        encoded = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(encoded)

    def redirect_response(self, location, headers=None):
        self.send_response(302)
        self.send_header("Location", location)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()

    def read_json(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else b"{}"
        content_type = self.headers.get("Content-Type", "")
        decoded = body.decode("utf-8")
        if "application/json" in content_type:
            return json.loads(decoded or "{}")
        return {key: values[0] for key, values in urllib.parse.parse_qs(decoded).items()}

    def is_admin_authenticated(self):
        raw = self.headers.get("Cookie", "")
        if not raw:
            return False
        jar = cookies.SimpleCookie()
        jar.load(raw)
        return jar.get(ADMIN_COOKIE_NAME) is not None and jar[ADMIN_COOKIE_NAME].value == ADMIN_COOKIE_VALUE

    def require_admin(self):
        if self.is_admin_authenticated():
            return True
        if self.path.startswith("/api/"):
            self.json_response({"error": "Authentication required."}, status=401)
        else:
            self.redirect_response("/admin/login")
        return False

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self.html_response(LEADERBOARD_HTML)
            return
        if path.startswith("/owner/"):
            self.html_response(PUBLIC_OWNER_HTML)
            return
        if path == "/admin/login":
            self.html_response(ADMIN_LOGIN_HTML.replace("{{ERROR}}", ""))
            return
        if path == "/admin/logout":
            self.redirect_response(
                "/admin/login",
                headers={"Set-Cookie": f"{ADMIN_COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"},
            )
            return
        if path == "/admin":
            if not self.require_admin():
                return
            self.html_response(HTML)
            return
        if path == "/admin/players":
            if not self.require_admin():
                return
            self.html_response(ADMIN_PLAYERS_HTML)
            return
        if path == "/admin/analytics":
            if not self.require_admin():
                return
            self.html_response(ADMIN_ANALYTICS_HTML)
            return
        if path.startswith("/admin/owner/"):
            if not self.require_admin():
                return
            self.html_response(ADMIN_OWNER_HTML)
            return
        if path == "/api/state":
            self.json_response(public_state_payload())
            return
        if path.startswith("/api/owner/"):
            slug = path.split("/api/owner/", 1)[1]
            state = maybe_refresh_state()
            owner = owner_slug_map(state).get(slug)
            if not owner:
                self.json_response({"error": "Owner not found."}, status=404)
                return
            self.json_response(owner_detail_payload(owner, state))
            return
        if path == "/api/admin/state":
            if not self.require_admin():
                return
            self.json_response(leaderboard_state())
            return
        if path == "/api/admin/players":
            if not self.require_admin():
                return
            state = maybe_refresh_state()
            self.json_response(player_leaderboard_data(state))
            return
        if path == "/api/admin/analytics":
            if not self.require_admin():
                return
            state = maybe_refresh_state()
            self.json_response(analytics_payload(state))
            return
        if path.startswith("/api/admin/owner/"):
            if not self.require_admin():
                return
            slug = path.split("/api/admin/owner/", 1)[1]
            state = maybe_refresh_state()
            owner = owner_slug_map(state).get(slug)
            if not owner:
                self.json_response({"error": "Owner not found."}, status=404)
                return
            self.json_response(owner_detail_payload(owner, state))
            return
        if path == "/api/admin/search":
            if not self.require_admin():
                return
            state = maybe_refresh_state()
            query = urllib.parse.parse_qs(urlparse(self.path).query).get("q", [""])[0]
            self.json_response(search_results(state, query))
            return
        self.send_error(404, "Not Found")

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/admin/login":
                payload = self.read_json()
                if str(payload.get("password", "")) == ADMIN_PASSWORD:
                    self.redirect_response(
                        "/admin",
                        headers={"Set-Cookie": f"{ADMIN_COOKIE_NAME}={ADMIN_COOKIE_VALUE}; Path=/; HttpOnly; SameSite=Lax"},
                    )
                else:
                    self.html_response(ADMIN_LOGIN_HTML.replace("{{ERROR}}", "Wrong password."), status=401)
                return

            if path == "/api/refresh-now":
                if not self.require_admin():
                    return
                maybe_refresh_state(force=True)
                self.json_response(leaderboard_state())
                return

            if path == "/api/captain-change":
                if not self.require_admin():
                    return
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
