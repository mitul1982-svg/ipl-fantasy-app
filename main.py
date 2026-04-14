import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "fantasy_data.json")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8000"))

POINT_RULES = {
    "run": 1,
    "wicket": 25,
    "four": 1,
    "six": 2,
    "maiden_over": 12,
    "catch": 8,
    "stumping": 12,
    "run_out_direct": 12,
    "run_out_indirect": 6,
    "duck": -2,
    "thirty_bonus": 4,
    "fifty_bonus": 8,
    "hundred_bonus": 16,
    "three_wicket_bonus": 4,
    "four_wicket_bonus": 8,
    "five_wicket_bonus": 16,
    "lbw_bowled_bonus": 8,
}


def default_player(player_name):
    return {
        "player_name": player_name,
        "runs": 0,
        "wickets": 0,
        "fours": 0,
        "sixes": 0,
        "balls_faced": 0,
        "balls_bowled": 0,
        "runs_conceded": 0,
        "maiden_overs": 0,
        "catches": 0,
        "stumpings": 0,
        "run_out_direct": 0,
        "run_out_indirect": 0,
        "lbw_bowled_wickets": 0,
    }


def build_demo_data():
    owners = []
    for owner_index in range(1, 13):
        owners.append(
            {
                "owner_name": f"Owner {owner_index}",
                "team_name": f"Fantasy XI {owner_index}",
                "players": [
                    default_player(f"Player {owner_index}-{player_index}")
                    for player_index in range(1, 13)
                ],
            }
        )
    return {"owners": owners}


def load_data():
    if not os.path.exists(DATA_FILE):
        data = build_demo_data()
        save_data(data)
        return data
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def strike_rate_bonus(runs, balls_faced):
    if balls_faced < 10:
        return 0
    strike_rate = (runs / balls_faced) * 100 if balls_faced else 0
    if strike_rate >= 170:
        return 6
    if strike_rate >= 150:
        return 4
    if strike_rate >= 130:
        return 2
    if strike_rate < 50:
        return -6
    if strike_rate < 60:
        return -4
    if strike_rate < 70:
        return -2
    return 0


def economy_bonus(runs_conceded, balls_bowled):
    if balls_bowled < 12:
        return 0
    overs = balls_bowled / 6
    economy = runs_conceded / overs if overs else 0
    if economy <= 4:
        return 6
    if economy <= 5:
        return 4
    if economy <= 6:
        return 2
    if economy >= 12:
        return -6
    if economy >= 10:
        return -4
    if economy >= 8:
        return -2
    return 0


def batting_bonus(runs):
    if runs >= 100:
        return POINT_RULES["hundred_bonus"]
    if runs >= 50:
        return POINT_RULES["fifty_bonus"]
    if runs >= 30:
        return POINT_RULES["thirty_bonus"]
    return 0


def bowling_bonus(wickets):
    if wickets >= 5:
        return POINT_RULES["five_wicket_bonus"]
    if wickets >= 4:
        return POINT_RULES["four_wicket_bonus"]
    if wickets >= 3:
        return POINT_RULES["three_wicket_bonus"]
    return 0


def calculate_player_points(player):
    runs = as_int(player.get("runs"))
    wickets = as_int(player.get("wickets"))
    fours = as_int(player.get("fours"))
    sixes = as_int(player.get("sixes"))
    balls_faced = as_int(player.get("balls_faced"))
    balls_bowled = as_int(player.get("balls_bowled"))
    runs_conceded = as_int(player.get("runs_conceded"))
    maiden_overs = as_int(player.get("maiden_overs"))
    catches = as_int(player.get("catches"))
    stumpings = as_int(player.get("stumpings"))
    run_out_direct = as_int(player.get("run_out_direct"))
    run_out_indirect = as_int(player.get("run_out_indirect"))
    lbw_bowled_wickets = as_int(player.get("lbw_bowled_wickets"))

    total = 0
    total += runs * POINT_RULES["run"]
    total += wickets * POINT_RULES["wicket"]
    total += fours * POINT_RULES["four"]
    total += sixes * POINT_RULES["six"]
    total += maiden_overs * POINT_RULES["maiden_over"]
    total += catches * POINT_RULES["catch"]
    total += stumpings * POINT_RULES["stumping"]
    total += run_out_direct * POINT_RULES["run_out_direct"]
    total += run_out_indirect * POINT_RULES["run_out_indirect"]
    total += lbw_bowled_wickets * POINT_RULES["lbw_bowled_bonus"]
    total += batting_bonus(runs)
    total += bowling_bonus(wickets)
    total += strike_rate_bonus(runs, balls_faced)
    total += economy_bonus(runs_conceded, balls_bowled)

    if runs == 0 and balls_faced > 0:
        total += POINT_RULES["duck"]

    return total


def owner_total(owner):
    return sum(calculate_player_points(player) for player in owner["players"])


def leaderboard_state():
    data = load_data()
    owners = []
    for owner in data["owners"]:
        players = []
        for player in owner["players"]:
            player_copy = dict(player)
            player_copy["points"] = calculate_player_points(player)
            players.append(player_copy)
        owner_copy = {
            "owner_name": owner["owner_name"],
            "team_name": owner.get("team_name", owner["owner_name"]),
            "players": players,
        }
        owner_copy["total_points"] = sum(player["points"] for player in players)
        owners.append(owner_copy)
    owners.sort(key=lambda owner: owner["total_points"], reverse=True)
    return {"owners": owners, "point_rules": POINT_RULES}


def normalize_payload(payload):
    owners = []
    for owner in payload.get("owners", []):
        owner_name = (owner.get("owner_name") or "").strip()
        if not owner_name:
            continue
        players = []
        for incoming_player in owner.get("players", []):
            player_name = (incoming_player.get("player_name") or "").strip()
            if not player_name:
                continue
            merged = default_player(player_name)
            merged.update(incoming_player)
            merged["player_name"] = player_name
            players.append(merged)
        owners.append(
            {
                "owner_name": owner_name,
                "team_name": (owner.get("team_name") or owner_name).strip(),
                "players": players[:12],
            }
        )
    if owners:
        save_data({"owners": owners})
    return leaderboard_state()


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>IPL 2026 Fantasy League</title>
  <style>
    :root {
      --bg: #08131f;
      --panel: rgba(9, 24, 40, 0.82);
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
    .shell {
      width: min(1400px, calc(100% - 32px));
      margin: 24px auto 48px;
    }
    .hero {
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
      backdrop-filter: blur(18px);
      box-shadow: var(--shadow);
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(30px, 4vw, 54px);
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .hero p {
      margin: 12px 0 0;
      color: var(--muted);
      max-width: 900px;
      line-height: 1.6;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 22px;
      margin-top: 22px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px;
      backdrop-filter: blur(18px);
      box-shadow: var(--shadow);
    }
    .panel h2 {
      margin: 0 0 8px;
      font-size: 22px;
    }
    .sub {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
      margin-bottom: 16px;
    }
    .leaderboard-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 14px 16px;
      background: var(--panel-2);
      border-radius: 18px;
      border: 1px solid rgba(255,255,255,0.06);
      margin-bottom: 12px;
    }
    .rank {
      width: 42px;
      height: 42px;
      border-radius: 14px;
      display: grid;
      place-items: center;
      background: linear-gradient(180deg, rgba(255,183,3,0.22), rgba(255,183,3,0.06));
      color: var(--accent);
      font-weight: 700;
    }
    .points {
      font-size: 24px;
      color: var(--accent-2);
      font-weight: 700;
      white-space: nowrap;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 18px;
    }
    button, select, input, textarea {
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 14px;
      background: rgba(255,255,255,0.06);
      color: var(--text);
      padding: 12px 14px;
      font-size: 14px;
    }
    button {
      cursor: pointer;
      background: linear-gradient(180deg, rgba(255,183,3,0.24), rgba(255,183,3,0.08));
    }
    button:hover {
      filter: brightness(1.08);
    }
    .ghost {
      background: rgba(255,255,255,0.05);
    }
    .owner-editor {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
    }
    .card {
      background: var(--panel-2);
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 18px;
      padding: 16px;
    }
    .card textarea {
      width: 100%;
      min-height: 220px;
      resize: vertical;
      margin-top: 10px;
      font-family: Consolas, monospace;
    }
    .score-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }
    .score-grid label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }
    .score-grid input {
      width: 100%;
    }
    .status {
      margin-top: 12px;
      min-height: 24px;
      color: var(--accent-2);
      font-size: 14px;
    }
    .rules {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }
    .rule {
      padding: 12px;
      border-radius: 16px;
      background: rgba(255,255,255,0.05);
    }
    .live-tag {
      display: inline-block;
      margin-top: 14px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(52, 211, 153, 0.12);
      color: var(--accent-2);
      font-size: 13px;
    }
    .players {
      margin-top: 18px;
      display: grid;
      gap: 12px;
    }
    .player-line {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.04);
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .score-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>IPL 2026 Fantasy Cricket</h1>
      <p>Create 12 owner squads, enter live match stats, and watch the leaderboard refresh automatically every 5 seconds. Replace the demo names with your friends and real players anytime.</p>
      <div class="live-tag">Live tracking is ON</div>
    </section>

    <div class="grid">
      <section class="panel">
        <h2>Leaderboard</h2>
        <div class="sub">Sorted by total fantasy points across each owner's 12-player squad.</div>
        <div id="leaderboard"></div>
      </section>

      <section class="panel">
        <h2>Scoring Rules</h2>
        <div class="sub">Core rules are built in. You can tweak the Python dictionary later if your league wants different values.</div>
        <div id="rules" class="rules"></div>
      </section>
    </div>

    <div class="grid">
      <section class="panel">
        <h2>Step 1: Owners And Players</h2>
        <div class="sub">Each card below expects one owner and exactly 12 players, one per line. The app starts with demo data so you can test everything immediately.</div>
        <div class="toolbar">
          <button onclick="loadOwnersIntoEditor()">Load Current Teams</button>
          <button class="ghost" onclick="resetDemoData()">Reset Demo Teams</button>
          <button onclick="saveOwners()">Save Owners And Players</button>
        </div>
        <div id="ownerEditor" class="owner-editor"></div>
        <div id="ownerStatus" class="status"></div>
      </section>

      <section class="panel">
        <h2>Step 2: Update Live Player Stats</h2>
        <div class="sub">Pick an owner, pick a player, enter match stats, then save. Leaderboard refreshes automatically for everyone viewing the page.</div>
        <div class="toolbar">
          <select id="ownerSelect" onchange="populatePlayers()"></select>
          <select id="playerSelect"></select>
          <button onclick="fillSelectedPlayer()">Load Player Stats</button>
          <button onclick="savePlayerStats()">Save Player Stats</button>
        </div>
        <div class="score-grid">
          <div><label>Runs</label><input id="runs" type="number" min="0" value="0"></div>
          <div><label>Wickets</label><input id="wickets" type="number" min="0" value="0"></div>
          <div><label>Fours</label><input id="fours" type="number" min="0" value="0"></div>
          <div><label>Sixes</label><input id="sixes" type="number" min="0" value="0"></div>
          <div><label>Balls Faced</label><input id="balls_faced" type="number" min="0" value="0"></div>
          <div><label>Balls Bowled</label><input id="balls_bowled" type="number" min="0" value="0"></div>
          <div><label>Runs Conceded</label><input id="runs_conceded" type="number" min="0" value="0"></div>
          <div><label>Maiden Overs</label><input id="maiden_overs" type="number" min="0" value="0"></div>
          <div><label>Catches</label><input id="catches" type="number" min="0" value="0"></div>
          <div><label>Stumpings</label><input id="stumpings" type="number" min="0" value="0"></div>
          <div><label>Run Out Direct</label><input id="run_out_direct" type="number" min="0" value="0"></div>
          <div><label>Run Out Indirect</label><input id="run_out_indirect" type="number" min="0" value="0"></div>
          <div><label>LBW/Bowled Wkts</label><input id="lbw_bowled_wickets" type="number" min="0" value="0"></div>
        </div>
        <div id="playerStatus" class="status"></div>
        <div class="players" id="playerBreakdown"></div>
      </section>
    </div>
  </div>

  <script>
    let state = null;

    function ownerCard(index, owner) {
      const lines = owner.players.map(player => player.player_name).join("\n");
      return `
        <div class="card">
          <input id="owner_name_${index}" value="${owner.owner_name}" placeholder="Owner name">
          <input id="team_name_${index}" value="${owner.team_name || owner.owner_name}" placeholder="Team name" style="margin-top:10px;width:100%;">
          <textarea id="players_${index}" placeholder="Enter 12 players, one per line">${lines}</textarea>
        </div>
      `;
    }

    function renderRules(rules) {
      const container = document.getElementById("rules");
      container.innerHTML = Object.entries(rules).map(([rule, value]) => `
        <div class="rule">
          <strong>${rule.replaceAll("_", " ")}</strong><br>
          ${value} pts
        </div>
      `).join("");
    }

    function renderLeaderboard() {
      const container = document.getElementById("leaderboard");
      if (!state) {
        container.innerHTML = "";
        return;
      }
      container.innerHTML = state.owners.map((owner, index) => `
        <div class="leaderboard-item">
          <div style="display:flex;gap:12px;align-items:center;">
            <div class="rank">#${index + 1}</div>
            <div>
              <div style="font-weight:700;font-size:18px;">${owner.owner_name}</div>
              <div style="color:var(--muted);font-size:13px;">${owner.team_name}</div>
            </div>
          </div>
          <div class="points">${owner.total_points} pts</div>
        </div>
      `).join("");
    }

    function renderOwnerEditor() {
      const container = document.getElementById("ownerEditor");
      container.innerHTML = state.owners.map((owner, index) => ownerCard(index, owner)).join("");
    }

    function populateOwners() {
      const ownerSelect = document.getElementById("ownerSelect");
      ownerSelect.innerHTML = state.owners.map((owner, index) => `<option value="${index}">${owner.owner_name}</option>`).join("");
      populatePlayers();
    }

    function populatePlayers() {
      const ownerIndex = Number(document.getElementById("ownerSelect").value || 0);
      const playerSelect = document.getElementById("playerSelect");
      const owner = state.owners[ownerIndex];
      if (!owner) {
        playerSelect.innerHTML = "";
        return;
      }
      playerSelect.innerHTML = owner.players.map((player, index) => `<option value="${index}">${player.player_name}</option>`).join("");
      fillSelectedPlayer();
      renderBreakdown(owner);
    }

    function fillSelectedPlayer() {
      const owner = state.owners[Number(document.getElementById("ownerSelect").value || 0)];
      const player = owner?.players[Number(document.getElementById("playerSelect").value || 0)];
      if (!player) return;
      const fields = ["runs", "wickets", "fours", "sixes", "balls_faced", "balls_bowled", "runs_conceded", "maiden_overs", "catches", "stumpings", "run_out_direct", "run_out_indirect", "lbw_bowled_wickets"];
      fields.forEach(field => {
        document.getElementById(field).value = player[field] ?? 0;
      });
    }

    function renderBreakdown(owner) {
      const container = document.getElementById("playerBreakdown");
      container.innerHTML = owner.players.map(player => `
        <div class="player-line">
          <div>
            <div style="font-weight:700;">${player.player_name}</div>
            <div style="color:var(--muted);font-size:13px;">Runs ${player.runs} | Wickets ${player.wickets}</div>
          </div>
          <div class="points">${player.points} pts</div>
        </div>
      `).join("");
    }

    async function fetchState() {
      const response = await fetch("/api/state");
      state = await response.json();
      renderRules(state.point_rules);
      renderLeaderboard();
      renderOwnerEditor();
      populateOwners();
    }

    function loadOwnersIntoEditor() {
      renderOwnerEditor();
      document.getElementById("ownerStatus").textContent = "Loaded the current saved owners and player names.";
    }

    async function saveOwners() {
      const owners = [];
      for (let index = 0; index < 12; index++) {
        const ownerNameElement = document.getElementById(`owner_name_${index}`);
        const teamNameElement = document.getElementById(`team_name_${index}`);
        const playersElement = document.getElementById(`players_${index}`);
        if (!ownerNameElement || !playersElement) continue;
        const playerNames = playersElement.value.split("\n").map(line => line.trim()).filter(Boolean).slice(0, 12);
        owners.push({
          owner_name: ownerNameElement.value.trim(),
          team_name: teamNameElement.value.trim(),
          players: playerNames.map(name => ({ player_name: name }))
        });
      }
      const response = await fetch("/api/owners", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ owners })
      });
      state = await response.json();
      renderLeaderboard();
      renderOwnerEditor();
      populateOwners();
      document.getElementById("ownerStatus").textContent = "Owners and players saved successfully.";
    }

    async function savePlayerStats() {
      const ownerIndex = Number(document.getElementById("ownerSelect").value || 0);
      const playerIndex = Number(document.getElementById("playerSelect").value || 0);
      const payload = {
        owner_index: ownerIndex,
        player_index: playerIndex,
        stats: {}
      };
      ["runs", "wickets", "fours", "sixes", "balls_faced", "balls_bowled", "runs_conceded", "maiden_overs", "catches", "stumpings", "run_out_direct", "run_out_indirect", "lbw_bowled_wickets"]
        .forEach(field => payload.stats[field] = Number(document.getElementById(field).value || 0));
      const response = await fetch("/api/player-stats", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state = await response.json();
      renderLeaderboard();
      populateOwners();
      document.getElementById("ownerSelect").value = String(ownerIndex);
      populatePlayers();
      document.getElementById("playerSelect").value = String(playerIndex);
      fillSelectedPlayer();
      document.getElementById("playerStatus").textContent = "Player stats updated and live scores refreshed.";
    }

    async function resetDemoData() {
      const response = await fetch("/api/reset-demo", { method: "POST" });
      state = await response.json();
      renderLeaderboard();
      renderOwnerEditor();
      populateOwners();
      document.getElementById("ownerStatus").textContent = "Demo teams restored.";
    }

    setInterval(async () => {
      const currentOwnerIndex = document.getElementById("ownerSelect")?.value ?? "0";
      const currentPlayerIndex = document.getElementById("playerSelect")?.value ?? "0";
      const response = await fetch("/api/state");
      state = await response.json();
      renderLeaderboard();
      populateOwners();
      document.getElementById("ownerSelect").value = currentOwnerIndex;
      populatePlayers();
      document.getElementById("playerSelect").value = currentPlayerIndex;
      fillSelectedPlayer();
    }, 5000);

    fetchState();
  </script>
</body>
</html>
"""


class FantasyCricketHandler(BaseHTTPRequestHandler):
    def _json_response(self, payload, status=200):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _html_response(self, payload):
        encoded = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _read_json(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else b"{}"
        return json.loads(body.decode("utf-8"))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self._html_response(HTML)
            return
        if path == "/api/state":
            self._json_response(leaderboard_state())
            return
        self.send_error(404, "Not Found")

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/owners":
            payload = self._read_json()
            self._json_response(normalize_payload(payload))
            return
        if path == "/api/player-stats":
            payload = self._read_json()
            data = load_data()
            owner_index = as_int(payload.get("owner_index"))
            player_index = as_int(payload.get("player_index"))
            stats = payload.get("stats", {})
            try:
                player = data["owners"][owner_index]["players"][player_index]
            except (IndexError, KeyError, TypeError):
                self._json_response({"error": "Invalid owner or player selection."}, status=400)
                return
            for field in [
                "runs",
                "wickets",
                "fours",
                "sixes",
                "balls_faced",
                "balls_bowled",
                "runs_conceded",
                "maiden_overs",
                "catches",
                "stumpings",
                "run_out_direct",
                "run_out_indirect",
                "lbw_bowled_wickets",
            ]:
                player[field] = as_int(stats.get(field))
            save_data(data)
            self._json_response(leaderboard_state())
            return
        if path == "/api/reset-demo":
            save_data(build_demo_data())
            self._json_response(leaderboard_state())
            return
        self.send_error(404, "Not Found")

    def log_message(self, format_string, *args):
        return


if __name__ == "__main__":
    load_data()
    server = ThreadingHTTPServer((HOST, PORT), FantasyCricketHandler)
    print(f"Fantasy cricket app running at http://{HOST}:{PORT}")
    print("Open the URL in your browser. Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.server_close()
