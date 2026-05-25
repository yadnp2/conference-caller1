import vonage, threading, time, os, json, functools, uuid as _uuid, requests, queue as _queue, gspread
from google.oauth2.service_account import Credentials as _SACredentials
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, redirect, session, url_for, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from vonage_voice.models import CreateCallRequest, ToPhone, Phone, TtsStreamOptions
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
app.secret_key = os.environ["SESSION_SECRET"]

VONAGE_APP_ID      = os.environ["VONAGE_APP_ID"]
FROM_NUMBER        = os.environ["FROM_NUMBER"]
BASE_URL           = os.environ["BASE_URL"]
SHEET_TAB          = os.environ.get("SHEET_NAME", "Sheet1")
CONFERENCE_NAME    = "DailyConference"
EASTERN            = ZoneInfo("America/New_York")
BASE_DIR           = os.path.dirname(__file__)
LOCAL_NUMBERS_FILE = os.path.join(BASE_DIR, "numbers_local.json")
NAMES_FILE         = os.path.join(BASE_DIR, "names.json")
BOOK_FILE          = os.path.join(BASE_DIR, "book_state.json")
SETTINGS_FILE      = os.path.join(BASE_DIR, "settings.json")
SCHEDULE_FILE      = os.path.join(BASE_DIR, "schedule.json")
USERS_FILE         = os.path.join(BASE_DIR, "users.json")
RECORDINGS_DIR     = os.path.join(BASE_DIR, "recordings")
RECORDING_META_FILE= os.path.join(BASE_DIR, "recording_meta.json")
os.makedirs(RECORDINGS_DIR, exist_ok=True)

_raw_key = os.environ["VONAGE_PRIVATE_KEY"]
_private_key = _raw_key.replace("\\n", "\n")

client = vonage.Vonage(vonage.Auth(
    application_id=VONAGE_APP_ID,
    private_key=_private_key
))

# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS call_logs (
                    id SERIAL PRIMARY KEY,
                    run_time TIMESTAMPTZ DEFAULT NOW(),
                    number TEXT NOT NULL,
                    name TEXT DEFAULT '',
                    status TEXT NOT NULL,
                    uuid TEXT,
                    error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS recording_meta_db (
                    id SERIAL PRIMARY KEY,
                    url TEXT,
                    date TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    size_bytes INT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        conn.commit()
    print("Database initialized.")

def log_call_db(number, name, status, uuid=None, error=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO call_logs (number, name, status, uuid, error)
                    VALUES (%s, %s, %s, %s, %s) RETURNING id
                """, (number, name, status, uuid, error))
                row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
    except Exception as e:
        print(f"log_call_db error: {e}")
        return None

def update_call_log_db(log_id, status):
    if not log_id:
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE call_logs SET status=%s WHERE id=%s", (status, log_id))
            conn.commit()
    except Exception as e:
        print(f"update_call_log_db error: {e}")

def get_call_history_db(limit=200):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT number, name, status, uuid, error,
                           run_time AT TIME ZONE 'America/New_York' as run_time_et
                    FROM call_logs ORDER BY id DESC LIMIT %s
                """, (limit,))
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"get_call_history_db error: {e}")
        return []

def save_recording_meta_db(data):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO recording_meta_db (url, date, start_time, end_time, size_bytes)
                    VALUES (%s, %s, %s, %s, %s)
                """, (data.get("url"), data.get("date"), data.get("start_time"),
                      data.get("end_time"), data.get("size_bytes", 0)))
            conn.commit()
    except Exception as e:
        print(f"save_recording_meta_db error: {e}")

# ── Auth ──────────────────────────────────────────────────────────────────────

def get_user():
    return _read_json(USERS_FILE, {})

def account_exists():
    return bool(get_user().get("username"))

def create_account(username, password):
    _write_json(USERS_FILE, {
        "username": username.strip().lower(),
        "password_hash": generate_password_hash(password)
    })

def check_credentials(username, password):
    u = get_user()
    return (u.get("username") == username.strip().lower()
            and check_password_hash(u.get("password_hash", ""), password))

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

_AUTH_CSS = """
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
       background:#0f1117;color:#e2e8f0;display:flex;align-items:center;
       justify-content:center;min-height:100vh;padding:1.5rem}
  .card{background:#1e2433;border:1px solid #2d3748;border-radius:14px;
        padding:2rem 1.75rem;width:100%;max-width:360px;display:flex;
        flex-direction:column;gap:1.25rem}
  h1{font-size:1.25rem;font-weight:700;color:#f8fafc}
  .sub{font-size:.85rem;color:#64748b}
  label{font-size:.78rem;font-weight:600;color:#94a3b8;display:block;margin-bottom:.35rem}
  input{width:100%;background:#0f1117;border:1px solid #2d3748;color:#e2e8f0;
        border-radius:8px;padding:.65rem .85rem;font-size:.9rem}
  input:focus{outline:none;border-color:#3b82f6}
  .btn{width:100%;padding:.75rem;background:#2563eb;color:#fff;border:none;
       border-radius:10px;font-size:.95rem;font-weight:700;cursor:pointer;margin-top:.25rem}
  .btn:hover{background:#1d4ed8}
  .err{background:#450a0a;border:1px solid #7f1d1d;color:#fca5a5;
       border-radius:8px;padding:.6rem .85rem;font-size:.85rem}
"""

@app.route("/login", methods=["GET","POST"])
def login():
    if not account_exists():
        return redirect(url_for("setup"))
    error = ""
    if request.method == "POST":
        if check_credentials(request.form.get("username",""),
                             request.form.get("password","")):
            session["logged_in"] = True
            session.permanent = True
            next_url = request.args.get("next") or request.form.get("next") or "/status"
            return redirect(next_url)
        error = "<div class='err'>Incorrect username or password.</div>"
    next_h = f"<input type='hidden' name='next' value='{request.args.get('next','')}'>" if request.args.get("next") else ""
    return f"""<!DOCTYPE html><html lang='en'><head>
  <meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Sign In — Conference Manager</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#1e2433'/>
  <style>{_AUTH_CSS}</style></head><body>
  <div class='card'>
    <div><h1>Conference Manager</h1><p class='sub'>Sign in to continue</p></div>
    {error}
    <form method='POST'>
      {next_h}
      <div><label>Username</label><input name='username' type='text' autocomplete='username' required/></div>
      <div><label>Password</label><input name='password' type='password' autocomplete='current-password' required/></div>
      <button class='btn'>Sign In</button>
    </form>
  </div></body></html>"""

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/setup", methods=["GET","POST"])
def setup():
    if account_exists():
        return redirect(url_for("login"))
    error = ""
    if request.method == "POST":
        username = request.form.get("username","").strip()
        password = request.form.get("password","")
        confirm  = request.form.get("confirm","")
        if not username or not password:
            error = "<div class='err'>Username and password are required.</div>"
        elif password != confirm:
            error = "<div class='err'>Passwords do not match.</div>"
        elif len(password) < 6:
            error = "<div class='err'>Password must be at least 6 characters.</div>"
        else:
            create_account(username, password)
            session["logged_in"] = True
            return redirect("/status")
    return f"""<!DOCTYPE html><html lang='en'><head>
  <meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Create Account — Conference Manager</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#1e2433'/>
  <style>{_AUTH_CSS}</style></head><body>
  <div class='card'>
    <div><h1>Create Your Account</h1>
    <p class='sub'>Set up your admin account to access Conference Manager.</p></div>
    {error}
    <form method='POST'>
      <div><label>Username</label><input name='username' type='text' autocomplete='username' required/></div>
      <div><label>Password</label><input name='password' type='password' autocomplete='new-password' required/></div>
      <div><label>Confirm Password</label><input name='confirm' type='password' autocomplete='new-password' required/></div>
      <button class='btn'>Create Account</button>
    </form>
  </div></body></html>"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean(n):
    n = str(n).strip().replace(" ","").replace("-","").replace("(","").replace(")","")
    if n.isdigit() and len(n) >= 10:
        return n if n.startswith("1") else "1" + n
    return None

def _read_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def _write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# ── Settings (persisted) ──────────────────────────────────────────────────────

settings_lock = threading.Lock()

def _settings():
    return _read_json(SETTINGS_FILE, {})

def get_reading_enabled():
    with settings_lock:
        return _settings().get("reading_enabled", False)

def set_reading_enabled(value: bool):
    with settings_lock:
        s = _settings(); s["reading_enabled"] = value; _write_json(SETTINGS_FILE, s)

def get_record_enabled():
    with settings_lock:
        return _settings().get("record_enabled", True)

def set_record_enabled(value: bool):
    with settings_lock:
        s = _settings(); s["record_enabled"] = value; _write_json(SETTINGS_FILE, s)

def get_replay_enabled():
    with settings_lock:
        return _settings().get("replay_enabled", True)

def set_replay_enabled(value: bool):
    with settings_lock:
        s = _settings(); s["replay_enabled"] = value; _write_json(SETTINGS_FILE, s)

def get_announcements_enabled():
    with settings_lock:
        return _settings().get("announcements_enabled", True)

def set_announcements_enabled(value: bool):
    with settings_lock:
        s = _settings(); s["announcements_enabled"] = value; _write_json(SETTINGS_FILE, s)

def get_spreadsheet_id():
    # Env var takes priority so it survives redeployments without manual re-entry
    return os.environ.get("SPREADSHEET_ID", "").strip() or _settings().get("spreadsheet_id", "")

# ── Recording metadata ────────────────────────────────────────────────────────

recording_lock = threading.Lock()

def load_recording_meta():
    with recording_lock:
        return _read_json(RECORDING_META_FILE, {})

def save_recording_meta(data):
    with recording_lock:
        _write_json(RECORDING_META_FILE, data)

def _vonage_jwt():
    import jwt as pyjwt
    now = int(time.time())
    payload = {"application_id": VONAGE_APP_ID, "iat": now,
                "jti": str(_uuid.uuid4()), "exp": now + 300}
    key = _private_key.encode() if isinstance(_private_key, str) else _private_key
    return pyjwt.encode(payload, key, algorithm="RS256")

def download_recording(url):
    """Download Vonage recording to disk. Returns True on success."""
    try:
        token = _vonage_jwt()
        resp  = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if resp.status_code == 200:
            with open(os.path.join(RECORDINGS_DIR, "latest.mp3"), "wb") as f:
                f.write(resp.content)
            return True
        print(f"Recording download failed: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"Recording download error: {e}")
    return False

# ── Schedule management ───────────────────────────────────────────────────────

DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

DEFAULT_SCHEDULE = []

schedule_lock = threading.Lock()

def load_schedule():
    with schedule_lock:
        return _read_json(SCHEDULE_FILE, DEFAULT_SCHEDULE)

def save_schedule(entries):
    with schedule_lock:
        _write_json(SCHEDULE_FILE, entries)

def add_schedule_entry(day: int, hour: int, minute: int):
    entries = load_schedule()
    entry   = {"day": day, "hour": hour, "minute": minute}
    if entry not in entries:
        entries.append(entry)
        entries.sort(key=lambda e: (e["day"], e["hour"], e["minute"]))
        save_schedule(entries)

def remove_schedule_entry(day: int, hour: int, minute: int):
    entries = load_schedule()
    entries = [e for e in entries
               if not (e["day"] == day and e["hour"] == hour and e["minute"] == minute)]
    save_schedule(entries)

def set_day_schedule(day: int, hour: int, minute: int):
    """Replace whatever is scheduled for `day` with a single new time."""
    entries = [e for e in load_schedule() if e["day"] != day]
    entries.append({"day": day, "hour": hour, "minute": minute})
    entries.sort(key=lambda e: (e["day"], e["hour"], e["minute"]))
    save_schedule(entries)

def clear_day_schedule(day: int):
    """Remove all schedule entries for `day`."""
    save_schedule([e for e in load_schedule() if e["day"] != day])

def fmt_schedule_entry(e):
    h, m = e["hour"], e["minute"]
    ampm  = "AM" if h < 12 else "PM"
    h12   = h % 12 or 12
    return f"{DAYS[e['day']]}  {h12}:{m:02d} {ampm} ET"

# ── Google Sheets (service account via gspread) ───────────────────────────────

_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def _gspread_client():
    """Return an authorised gspread client using the service account JSON secret."""
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON secret is not set.")
    info = json.loads(raw)
    creds = _SACredentials.from_service_account_info(info, scopes=_SHEETS_SCOPES)
    return gspread.authorize(creds)

def sync_from_sheets():
    """Read numbers + names from the configured Google Sheet and merge into local store.
       Sheet format: Column A = phone number, Column B = name (header in row 1).
       Returns (success: bool, message: str).
    """
    sid = get_spreadsheet_id()
    if not sid:
        return False, "No spreadsheet ID saved yet."
    try:
        gc = _gspread_client()
        sh = gc.open_by_key(sid)
        ws = sh.worksheet(SHEET_TAB)
        rows = ws.get("A2:B") or []
    except gspread.exceptions.SpreadsheetNotFound:
        return False, "Spreadsheet not found. Check the ID and make sure it's shared with the service account."
    except gspread.exceptions.WorksheetNotFound:
        return False, f"Tab '{SHEET_TAB}' not found in the spreadsheet."
    except Exception as e:
        return False, f"Could not read sheet: {e}"
    if not rows:
        return True, "Sheet appears to be empty (no data below header row)."
    with numbers_lock:
        added, removed, paused = _load_local()
    names = _read_json(NAMES_FILE, {})
    imported = 0
    for row in rows:
        # Column A = name, Column B = number
        name = (row[0] if row else "").strip()
        raw  = (row[1] if len(row) > 1 else "").strip()
        clean = _clean(raw)
        if not clean:
            continue
        added.add(clean)
        removed.discard(clean)
        if name:
            names[clean] = name
        imported += 1
    with numbers_lock:
        _save_local(added, removed, paused)
    _write_json(NAMES_FILE, names)
    return True, f"Synced {imported} number(s) from Google Sheet."

# ── Local number + name store ─────────────────────────────────────────────────

numbers_lock = threading.Lock()

def _load_local():
    d = _read_json(LOCAL_NUMBERS_FILE, {"added": [], "removed": [], "paused": []})
    return set(d.get("added", [])), set(d.get("removed", [])), set(d.get("paused", []))

def _save_local(added, removed, paused=None):
    _write_json(LOCAL_NUMBERS_FILE, {
        "added":  sorted(added),
        "removed": sorted(removed),
        "paused": sorted(paused or set()),
    })

def get_name(number):
    return _read_json(NAMES_FILE, {}).get(number, "")

def set_name(number, name):
    names = _read_json(NAMES_FILE, {})
    if name:
        names[number] = name.strip()
    else:
        names.pop(number, None)
    _write_json(NAMES_FILE, names)

# ── Book management ───────────────────────────────────────────────────────────

book_lock = threading.Lock()

def load_book():
    return _read_json(BOOK_FILE, {"portions": [], "current_index": 0, "title": ""})

def save_book(data):
    _write_json(BOOK_FILE, data)

def get_todays_reading():
    with book_lock:
        b = load_book()
    portions = b.get("portions", [])
    if not portions:
        return None
    return portions[b.get("current_index", 0) % len(portions)]

def advance_reading():
    with book_lock:
        b = load_book()
        if b.get("portions"):
            b["current_index"] = (b.get("current_index", 0) + 1) % len(b["portions"])
            save_book(b)

def upload_book(text, title="", lines_per_portion=30):
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    if len(paragraphs) < 2:
        all_lines = [l for l in text.splitlines() if l.strip()]
        paragraphs = []
        for i in range(0, len(all_lines), lines_per_portion):
            chunk = " ".join(all_lines[i:i + lines_per_portion])
            if chunk:
                paragraphs.append(chunk)
    portions = [p[:1500] for p in paragraphs if p]
    with book_lock:
        save_book({"portions": portions, "current_index": 0, "title": title})
    return len(portions)

# ── Reading vote tracking ─────────────────────────────────────────────────────
# expected: UUIDs of participants who actually answered (connected)
# votes:    UUIDs of connected participants who pressed 1
# triggered: whether the reading has already been played this session

vote_lock = threading.Lock()
reading_session = {"expected": set(), "votes": set(), "triggered": False}

def _reset_reading_session():
    with vote_lock:
        reading_session["expected"] = set()
        reading_session["votes"]    = set()
        reading_session["triggered"] = False

def _mark_answered(uuid):
    with vote_lock:
        reading_session["expected"].add(uuid)
    _check_and_trigger_reading()

def _record_vote(uuid):
    with vote_lock:
        reading_session["votes"].add(uuid)
    _check_and_trigger_reading()

def _check_and_trigger_reading():
    with vote_lock:
        expected  = set(reading_session["expected"])
        votes     = set(reading_session["votes"])
        triggered = reading_session["triggered"]

    if triggered or not expected or not votes:
        return
    if votes < expected:          # not everyone voted yet
        return
    if votes != expected:         # someone joined but didn't vote — no auto-read
        return

    # All connected participants voted yes — play the reading
    with vote_lock:
        if reading_session["triggered"]:  # double-check inside lock
            return
        reading_session["triggered"] = True
        first_uuid = next(iter(votes))

    reading = get_todays_reading()
    if not reading:
        return

    print("All participants voted yes — playing reading into conference.")
    try:
        client.voice.play_tts_into_call(
            first_uuid,
            TtsStreamOptions(text=reading, language="en-US", level=1.0)
        )
    except Exception as e:
        print(f"Failed to play reading: {e}")

# ── Call state (in-memory) ────────────────────────────────────────────────────
# running:           dial-out loop is still sending calls
# conference_active: at least one person is currently in the conference
# pending_calls:     outbound calls not yet in a final state
# summary_fired:     welcome announcement already played this session

last_run = {"time": None, "calls": [], "inbound_calls": [], "running": False,
            "conference_active": False, "pending_calls": 0, "summary_fired": False}
call_status_map  = {}
inbound_uuid_map = {}
session_blocked  = set()   # numbers blocked from joining THIS session only
log_lock = threading.Lock()

FINAL_STATUSES = {"connected", "voicemail", "completed", "busy", "cancelled",
                  "failed", "rejected", "unanswered", "timeout", "error"}

# ── Serialized announcement queue ─────────────────────────────────────────────
# All join announcements are processed one at a time so they never overlap.

_ann_queue = _queue.Queue()

def _announcement_worker():
    while True:
        name, exclude_uuid = _ann_queue.get()
        try:
            text = f"{name} has joined the conference."
            with log_lock:
                uuids = [u for u, e in call_status_map.items()
                         if e.get("status") == "connected" and u != exclude_uuid]
            for u in uuids:
                try:
                    client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US", level=1.0))
                except Exception as e:
                    print(f"Announce join failed for {u}: {e}")
            # Wait for speech to finish before playing the next announcement.
            # Estimate ~10 chars/second speaking rate plus a 1-second buffer.
            time.sleep(max(3.0, len(text) / 10) + 1.0)
        except Exception as e:
            print(f"Announcement worker error: {e}")
        finally:
            _ann_queue.task_done()

threading.Thread(target=_announcement_worker, daemon=True).start()

def announce_join(name, exclude_uuid=None, delay=0):
    """Queue a join announcement. Announcements play one at a time, never overlapping."""
    if not name:
        return
    if delay:
        time.sleep(delay)
    _ann_queue.put((name, exclude_uuid))

def get_numbers():
    with numbers_lock:
        added, removed, paused = _load_local()
    return sorted(added - paused)

def get_all_numbers_with_source():
    with numbers_lock:
        added, removed, paused = _load_local()
    return [(n, "local", get_name(n), n in paused) for n in sorted(added)]

def dial(number):
    # Dedup: skip if this number is already being dialed in this session
    with log_lock:
        already = any(e.get("number") == number for e in last_run.get("calls", []))
    if already:
        print(f"Skipping duplicate dial for {number}")
        return
    name = get_name(number)
    try:
        response = client.voice.create_call(CreateCallRequest(
            to=[ToPhone(number=number)],
            from_=Phone(number=FROM_NUMBER),
            answer_url=[f"{BASE_URL}/answer"],
            event_url=[f"{BASE_URL}/event"],
            machine_detection="hangup",
        ))
        uuid   = getattr(response, "uuid", None)
        log_id = log_call_db(number, name, "dialing", uuid=uuid)
        entry  = {"number": number, "name": name, "status": "dialing", "uuid": uuid, "log_id": log_id}
        with log_lock:
            if uuid:
                call_status_map[uuid] = entry
            last_run["calls"].append(entry)
    except Exception as e:
        log_call_db(number, name, "error", error=str(e))
        entry = {"number": number, "name": name,
                 "status": "error", "uuid": None, "error": str(e)}
        with log_lock:
            last_run["calls"].append(entry)

def _play_participant_summary():
    """Wait until every outbound call has a final status, then announce connected participants."""
    MAX_WAIT = 120
    POLL     = 1
    waited   = 0
    while waited < MAX_WAIT:
        time.sleep(POLL)
        waited += POLL
        with log_lock:
            pending   = last_run.get("pending_calls", 0)
            still_out = last_run.get("running", False)
        if still_out or pending > 0:
            continue
        break

    with log_lock:
        if last_run.get("summary_fired"):
            return
        last_run["summary_fired"] = True
        connected_names = [e["name"] for e in last_run.get("calls", [])
                           if e.get("status") == "connected" and e.get("name")]
        uuids = [u for u, e in call_status_map.items() if e.get("status") == "connected"]

    if not connected_names or not uuids:
        print("Summary: no connected participants to announce.")
        return
    if len(connected_names) == 1:
        text = f"Welcome. {connected_names[0]} has joined the call."
    elif len(connected_names) == 2:
        text = f"Welcome everyone. {connected_names[0]} and {connected_names[1]} have joined the call."
    else:
        names_list = ", ".join(connected_names[:-1]) + ", and " + connected_names[-1]
        text = f"Welcome everyone. The following participants have joined: {names_list}."
    print(f"Summary announcement: {text}")
    for u in uuids:
        try:
            client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US", level=1.0))
        except Exception as e:
            print(f"Summary announcement failed for {u}: {e}")

def start_conference():
    with log_lock:
        if last_run["running"]:
            return
        last_run["running"]           = True
        last_run["conference_active"] = False
        last_run["pending_calls"]     = 0
        last_run["summary_fired"]     = False
        last_run["time"]         = datetime.now(EASTERN).strftime("%A %b %d at %-I:%M %p %Z")
        last_run["calls"]        = []
        last_run["inbound_calls"] = []
        call_status_map.clear()
    session_blocked.clear()
    _reset_reading_session()
    advance_reading()
    print("Starting conference...")
    numbers = get_numbers()
    with log_lock:
        last_run["pending_calls"] = len(numbers)
    try:
        for number in numbers:
            dial(number)
            time.sleep(2)
    finally:
        with log_lock:
            last_run["running"] = False
        if get_announcements_enabled():
            threading.Thread(target=_play_participant_summary, daemon=True).start()

# ── Vonage webhooks ───────────────────────────────────────────────────────────

def _conference_ncco():
    ncco = {"action": "conversation", "name": CONFERENCE_NAME,
            "startOnEnter": True, "endOnExit": False}
    if get_record_enabled():
        ncco["record"] = True
        ncco["eventUrl"] = [f"{BASE_URL}/recording"]
    return [ncco]

def _vote_ncco():
    """NCCO with vote prompt then conference — used when reading is enabled and a book is loaded."""
    return [
        {
            "action": "talk",
            "text": ("Joining you into the Shmiras Halashon conference. "
                     "Press 1 to vote for today's reading to be read aloud automatically. "
                     "Otherwise, stay on the line to join now.")
        },
        {
            "action": "input",
            "type": ["dtmf"],
            "dtmf": {"maxDigits": 1, "timeOut": 8},
            "eventUrl": [f"{BASE_URL}/reading-vote"]
        },
        *_conference_ncco()
    ]

def _plain_ncco():
    return [
        {"action": "talk", "text": "Joining you into the Shmiras Halashon conference."},
        *_conference_ncco()
    ]

def _inbound_join_ncco():
    """NCCO for inbound callers: press 1 to join, hang up if they don't.
    /join-announce returns the conference NCCO only if they pressed 1,
    otherwise returns an empty NCCO which ends the call."""
    return [
        {"action": "talk",
         "text": "Press 1 to join the Shmiras Halashon conference."},
        {"action": "input", "type": ["dtmf"],
         "dtmf": {"maxDigits": 1, "timeOut": 6},
         "eventUrl": [f"{BASE_URL}/join-announce"]},
    ]

def _replay_ncco():
    """NCCO that plays back the latest recording for a late caller."""
    meta = load_recording_meta()
    date_str = meta.get("date", "a previous session")
    return [
        {"action": "talk", "text": f"The Shmiras Halashon conference from {date_str} is now playing."},
        {"action": "stream", "streamUrl": [f"{BASE_URL}/recordings/audio"], "level": 0},
        {"action": "talk", "text": "You have reached the end of the recording. Goodbye."},
    ]

def _answer_ncco(uuid=None, inbound=False):
    """Return the right NCCO. For inbound callers we also register them in the
    expected-voter set so the vote count is accurate."""
    # Late-caller replay: inbound, conference NOT active, replay on, recording exists
    if inbound:
        with log_lock:
            conf_active = last_run.get("conference_active", False)
        if not conf_active and get_replay_enabled():
            if os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3")):
                return _replay_ncco()

    if get_reading_enabled() and get_todays_reading():
        if inbound and uuid:
            threading.Thread(target=_mark_answered, args=(uuid,), daemon=True).start()
        return _vote_ncco()
    if inbound and get_announcements_enabled():
        return _inbound_join_ncco()
    return _plain_ncco()

def _handle_inbound_announcement(uuid, from_number):
    """Store inbound UUID→number mapping for use by /join-announce."""
    clean = _clean(from_number) if from_number else None
    num   = clean or from_number
    if num:
        with log_lock:
            inbound_uuid_map[uuid] = num

def _is_approved_member(number):
    """Check if a number is in the approved members list (from Google Sheet / local store)."""
    if not number or number == "Unknown":
        return False
    with numbers_lock:
        added, removed, paused = _load_local()
    result = number in added
    if not result:
        print(f"[member check] {number} NOT found in approved list. List has {len(added)} numbers: {list(added)[:5]}...")
    return result

@app.route("/answer", methods=["GET","POST"])
def answer():
    data = request.get_json(silent=True) or request.values
    uuid = data.get("uuid", "")
    with log_lock:
        is_inbound = uuid not in call_status_map
    if is_inbound:
        from_num = data.get("from", "")
        clean    = _clean(from_num) if from_num else None
        num      = clean or from_num or "Unknown"

        # Reject if no caller ID (private/hidden number)
        if not clean:
            return jsonify([{"action": "talk",
                "text": "Sorry, calls with a hidden caller ID cannot join this conference. Please call back with caller ID enabled. Goodbye."}])

        # Reject if not on the approved members list
        if not _is_approved_member(clean):
            return jsonify([{"action": "talk",
                "text": "Sorry, your number is not registered for this conference. Please contact the administrator. Goodbye."}])

        # Reject if session-blocked (kicked this session)
        if clean in session_blocked:
            return jsonify([{"action": "talk",
                "text": "You are not able to join this conference session. Goodbye."}])

        _handle_inbound_announcement(uuid, from_num)
        # Determine call source: replay vs live
        with log_lock:
            running = last_run.get("running", False)
        if not running and get_replay_enabled() and os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3")):
            source = "inbound-replay"
        else:
            source = "inbound-live"
        entry = {
            "number": num,
            "name": get_name(num),
            "source": source,
            "time": datetime.now(EASTERN).strftime("%-I:%M %p %Z"),
        }
        with log_lock:
            last_run["inbound_calls"].append(entry)
    return jsonify(_answer_ncco(uuid=uuid, inbound=is_inbound))

@app.route("/inbound", methods=["GET","POST"])
def inbound():
    data = request.get_json(silent=True) or request.values
    uuid = data.get("uuid", "")
    _handle_inbound_announcement(uuid, data.get("from", ""))
    return jsonify(_answer_ncco(uuid=uuid, inbound=True))

@app.route("/join-announce", methods=["GET","POST"])
def join_announce():
    """Called by Vonage when an inbound caller presses a key on the join prompt.
    Only joins the conference if they pressed 1. Otherwise hangs up."""
    data  = request.get_json(silent=True) or {}
    uuid  = data.get("uuid", "")
    digit = (data.get("dtmf") or {}).get("digits", "") or data.get("digits", "")
    if str(digit).strip() == "1":
        if uuid and get_announcements_enabled():
            with log_lock:
                num = inbound_uuid_map.get(uuid, "")
            name = get_name(num) if num else ""
            if name:
                threading.Thread(target=announce_join,
                                 kwargs={"name": name, "exclude_uuid": uuid, "delay": 4},
                                 daemon=True).start()
        return jsonify(_conference_ncco())
    else:
        # They didn't press 1 — end the call
        return jsonify([{"action": "talk", "text": "Goodbye."}])

@app.route("/reading-vote", methods=["GET","POST"])
def reading_vote():
    """Called by Vonage when a participant presses a key during the vote prompt."""
    data = request.get_json(silent=True) or {}
    uuid = data.get("uuid", "")
    dtmf = (data.get("dtmf") or {}).get("digits", "") or data.get("digits", "")
    if str(dtmf).strip() == "1" and uuid:
        threading.Thread(target=_record_vote, args=(uuid,), daemon=True).start()
    # Always return the conference NCCO — everyone joins regardless of their vote
    return jsonify(_conference_ncco())

@app.route("/event", methods=["GET","POST"])
def event():
    data   = request.get_json(silent=True) or {}
    uuid   = data.get("uuid", "")
    status = data.get("status", "")
    print(f"Event: {status} -> {data.get('to','')}")
    with log_lock:
        if uuid in call_status_map:
            entry       = call_status_map[uuid]
            log_id      = entry.get("log_id")
            prev_status = entry.get("status", "dialing")

            if status == "answered":
                entry["status"]     = "connected"
                entry["answered_at"] = time.time()
                last_run["conference_active"] = True
                if prev_status not in FINAL_STATUSES:
                    last_run["pending_calls"] = max(0, last_run["pending_calls"] - 1)
                if log_id:
                    threading.Thread(target=update_call_log_db, args=(log_id, "connected"), daemon=True).start()
                if get_reading_enabled() and get_todays_reading():
                    threading.Thread(target=_mark_answered, args=(uuid,), daemon=True).start()

            elif status == "machine":
                entry["status"] = "voicemail"
                entry.pop("answered_at", None)
                if prev_status not in FINAL_STATUSES:
                    last_run["pending_calls"] = max(0, last_run["pending_calls"] - 1)
                if log_id:
                    threading.Thread(target=update_call_log_db, args=(log_id, "voicemail"), daemon=True).start()

            elif status in ("completed","busy","cancelled","failed","rejected","unanswered","timeout"):
                if entry["status"] == "connected":
                    duration = time.time() - entry.get("answered_at", time.time())
                    if duration < 8:
                        entry["status"] = "unanswered"
                        if log_id:
                            threading.Thread(target=update_call_log_db, args=(log_id, "unanswered"), daemon=True).start()
                    # Update conference_active: check if anyone else is still connected
                    still_connected = any(
                        e.get("status") == "connected" and e_uuid != uuid
                        for e_uuid, e in call_status_map.items()
                    )
                    last_run["conference_active"] = still_connected
                elif entry["status"] != "voicemail":
                    entry["status"] = status
                    if prev_status not in FINAL_STATUSES:
                        last_run["pending_calls"] = max(0, last_run["pending_calls"] - 1)
                    if log_id:
                        threading.Thread(target=update_call_log_db, args=(log_id, status), daemon=True).start()
    return "OK", 200

# ── Recording webhook & audio serve ──────────────────────────────────────────

@app.route("/recording", methods=["GET","POST"])
def recording_webhook():
    """Vonage posts here when a conference recording is ready."""
    data = request.get_json(silent=True) or request.values.to_dict()
    rec_url    = data.get("recording_url") or data.get("url")
    start_time = data.get("start_time", "")
    end_time   = data.get("end_time", "")
    size_bytes = data.get("size", 0)
    print(f"Recording webhook: url={rec_url} start={start_time} size={size_bytes}")
    if rec_url:
        # Format date for display
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            date_str = dt.astimezone(EASTERN).strftime("%A %B %-d, %Y at %-I:%M %p ET")
        except Exception:
            date_str = start_time or "unknown"
        def _do_download():
            ok = download_recording(rec_url)
            if ok:
                meta = {
                    "url": rec_url, "date": date_str,
                    "start_time": start_time, "end_time": end_time,
                    "size_bytes": int(size_bytes),
                }
                save_recording_meta(meta)
                save_recording_meta_db(meta)
                print("Recording saved successfully.")
        threading.Thread(target=_do_download, daemon=True).start()
    return "OK", 200

@app.route("/recordings/audio")
def recording_audio():
    """Serve the latest recording MP3 (for Vonage stream NCCO or browser download)."""
    from flask import send_from_directory
    path = os.path.join(RECORDINGS_DIR, "latest.mp3")
    if not os.path.exists(path):
        return "No recording available", 404
    return send_from_directory(RECORDINGS_DIR, "latest.mp3", mimetype="audio/mpeg",
                               as_attachment=False)

@app.route("/recording/toggle", methods=["POST"])
@login_required
def recording_toggle():
    set_record_enabled(not get_record_enabled())
    return jsonify({"ok": True, "value": get_record_enabled()})

@app.route("/replay/toggle", methods=["POST"])
@login_required
def replay_toggle():
    set_replay_enabled(not get_replay_enabled())
    return jsonify({"ok": True, "value": get_replay_enabled()})

@app.route("/announcements/toggle", methods=["POST"])
@login_required
def announcements_toggle():
    set_announcements_enabled(not get_announcements_enabled())
    return jsonify({"ok": True, "value": get_announcements_enabled()})

# ── Root redirect ─────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def root():
    return redirect("/status")

# ── PWA manifest & service worker ─────────────────────────────────────────────

@app.route("/manifest.json")
def manifest():
    from flask import Response
    import json as _json
    data = {
        "name": "Conference Manager",
        "short_name": "Conference",
        "description": "Manage and monitor daily conference calls",
        "start_url": "/status",
        "display": "standalone",
        "background_color": "#0f1117",
        "theme_color": "#1e2433",
        "orientation": "portrait-primary",
        "icons": [
            {"src": "/static/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}
        ]
    }
    return Response(_json.dumps(data), mimetype="application/manifest+json")

@app.route("/sw.js")
def service_worker():
    from flask import Response
    sw = """
const CACHE = 'conf-v1';
const OFFLINE = ['/status', '/static/icon.svg'];
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(OFFLINE)));
  self.skipWaiting();
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});
self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});
"""
    return Response(sw, mimetype="application/javascript")

# ── Trigger ───────────────────────────────────────────────────────────────────

@app.route("/trigger", methods=["POST"])
@login_required
def trigger():
    with log_lock:
        if last_run["running"]:
            return jsonify({"ok": False, "error": "Already running"}), 409
    threading.Thread(target=start_conference, daemon=True).start()
    return jsonify({"ok": True})

# ── Number management ─────────────────────────────────────────────────────────

@app.route("/sheets/sync", methods=["POST"])
@login_required
def sheets_sync():
    ok, msg = sync_from_sheets()
    session["sheets_msg"] = msg
    session["sheets_ok"]  = ok
    return redirect("/status")

@app.route("/numbers/add", methods=["POST"])
@login_required
def numbers_add():
    n    = _clean(request.form.get("number", "") or (request.json or {}).get("number", ""))
    name = (request.form.get("name", "") or (request.json or {}).get("name", "")).strip()
    if not n:
        return jsonify({"ok": False, "error": "Invalid number"}), 400
    with numbers_lock:
        added, removed, paused = _load_local()
        added.add(n)
        removed.discard(n)
        paused.discard(n)
        _save_local(added, removed, paused)
    if name:
        set_name(n, name)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

@app.route("/numbers/remove", methods=["POST"])
@login_required
def numbers_remove():
    n = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    if not n:
        return jsonify({"ok": False})
    with numbers_lock:
        added, removed, paused = _load_local()
        added.discard(n)
        removed.add(n)
        paused.discard(n)
        _save_local(added, removed, paused)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

@app.route("/numbers/pause", methods=["POST"])
@login_required
def numbers_pause():
    n = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    if n:
        with numbers_lock:
            added, removed, paused = _load_local()
            paused.add(n)
            _save_local(added, removed, paused)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

@app.route("/numbers/unpause", methods=["POST"])
@login_required
def numbers_unpause():
    n = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    if n:
        with numbers_lock:
            added, removed, paused = _load_local()
            paused.discard(n)
            _save_local(added, removed, paused)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

@app.route("/numbers/setname", methods=["POST"])
@login_required
def numbers_setname():
    n    = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    name = (request.form.get("name", "") or (request.json or {}).get("name", "")).strip()
    if n:
        set_name(n, name)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

# ── Reading toggle ────────────────────────────────────────────────────────────

@app.route("/reading/toggle", methods=["POST"])
@login_required
def reading_toggle():
    current = get_reading_enabled()
    set_reading_enabled(not current)
    return jsonify({"ok": True, "value": get_reading_enabled()})

# ── Book management ───────────────────────────────────────────────────────────

@app.route("/book/upload", methods=["POST"])
@login_required
def book_upload():
    f = request.files.get("book")
    if not f:
        return jsonify({"ok": False, "error": "No file"}), 400
    title = request.form.get("title", f.filename).strip()
    lpp   = int(request.form.get("lines_per_portion", 30))
    text  = f.read().decode("utf-8", errors="replace")
    count = upload_book(text, title=title, lines_per_portion=lpp)
    print(f"Book '{title}' uploaded — {count} portions")
    return jsonify({"ok": True, "count": count})

@app.route("/book/advance", methods=["POST"])
@login_required
def book_advance():
    advance_reading()
    return jsonify({"ok": True})

@app.route("/book/remove", methods=["POST"])
@login_required
def book_remove():
    with book_lock:
        save_book({"portions": [], "current_index": 0, "title": ""})
    return jsonify({"ok": True})

# ── Status page ───────────────────────────────────────────────────────────────

STATUS_ICONS_PY = {
    "connected":  ("✅", "#4ade80"), "voicemail":  ("📵", "#fb923c"),
    "dialing":    ("⏳", "#facc15"), "busy":       ("🔴", "#f87171"),
    "unanswered": ("🔕", "#94a3b8"), "timeout":    ("🔕", "#94a3b8"),
    "failed":     ("❌", "#f87171"), "error":      ("❌", "#f87171"),
}

@app.route("/history")
@login_required
def history():
    calls = get_call_history_db(limit=200)
    STATUS_ICONS = {
        "connected":  ("✅", "#4ade80"), "voicemail": ("📵", "#fb923c"),
        "dialing":    ("⏳", "#facc15"), "busy":      ("🔴", "#f87171"),
        "unanswered": ("🔕", "#94a3b8"), "timeout":   ("🔕", "#94a3b8"),
        "failed":     ("❌", "#f87171"), "error":     ("❌", "#f87171"),
    }
    rows = ""
    for c in calls:
        s = c.get("status", "unknown")
        icon, color = STATUS_ICONS.get(s, ("❓", "#94a3b8"))
        name = c.get("name", "")
        try:
            dt = c["run_time_et"]
            time_str = dt.strftime("%-m/%-d %I:%M %p") if hasattr(dt, "strftime") else str(dt)
        except Exception:
            time_str = ""
        rows += f"""<tr>
            <td>{time_str}</td>
            <td style='font-family:monospace'>{c['number']}</td>
            <td>{name}</td>
            <td style='color:{color}'>{icon} {s}</td>
        </tr>"""
    if not rows:
        rows = "<tr><td colspan='4' style='color:#64748b;text-align:center;padding:2rem'>No call history yet.</td></tr>"
    return f"""<!DOCTYPE html><html lang='en'><head>
  <meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Call History — Conference Manager</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0a0e1a;color:#e2e8f0;padding:1.5rem 1rem 3rem;min-height:100vh}}
    .wrap{{max-width:720px;margin:0 auto}}
    h1{{font-size:1.3rem;font-weight:700;margin-bottom:1.25rem;display:flex;align-items:center;gap:.5rem}}
    a{{color:#6366f1;text-decoration:none;font-size:.85rem}}
    .card{{background:#111827;border:1px solid #1f2937;border-radius:14px;overflow:hidden}}
    table{{width:100%;border-collapse:collapse;font-size:.85rem}}
    th{{text-align:left;padding:.65rem 1rem;color:#4b5563;font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #1f2937;background:#0d1421}}
    td{{padding:.6rem 1rem;border-bottom:1px solid #111827}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:#0d1421}}
  </style></head><body>
  <div class='wrap'>
    <div style='display:flex;align-items:center;justify-content:space-between;margin-bottom:1.25rem'>
      <h1>📋 Call History</h1><a href='/status'>← Back to Dashboard</a>
    </div>
    <div class='card'>
      <table><thead><tr><th>Time (ET)</th><th>Number</th><th>Name</th><th>Status</th></tr></thead>
      <tbody>{rows}</tbody></table>
    </div>
  </div></body></html>"""

@app.route("/download-code")
@login_required
def download_code():
    import zipfile, io
    buf = io.BytesIO()
    skip_dirs = {"__pycache__", ".pythonlibs", "recordings", ".replit-artifact"}
    base = os.path.dirname(os.path.abspath(__file__))
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for f in files:
                if f.endswith((".pyc", ".zip")):
                    continue
                full = os.path.join(root, f)
                z.write(full, os.path.relpath(full, base))
    buf.seek(0)
    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name="conference-manager.zip")

@app.route("/api/state")
@login_required
def api_state():
    with log_lock:
        run_time      = last_run["time"]
        calls         = list(last_run["calls"])
        inbound_calls = list(last_run["inbound_calls"])
        running       = last_run["running"]
    with vote_lock:
        voted     = len(reading_session["votes"])
        expected  = len(reading_session["expected"])
        triggered = reading_session["triggered"]
    rec_meta   = load_recording_meta()
    rec_exists = os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3"))
    book = load_book()
    spreadsheet_id = get_spreadsheet_id()
    sheets_msg = session.pop("sheets_msg", "")
    sheets_ok  = session.pop("sheets_ok", True)
    return jsonify({
        "running":               running,
        "run_time":              run_time,
        "calls":                 calls,
        "inbound_calls":         inbound_calls,
        "numbers":               get_all_numbers_with_source(),
        "schedule":              load_schedule(),
        "reading_enabled":       get_reading_enabled(),
        "record_enabled":        get_record_enabled(),
        "replay_enabled":        get_replay_enabled(),
        "announcements_enabled": get_announcements_enabled(),
        "voted":                 voted,
        "expected":              expected,
        "triggered":             triggered,
        "rec_meta":              rec_meta,
        "rec_exists":            rec_exists,
        "book_title":            book.get("title", ""),
        "book_total":            len(book.get("portions", [])),
        "book_index":            book.get("current_index", 0),
        "spreadsheet_id":        spreadsheet_id,
        "sheets_msg":            sheets_msg,
        "sheets_ok":             sheets_ok,
    })

@app.route("/status")
@login_required
def status():
    raw = FROM_NUMBER.lstrip("1") if FROM_NUMBER.startswith("1") else FROM_NUMBER
    dial_in_fmt = (f"({raw[0:3]}) {raw[3:6]}-{raw[6:10]}" if len(raw) >= 10 else FROM_NUMBER)
    return f"""<!DOCTYPE html>
<html lang='en'><head>
  <meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Conference Manager</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#0a0f1e'/>
  <meta name='apple-mobile-web-app-capable' content='yes'/>
  <meta name='apple-mobile-web-app-status-bar-style' content='black-translucent'/>
  <link rel='preconnect' href='https://fonts.googleapis.com'/>
  <link rel='preconnect' href='https://fonts.gstatic.com' crossorigin/>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap' rel='stylesheet'/>
  <style>
    :root {{
      --bg:        #0a0f1e;
      --surface:   #111827;
      --surface2:  #1a2235;
      --border:    #1f2d45;
      --border2:   #2a3a55;
      --text:      #f0f4ff;
      --text2:     #8899bb;
      --text3:     #4a5f80;
      --blue:      #3b82f6;
      --blue-dark: #1d4ed8;
      --blue-glow: rgba(59,130,246,.15);
      --green:     #22c55e;
      --green-dim: #14532d;
      --orange:    #f97316;
      --orange-dim:#431407;
      --red:       #ef4444;
      --red-dim:   #450a0a;
      --purple:    #a78bfa;
      --yellow:    #fbbf24;
      --radius:    12px;
      --radius-sm: 8px;
      --shadow:    0 4px 24px rgba(0,0,0,.4);
    }}
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding:0 0 5rem}}
    a{{color:var(--blue);text-decoration:none}}
    a:hover{{text-decoration:underline}}

    /* ── Layout ── */
    .topbar{{background:var(--surface);border-bottom:1px solid var(--border);padding:.85rem 1.5rem;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;backdrop-filter:blur(12px)}}
    .topbar-brand{{display:flex;align-items:center;gap:.6rem}}
    .topbar-icon{{width:28px;height:28px;background:linear-gradient(135deg,#3b82f6,#6366f1);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:.9rem}}
    .topbar-title{{font-size:1rem;font-weight:700;color:var(--text)}}
    .topbar-right{{display:flex;align-items:center;gap:.75rem}}
    .signout-btn{{background:none;border:1px solid var(--border2);color:var(--text2);border-radius:var(--radius-sm);padding:.35rem .85rem;font-size:.78rem;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif;transition:all .15s}}
    .signout-btn:hover{{border-color:var(--blue);color:var(--text)}}

    .page{{max-width:640px;margin:0 auto;padding:1.5rem 1rem}}
    .grid{{display:flex;flex-direction:column;gap:1.25rem}}

    /* ── Cards ── */
    .card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:1.25rem 1.25rem 1rem;box-shadow:var(--shadow)}}
    .card-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem}}
    .card-title{{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text3)}}
    .card-badge{{font-size:.72rem;font-weight:700;padding:.2rem .55rem;border-radius:999px}}
    .badge-green{{background:rgba(34,197,94,.15);color:var(--green)}}
    .badge-orange{{background:rgba(249,115,22,.15);color:var(--orange)}}
    .badge-blue{{background:rgba(59,130,246,.15);color:var(--blue)}}

    /* ── Trigger ── */
    .trigger-btn{{width:100%;padding:1rem;background:linear-gradient(135deg,#2563eb,#4f46e5);color:#fff;border:none;border-radius:var(--radius);font-size:1rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;letter-spacing:.02em;transition:all .2s;box-shadow:0 4px 20px rgba(59,130,246,.3)}}
    .trigger-btn:hover:not([disabled]){{transform:translateY(-1px);box-shadow:0 6px 28px rgba(59,130,246,.45)}}
    .trigger-btn[disabled]{{background:linear-gradient(135deg,#1e3a5f,#2a2f6e);color:var(--text3);cursor:not-allowed;box-shadow:none;transform:none}}
    .live-dot{{display:inline-block;width:8px;height:8px;background:var(--green);border-radius:50%;margin-right:.4rem;animation:pulse 1.5s infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}

    /* ── Dial-in ── */
    .dialin-box{{background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:.85rem 1rem;display:flex;align-items:center;justify-content:space-between;gap:.5rem}}
    .dialin-label{{font-size:.8rem;color:var(--text2)}}
    .dialin-num{{font-family:'Inter',sans-serif;font-size:1.1rem;font-weight:700;color:var(--text);letter-spacing:.06em}}

    /* ── Last run ── */
    .run-meta{{display:flex;justify-content:space-between;align-items:center;font-size:.8rem;color:var(--text2);margin-bottom:.75rem;padding:.6rem .85rem;background:var(--surface2);border-radius:var(--radius-sm);border:1px solid var(--border)}}
    .run-counts{{font-weight:700;color:var(--green)}}
    .calls-list{{display:flex;flex-direction:column;gap:.35rem}}
    .call-row{{display:flex;align-items:center;gap:.6rem;padding:.55rem .85rem;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);font-size:.83rem}}
    .call-icon{{min-width:1.1rem;text-align:center}}
    .call-num{{font-family:'Inter',sans-serif;font-weight:600;color:var(--text)}}
    .call-name{{color:var(--text2);font-size:.78rem;flex:1}}
    .call-stat{{font-weight:600;font-size:.78rem;text-transform:capitalize}}
    .call-type-tag{{font-size:.68rem;font-weight:700;padding:.15rem .5rem;border-radius:999px;margin-left:auto;white-space:nowrap}}
    .tag-dialed{{background:rgba(59,130,246,.15);color:#7dd3fc}}
    .tag-live{{background:rgba(34,197,94,.12);color:#6ee7b7}}
    .tag-replay{{background:rgba(167,139,250,.12);color:#c4b5fd}}

    /* ── Numbers ── */
    .num-list{{display:flex;flex-direction:column;gap:.35rem}}
    .section-label{{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;padding:.35rem 0 .2rem;display:flex;align-items:center;gap:.4rem}}
    .section-label.active-label{{color:var(--green)}}
    .section-label.paused-label{{color:var(--orange)}}
    .num-row{{display:flex;align-items:center;gap:.5rem;padding:.6rem .85rem;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);transition:border-color .15s}}
    .num-row:hover{{border-color:var(--border2)}}
    .num-row.is-paused{{opacity:.6;border-style:dashed}}
    .num-main{{flex:1;min-width:0}}
    .num-phone{{font-family:'Inter',sans-serif;font-weight:600;font-size:.85rem;color:var(--text)}}
    .num-name-display{{font-size:.75rem;color:var(--text2);margin-top:.1rem}}
    .tag{{font-size:.65rem;font-weight:700;padding:.1rem .38rem;border-radius:4px;letter-spacing:.03em;margin-left:.3rem}}
    .tag-sheet{{background:rgba(59,130,246,.15);color:#7dd3fc}}
    .tag-paused-small{{background:rgba(249,115,22,.15);color:var(--orange)}}
    .num-actions{{display:flex;align-items:center;gap:.35rem;flex-shrink:0}}
    .name-inp{{background:var(--bg);border:1px solid var(--border2);color:var(--text);border-radius:6px;padding:.28rem .55rem;font-size:.78rem;font-family:'Inter',sans-serif;width:90px}}
    .name-inp:focus{{outline:none;border-color:var(--blue)}}
    .btn-save{{background:rgba(59,130,246,.15);color:var(--blue);border:1px solid rgba(59,130,246,.3);border-radius:6px;padding:.28rem .6rem;font-size:.75rem;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .btn-save:hover{{background:rgba(59,130,246,.25)}}
    .btn-pause{{background:rgba(249,115,22,.1);color:var(--orange);border:1px solid rgba(249,115,22,.25);border-radius:6px;padding:.28rem .6rem;font-size:.75rem;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .btn-pause:hover{{background:rgba(249,115,22,.2)}}
    .btn-resume{{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.25);border-radius:6px;padding:.28rem .6rem;font-size:.75rem;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .btn-resume:hover{{background:rgba(34,197,94,.2)}}
    .btn-remove{{background:rgba(239,68,68,.1);color:var(--red);border:1px solid rgba(239,68,68,.2);border-radius:6px;padding:.28rem .5rem;font-size:.75rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-remove:hover{{background:rgba(239,68,68,.2)}}
    .add-row{{display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:.85rem}}
    .add-inp{{flex:1;min-width:100px;background:var(--surface2);border:1px solid var(--border2);color:var(--text);border-radius:var(--radius-sm);padding:.6rem .85rem;font-size:.85rem;font-family:'Inter',sans-serif}}
    .add-inp:focus{{outline:none;border-color:var(--blue)}}
    .btn-add{{background:linear-gradient(135deg,#15803d,#16a34a);color:#fff;border:none;border-radius:var(--radius-sm);padding:.6rem 1.1rem;font-size:.85rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .btn-add:hover{{background:linear-gradient(135deg,#16a34a,#22c55e)}}

    /* ── Schedule spinners ── */
    .day-grid{{display:flex;flex-direction:column;gap:.45rem}}
    .day-row{{display:flex;align-items:center;gap:.6rem;padding:.6rem .85rem;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);flex-wrap:wrap;transition:border-color .2s}}
    .day-row.active{{border-color:var(--blue);background:rgba(59,130,246,.05)}}
    .day-name{{min-width:84px;font-size:.85rem;font-weight:600;color:var(--text)}}
    .day-form{{display:flex;align-items:center;gap:.4rem;flex:1;flex-wrap:wrap}}
    .spinner-wrap{{display:flex;flex-direction:column;align-items:center;gap:1px}}
    .spinner-btn{{background:none;border:none;color:var(--text3);font-size:.65rem;line-height:1;cursor:pointer;padding:.06rem .5rem;font-family:'Inter',sans-serif;transition:color .1s}}
    .spinner-btn:hover{{color:var(--text)}}
    .spinner-val{{background:var(--bg);border:1px solid var(--border2);color:var(--text);border-radius:6px;padding:.28rem 0;font-size:.95rem;font-weight:700;text-align:center;width:2.4rem;font-family:'Inter',sans-serif}}
    .sep{{color:var(--text3);font-size:1rem;font-weight:700;padding:0 .05rem}}
    .ampm-group{{display:flex;border:1px solid var(--border2);border-radius:6px;overflow:hidden}}
    .ampm-opt{{background:var(--bg);color:var(--text3);border:none;padding:.28rem .55rem;font-size:.8rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;transition:all .15s}}
    .ampm-opt.selected{{background:var(--blue);color:#fff}}
    .ampm-opt:hover:not(.selected){{color:var(--text)}}
    .btn-set{{background:linear-gradient(135deg,var(--blue-dark),var(--blue));color:#fff;border:none;border-radius:6px;padding:.32rem .85rem;font-size:.8rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .btn-set:hover{{opacity:.9}}
    .btn-clear-day{{background:none;border:none;color:var(--red);font-size:1rem;cursor:pointer;padding:.1rem .3rem;line-height:1;opacity:.7}}
    .btn-clear-day:hover{{opacity:1}}
    .sched-hint{{font-size:.74rem;color:var(--text3);margin-top:.5rem}}

    /* ── Toggles ── */
    .toggle-row{{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap;padding:.5rem 0}}
    .toggle-row+.toggle-row{{border-top:1px solid var(--border)}}
    .toggle-btn{{border:none;border-radius:20px;padding:.38rem 1rem;font-size:.82rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;transition:all .2s;white-space:nowrap}}
    .toggle-on{{background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3)}}
    .toggle-on:hover{{background:rgba(34,197,94,.25)}}
    .toggle-off{{background:var(--surface2);color:var(--text3);border:1px solid var(--border2)}}
    .toggle-off:hover{{border-color:var(--blue);color:var(--text)}}
    .toggle-hint{{font-size:.76rem;color:var(--text3);line-height:1.4;flex:1}}

    /* ── Hangup ── */
    .hangup-all-row{{display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:.75rem}}
    .btn-hangup-all{{flex:1;padding:.65rem;background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3);border-radius:var(--radius-sm);font-size:.85rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;transition:all .15s}}
    .btn-hangup-all:hover{{background:rgba(239,68,68,.25)}}
    .btn-hangup-block{{flex:1;padding:.65rem;background:rgba(249,115,22,.12);color:var(--orange);border:1px solid rgba(249,115,22,.25);border-radius:var(--radius-sm);font-size:.85rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;transition:all .15s}}
    .btn-hangup-block:hover{{background:rgba(249,115,22,.22)}}
    .live-row{{display:flex;align-items:center;justify-content:space-between;gap:.5rem;padding:.55rem .85rem;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);font-size:.83rem;margin-bottom:.35rem}}
    .live-info{{flex:1;min-width:0}}
    .live-actions{{display:flex;gap:.35rem}}
    .btn-hup{{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.25);border-radius:6px;padding:.25rem .6rem;font-size:.75rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-hup:hover{{background:rgba(239,68,68,.25)}}
    .btn-hup-block{{background:rgba(249,115,22,.1);color:var(--orange);border:1px solid rgba(249,115,22,.2);border-radius:6px;padding:.25rem .6rem;font-size:.75rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-hup-block:hover{{background:rgba(249,115,22,.2)}}

    /* ── Recording / Book ── */
    .rec-meta{{font-size:.78rem;color:var(--text2);margin:.5rem 0 0;padding:.5rem .75rem;background:var(--surface2);border-radius:var(--radius-sm);border:1px solid var(--border)}}
    .btn-dl{{display:inline-flex;align-items:center;gap:.35rem;background:rgba(59,130,246,.12);color:var(--blue);border:1px solid rgba(59,130,246,.25);border-radius:var(--radius-sm);padding:.4rem .85rem;font-size:.8rem;font-weight:600;font-family:'Inter',sans-serif;cursor:pointer;text-decoration:none;margin-top:.5rem}}
    .btn-dl:hover{{background:rgba(59,130,246,.2);text-decoration:none}}
    .book-info-row{{display:flex;justify-content:space-between;align-items:center;padding:.5rem .75rem;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);font-size:.83rem;margin-bottom:.5rem}}
    .book-actions{{display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:.5rem}}
    .btn-sec{{background:var(--surface2);color:var(--text);border:1px solid var(--border2);border-radius:var(--radius-sm);padding:.45rem .9rem;font-size:.82rem;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-sec:hover{{border-color:var(--blue);color:var(--blue)}}
    details summary{{cursor:pointer;font-size:.82rem;color:var(--blue);font-weight:600;user-select:none;padding:.3rem 0}}
    details summary:hover{{color:#60a5fa}}
    .upload-form{{display:flex;flex-direction:column;gap:.6rem;margin-top:.75rem;padding:.85rem;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm)}}
    .upload-form input{{background:var(--bg);border:1px solid var(--border2);color:var(--text);border-radius:var(--radius-sm);padding:.55rem .85rem;font-size:.82rem;font-family:'Inter',sans-serif;width:100%}}
    .upload-form input[type=file]{{color:var(--text2)}}
    .upload-form input:focus{{outline:none;border-color:var(--blue)}}
    .btn-upload{{background:linear-gradient(135deg,#4f46e5,#7c3aed);color:#fff;border:none;border-radius:var(--radius-sm);padding:.6rem 1rem;font-size:.85rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .hint{{font-size:.73rem;color:var(--text3);line-height:1.5}}

    /* ── Sheets ── */
    .sheets-row{{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap}}
    .btn-sync{{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.25);border-radius:var(--radius-sm);padding:.45rem 1rem;font-size:.82rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-sync:hover{{background:rgba(34,197,94,.2)}}
    .sheets-msg-ok{{color:var(--green);font-size:.78rem}}
    .sheets-msg-err{{color:var(--red);font-size:.78rem}}

    /* ── Footer ── */
    .footer-links{{display:flex;justify-content:center;gap:1.25rem;padding:1rem 0;font-size:.76rem;color:var(--text3)}}
    .footer-links a{{color:var(--text3);transition:color .15s}}
    .footer-links a:hover{{color:var(--blue);text-decoration:none}}

    /* ── Toast ── */
    .toast{{position:fixed;bottom:1.75rem;left:50%;transform:translateX(-50%);background:var(--surface);border:1px solid var(--border2);color:var(--text);padding:.6rem 1.4rem;border-radius:999px;font-size:.83rem;font-weight:500;opacity:0;transition:opacity .25s;pointer-events:none;z-index:999;box-shadow:var(--shadow);white-space:nowrap}}
    .toast.show{{opacity:1}}

    /* ── Install banner ── */
    #install-banner{{display:none;align-items:center;justify-content:space-between;gap:.75rem;background:rgba(59,130,246,.08);border:1px solid rgba(59,130,246,.2);border-radius:var(--radius-sm);padding:.65rem 1rem;font-size:.82rem;margin-bottom:.75rem}}
    #install-banner span{{color:var(--text2)}}
    .btn-install{{background:var(--blue);color:#fff;border:none;border-radius:6px;padding:.38rem .9rem;font-size:.82rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-dismiss{{background:none;border:none;color:var(--text3);cursor:pointer;font-size:1rem;padding:.2rem .4rem}}
  </style>
</head>
<body>

<!-- Top Bar -->
<div class='topbar'>
  <div class='topbar-brand'>
    <div class='topbar-icon'>📞</div>
    <span class='topbar-title'>Conference Manager</span>
  </div>
  <div class='topbar-right'>
    <form method='POST' action='/logout' style='margin:0'>
      <button class='signout-btn'>Sign out</button>
    </form>
  </div>
</div>

<div class='page'>
  <div class='grid'>

    <!-- Install Banner -->
    <div id='install-banner'>
      <span>📱 Install this app on your device for quick access.</span>
      <div style='display:flex;gap:.4rem;align-items:center'>
        <button class='btn-install' id='install-btn'>Install</button>
        <button class='btn-dismiss' id='dismiss-btn'>✕</button>
      </div>
    </div>

    <!-- START CONFERENCE -->
    <div class='card'>
      <button class='trigger-btn' id='trigger-btn' onclick='triggerConference()'>▶ &nbsp;Start Conference Now</button>
    </div>

    <!-- HANGUP (always visible) -->
    <div class='card' id='hangup-section'>
      <div class='card-header'><span class='card-title'>🔴 &nbsp;Active Call Controls</span></div>
      <div id='hangup-controls'><p style='color:var(--text3);font-size:.83rem'>No active calls right now.</p></div>
    </div>

    <!-- DIAL-IN -->
    <div class='card'>
      <div class='card-header'><span class='card-title'>Dial-In Number</span></div>
      <div class='dialin-box'>
        <span class='dialin-label'>Members call in directly:</span>
        <span class='dialin-num'>{dial_in_fmt}</span>
      </div>
    </div>

    <!-- LAST CONFERENCE -->
    <div class='card'>
      <div class='card-header'>
        <span class='card-title'>Last Conference</span>
        <a href='/history' style='font-size:.75rem;color:var(--blue)'>Full history →</a>
      </div>
      <div id='last-run'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
    </div>

    <!-- PHONE NUMBERS -->
    <div class='card'>
      <div class='card-header'>
        <span class='card-title'>Phone Numbers</span>
        <span class='card-badge badge-blue' id='num-count'>0</span>
      </div>
      <div class='add-row'>
        <input type='tel'  class='add-inp' id='new-number' placeholder='Number e.g. 2025551234'/>
        <input type='text' class='add-inp' id='new-name'   placeholder='Name (optional)'/>
        <button class='btn-add' onclick='addNumber()'>+ Add</button>
      </div>
      <div class='num-list' id='numbers-list'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
    </div>

    <!-- SCHEDULE -->
    <div class='card'>
      <div class='card-header'><span class='card-title'>Schedule</span></div>
      <div class='day-grid' id='day-grid'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
      <p class='sched-hint'>All times are Eastern (ET). Press Set to save a time.</p>
    </div>

    <!-- DAILY READING -->
    <div class='card'>
      <div class='card-header'><span class='card-title'>Daily Reading</span></div>
      <div id='reading-section'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
      <details id='book-upload-details' style='margin-top:.75rem'>
        <summary id='book-upload-summary'>Upload a book (.txt)</summary>
        <form id='book-upload-form' class='upload-form'>
          <input type='file'   name='book'              accept='.txt' required/>
          <input type='text'   name='title'             placeholder='Book title (optional)'/>
          <input type='number' name='lines_per_portion' value='30' min='5' max='200'/>
          <p class='hint'>Upload a plain .txt file. Participants vote by pressing 1 to hear it read aloud.</p>
          <button type='button' class='btn-upload' onclick='uploadBook()'>Upload &amp; Split</button>
        </form>
      </details>
    </div>

    <!-- RECORDING -->
    <div class='card'>
      <div class='card-header'><span class='card-title'>Recording</span></div>
      <div id='recording-section'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
    </div>

    <!-- JOIN ANNOUNCEMENTS -->
    <div class='card'>
      <div class='card-header'><span class='card-title'>Join Announcements</span></div>
      <div id='announcements-section'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
    </div>

    <!-- GOOGLE SHEETS -->
    <div class='card'>
      <div class='card-header'><span class='card-title'>Google Sheets Sync</span></div>
      <p style='font-size:.78rem;color:var(--text2);margin-bottom:.75rem'>Numbers sync automatically on startup. Column A&nbsp;=&nbsp;name, Column B&nbsp;=&nbsp;number, row&nbsp;1&nbsp;=&nbsp;header.</p>
      <div id='sheets-section'><p style='color:var(--text3);font-size:.85rem'>Loading...</p></div>
    </div>

  </div><!-- /grid -->

  <!-- Footer -->
  <div class='footer-links'>
    <a href='/history'>📋 Call History</a>
    <a href='/download-code'>⬇ Download Code</a>
  </div>

</div><!-- /page -->

<div class='toast' id='toast'></div>

<script>
const DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
const STATUS_ICONS = {{
  connected:["✅","#22c55e"], voicemail:["📵","#f97316"],
  dialing:["⏳","#fbbf24"],   busy:["🔴","#ef4444"],
  unanswered:["🔕","#8899bb"],timeout:["🔕","#8899bb"],
  failed:["❌","#ef4444"],    error:["❌","#ef4444"],
}};

function toast(msg, dur=2400) {{
  const t = document.getElementById("toast");
  t.textContent=msg; t.classList.add("show");
  setTimeout(()=>t.classList.remove("show"), dur);
}}

async function post(url, data={{}}, isForm=false) {{
  const opts = isForm
    ? {{method:"POST", body:data}}
    : {{method:"POST", headers:{{"Content-Type":"application/json"}}, body:JSON.stringify(data)}};
  const r = await fetch(url, opts);
  return r.json().catch(()=>({{ok:false}}));
}}

// ── Last Run ────────────────────────────────────────────────────────────────
function renderLastRun(s) {{
  const el  = document.getElementById("last-run");
  const btn = document.getElementById("trigger-btn");
  if (s.running) {{ btn.disabled=true; btn.innerHTML='<span class="live-dot"></span>Conference in Progress'; }}
  else           {{ btn.disabled=false; btn.innerHTML="▶ &nbsp;Start Conference Now"; }}
  if (!s.run_time) {{
    el.innerHTML='<p style="color:var(--text3);font-size:.85rem">No conference has run yet since the server started.</p>';
    return;
  }}
  const badge = s.running ? '<span style="color:var(--green);font-size:.75rem;font-weight:700">● Live</span>' : '';
  const allCalls = [...(s.calls||[])];
  const connected = allCalls.filter(c=>c.status==="connected").length;
  const rows = (s.calls||[]).map(c=>{{
    const [icon,color]=STATUS_ICONS[c.status]||["❓","#8899bb"];
    const name = c.name ? `<span class="call-name">${{c.name}}</span>` : '';
    return `<div class="call-row"><span class="call-icon">${{icon}}</span><span class="call-num">${{c.number}}</span>${{name}}<span class="call-stat" style="color:${{color}}">${{c.status}}</span><span class="call-type-tag tag-dialed">Dialed</span></div>`;
  }}).join("")+(s.inbound_calls||[]).map(c=>{{
    const src=c.source||"inbound-live";
    const name=c.name?`<span class="call-name">${{c.name}}</span>`:'';
    const [icon,color,cls]=src==="inbound-replay"?["🎧","#a78bfa","tag-replay"]:["📲","#38bdf8","tag-live"];
    return `<div class="call-row"><span class="call-icon">${{icon}}</span><span class="call-num">${{c.number}}</span>${{name}}<span class="call-stat" style="color:${{color}}">connected</span><span class="call-type-tag ${{cls}}">${{src==="inbound-replay"?"Replay":"Called in"}}</span></div>`;
  }}).join("");
  el.innerHTML=`<div class="run-meta"><span style="color:var(--text2)">${{s.run_time}} ${{badge}}</span><span class="run-counts">${{connected}}/${{(s.calls||[]).length}} connected</span></div><div class="calls-list">${{rows}}</div>`;
}}

// ── Numbers ─────────────────────────────────────────────────────────────────
function renderNumbers(numbers) {{
  document.getElementById("num-count").textContent = numbers.length;
  const el = document.getElementById("numbers-list");
  if (!numbers.length) {{
    el.innerHTML='<p style="color:var(--text3);font-size:.83rem;padding:.25rem 0">No numbers yet. Add one above.</p>';
    return;
  }}
  const active = numbers.filter(r=>!r[3]);
  const paused = numbers.filter(r=>r[3]);
  function row([n,src,name,isPaused]) {{
    const srcTag  = src==="sheet"?`<span class="tag tag-sheet">Sheet</span>`:'';
    const pauseTag= isPaused?`<span class="tag tag-paused-small">Paused</span>`:'';
    const disp    = name||'<span style="color:var(--text3);font-size:.75rem">No name</span>';
    return `<div class="num-row${{isPaused?' is-paused':''}}" >
      <div class="num-main">
        <div style="display:flex;align-items:center;gap:.3rem">
          <span class="num-phone">${{n}}</span>${{srcTag}}${{pauseTag}}
        </div>
        <div class="num-name-display">${{disp}}</div>
      </div>
      <div class="num-actions">
        <input type="text" class="name-inp" value="${{name}}" placeholder="Name" id="name-${{n}}"/>
        <button class="btn-save" onclick="saveName('${{n}}')">Save</button>
        <button class="${{isPaused?'btn-resume':'btn-pause'}}" onclick="togglePause('${{n}}',${{isPaused}})">${{isPaused?'Resume':'Pause'}}</button>
        <button class="btn-remove" onclick="removeNumber('${{n}}')">✕</button>
      </div>
    </div>`;
  }}
  let html='';
  if (active.length) html+=`<div class="section-label active-label">✅ Will be called (${{active.length}})</div>`+active.map(row).join('');
  if (paused.length) html+=`<div class="section-label paused-label" style="margin-top:.5rem">⏸ Paused — skipped on next call (${{paused.length}})</div>`+paused.map(row).join('');
  el.innerHTML=html;
}}

// ── Schedule Spinners ────────────────────────────────────────────────────────
const spinState = Array.from({{length:7}},()=>({{h:12,m:0,ampm:"AM"}}));
function to24(s){{let h=s.h%12;if(s.ampm==="PM")h+=12;return h;}}
function loadSpinState(day,h24,m){{
  const s=spinState[day]; s.m=m;
  if(h24===0){{s.h=12;s.ampm="AM";}}
  else if(h24<12){{s.h=h24;s.ampm="AM";}}
  else if(h24===12){{s.h=12;s.ampm="PM";}}
  else{{s.h=h24-12;s.ampm="PM";}}
}}
function updateSpinDisplay(day){{
  const s=spinState[day];
  const hEl=document.getElementById(`sh-${{day}}`);
  const mEl=document.getElementById(`sm-${{day}}`);
  if(hEl)hEl.textContent=String(s.h).padStart(2,"0");
  if(mEl)mEl.textContent=String(s.m).padStart(2,"0");
  ["AM","PM"].forEach(v=>{{
    const el=document.getElementById(`ampm-${{day}}-${{v}}`);
    if(el)el.className="ampm-opt"+(s.ampm===v?" selected":"");
  }});
}}
function spinH(day,d){{const s=spinState[day];s.h=(s.h-1+d+12)%12+1;updateSpinDisplay(day);}}
function spinM(day,d){{const s=spinState[day];s.m=((s.m+d)+60)%60;updateSpinDisplay(day);}}
function setAmpm(day,v){{spinState[day].ampm=v;updateSpinDisplay(day);}}
function spinnerHTML(day){{
  const s=spinState[day];
  return `
    <div class="spinner-wrap">
      <button class="spinner-btn" onclick="spinH(${{day}},1)">▲</button>
      <div class="spinner-val" id="sh-${{day}}">${{String(s.h).padStart(2,"0")}}</div>
      <button class="spinner-btn" onclick="spinH(${{day}},-1)">▼</button>
    </div>
    <span class="sep">:</span>
    <div class="spinner-wrap">
      <button class="spinner-btn" onclick="spinM(${{day}},1)">▲</button>
      <div class="spinner-val" id="sm-${{day}}">${{String(s.m).padStart(2,"0")}}</div>
      <button class="spinner-btn" onclick="spinM(${{day}},-1)">▼</button>
    </div>
    <div class="ampm-group">
      <button class="ampm-opt${{s.ampm==="AM"?" selected":""}}" id="ampm-${{day}}-AM" onclick="setAmpm(${{day}},'AM')">AM</button>
      <button class="ampm-opt${{s.ampm==="PM"?" selected":""}}" id="ampm-${{day}}-PM" onclick="setAmpm(${{day}},'PM')">PM</button>
    </div>`;
}}
function renderSchedule(schedule){{
  const grid=document.getElementById("day-grid");
  const byDay={{}};
  schedule.forEach(e=>{{if(!(e.day in byDay))byDay[e.day]=e;}});
  DAYS.forEach((_,i)=>{{
    if(byDay[i])loadSpinState(i,byDay[i].hour,byDay[i].minute);
    else{{spinState[i].h=12;spinState[i].m=0;spinState[i].ampm="AM";}}
  }});
  grid.innerHTML=DAYS.map((dayName,i)=>{{
    const isSet=!!byDay[i];
    const rowCls=isSet?"day-row active":"day-row";
    const setLbl=isSet?"Update":"Set";
    const clearBtn=isSet?`<button class="btn-clear-day" onclick="clearDay(${{i}},'${{dayName}}')">✕</button>`:"";
    return `<div class="${{rowCls}}" id="day-row-${{i}}">
      <span class="day-name">${{dayName}}</span>
      <div class="day-form">
        ${{spinnerHTML(i)}}
        <button class="btn-set" onclick="setDay(${{i}})">${{setLbl}}</button>
        ${{clearBtn}}
      </div>
    </div>`;
  }}).join("");
}}

// ── Reading ──────────────────────────────────────────────────────────────────
function renderReading(s){{
  const el=document.getElementById("reading-section");
  const on=s.reading_enabled;
  const hint=on?"Participants press 1 to vote. Plays if all vote yes.":"Enable to read a daily portion aloud on the call.";
  let body=`<div class="toggle-row"><button class="toggle-btn ${{on?'toggle-on':'toggle-off'}}" onclick="toggleReading()">${{on?"Auto-Read: On":"Auto-Read: Off"}}</button><span class="toggle-hint">${{hint}}</span></div>`;
  if(!s.book_total){{
    body+='<p style="color:var(--text3);font-size:.8rem;margin-top:.6rem">No book uploaded yet.</p>';
    document.getElementById("book-upload-summary").textContent="Upload a book (.txt)";
  }}else{{
    let vbadge="";
    if(on&&s.expected>0)vbadge=s.triggered?'<span style="color:var(--green);font-size:.78rem">📖 Reading played this session</span>':`<span style="color:var(--purple);font-size:.78rem">📖 ${{s.voted}}/${{s.expected}} voted</span>`;
    body+=`<div class="book-info-row" style="margin-top:.65rem"><span style="font-weight:700">${{s.book_title||"Untitled"}}</span><span style="color:var(--text2);font-size:.78rem">Portion ${{s.book_index+1}} of ${{s.book_total}}</span></div>
    ${{vbadge?`<div style="margin:.3rem 0">${{vbadge}}</div>`:""}}
    <div class="book-actions"><button class="btn-sec" onclick="bookAdvance()">Skip to Next</button><button class="btn-remove" style="padding:.45rem .9rem;border-radius:8px;font-size:.82rem" onclick="bookRemove()">Remove Book</button></div>`;
    document.getElementById("book-upload-summary").textContent="Replace book";
  }}
  el.innerHTML=body;
}}

// ── Recording ────────────────────────────────────────────────────────────────
function renderRecording(s){{
  const el=document.getElementById("recording-section");
  const recOn=s.record_enabled,repOn=s.replay_enabled;
  let info="",dl="";
  if(s.rec_exists&&s.rec_meta&&s.rec_meta.date){{
    const kb=(s.rec_meta.size_bytes||0)>>10;
    info=`<div class="rec-meta">Recorded: ${{s.rec_meta.date}} · ${{kb}} KB</div>`;
    dl=`<a href="/recordings/audio" class="btn-dl" download="conference.mp3">⬇ Download Recording</a>`;
  }}else{{
    info='<div class="rec-meta" style="color:var(--text3)">No recording saved yet.</div>';
  }}
  el.innerHTML=`
    <div class="toggle-row"><button class="toggle-btn ${{recOn?'toggle-on':'toggle-off'}}" onclick="toggleRecording()">${{recOn?"Record: On":"Record: Off"}}</button><span class="toggle-hint">${{recOn?"Next conference will be recorded.":"Enable to record conferences."}}</span></div>
    <div class="toggle-row"><button class="toggle-btn ${{repOn?'toggle-on':'toggle-off'}}" onclick="toggleReplay()">${{repOn?"Replay for Late Callers: On":"Replay for Late Callers: Off"}}</button><span class="toggle-hint">${{repOn?"Late callers hear the last recording.":"Late callers join a silent conference."}}</span></div>
    ${{info}}${{dl}}`;
}}

// ── Announcements ────────────────────────────────────────────────────────────
function renderAnnouncements(s){{
  const el=document.getElementById("announcements-section");
  const on=s.announcements_enabled;
  el.innerHTML=`<div class="toggle-row"><button class="toggle-btn ${{on?'toggle-on':'toggle-off'}}" onclick="toggleAnnouncements()">${{on?"Join Announcements: On":"Join Announcements: Off"}}</button><span class="toggle-hint">${{on?"Everyone hears '[Name] has joined' when someone connects.":"Enable to announce participants when they join."}}</span></div>`;
}}

// ── Sheets ───────────────────────────────────────────────────────────────────
function renderSheets(s){{
  const el=document.getElementById("sheets-section");
  const msg=s.sheets_msg||"";
  const msgHtml=msg?`<span class="${{s.sheets_ok?'sheets-msg-ok':'sheets-msg-err'}}">${{msg}}</span>`:"";
  el.innerHTML=`<div class="sheets-row"><button class="btn-sync" onclick="sheetsSync()">↺ Re-sync from Sheet</button>${{msgHtml}}</div>`;
}}

// ── Hangup ───────────────────────────────────────────────────────────────────
async function renderHangup(){{
  const sec=document.getElementById("hangup-section");
  const ctl=document.getElementById("hangup-controls");
  const r=await fetch("/api/live-calls",{{credentials:"include"}}).then(x=>x.json()).catch(()=>({{calls:[]}}));
  const calls=r.calls||[];
  if(!calls.length){{
    ctl.innerHTML='<p style="color:var(--text3);font-size:.83rem">No active calls right now.</p>';
    return;
  }}
  const connected=calls.filter(c=>c.status==="connected").length;
  const ringing=calls.filter(c=>c.status==="dialing").length;
  let html=`<div class="hangup-all-row">
    <button class="btn-hangup-all" onclick="hangupAllAction(false)">🔴 Hang Up Everyone (${{calls.length}})</button>
    <button class="btn-hangup-block" onclick="hangupAllAction(true)">🚫 Hang Up + Block All</button>
  </div>
  <p style="font-size:.73rem;color:var(--text3);margin-bottom:.5rem">${{connected}} connected · ${{ringing}} ringing</p>`;
  html+=calls.map(c=>{{
    const color=c.status==="connected"?"var(--green)":"var(--yellow)";
    const label=c.status==="connected"?"Connected":"Ringing";
    const blocked=c.blocked?'<span style="color:var(--orange);font-size:.72rem"> · Blocked</span>':'';
    return `<div class="live-row">
      <div class="live-info">
        <span style="font-family:'Inter',monospace;font-weight:600">${{c.number}}</span>
        ${{c.name?`<span style="color:var(--text2);font-size:.78rem"> — ${{c.name}}</span>`:''}}
        <span style="color:${{color}};font-size:.72rem;margin-left:.3rem">● ${{label}}</span>${{blocked}}
      </div>
      <div class="live-actions">
        <button class="btn-hup" onclick="hangupOneAction('${{c.uuid}}',false)">Hang Up</button>
        <button class="btn-hup-block" onclick="hangupOneAction('${{c.uuid}}',true)">+ Block</button>
      </div>
    </div>`;
  }}).join("");
  ctl.innerHTML=html;
}}

async function hangupAllAction(block){{
  if(!confirm(block?"Hang up all and block from calling back?":"Hang up all active calls?"))return;
  const r=await post("/hangup/all",{{block}});
  if(r.ok){{toast(block?`Hung up ${{r.hung_up.length}} and blocked`:`Hung up ${{r.hung_up.length}} call(s)`);setTimeout(renderHangup,1500);}}
  else toast("Hangup failed");
}}

async function hangupOneAction(uuid,block){{
  if(!confirm(block?"Hang up and block from calling back this session?":"Hang up this person?"))return;
  const r=await post("/hangup/one",{{uuid,block}});
  if(r.ok){{toast(block?"Hung up and blocked":"Hung up");setTimeout(renderHangup,1500);}}
  else toast("Failed: "+(r.error||""));
}}

// ── Actions ───────────────────────────────────────────────────────────────────
async function triggerConference(){{
  const btn=document.getElementById("trigger-btn");
  btn.disabled=true; btn.innerHTML='<span class="live-dot"></span>Starting…';
  const r=await post("/trigger");
  if(!r.ok){{toast("Already running");btn.disabled=false;btn.innerHTML="▶ &nbsp;Start Conference Now";}}
  else{{toast("Conference started!");setTimeout(refresh,2000);}}
}}
async function addNumber(){{
  const num=document.getElementById("new-number").value.trim();
  const name=document.getElementById("new-name").value.trim();
  if(!num)return;
  const r=await post("/numbers/add",{{number:num,name}});
  if(r.ok){{document.getElementById("new-number").value="";document.getElementById("new-name").value="";renderNumbers(r.numbers);toast("Number added");}}
  else toast("Invalid number");
}}
async function removeNumber(n){{if(!confirm(`Remove ${{n}}?`))return;const r=await post("/numbers/remove",{{number:n}});if(r.ok){{renderNumbers(r.numbers);toast("Removed");}}}}
async function togglePause(n,paused){{const r=await post(paused?"/numbers/unpause":"/numbers/pause",{{number:n}});if(r.ok){{renderNumbers(r.numbers);toast(paused?"Resumed":"Paused");}}}}
async function saveName(n){{const name=document.getElementById(`name-${{n}}`).value.trim();const r=await post("/numbers/setname",{{number:n,name}});if(r.ok){{renderNumbers(r.numbers);toast("Name saved");}}}}
async function setDay(day){{const s=spinState[day];const h24=to24(s);const t=`${{String(h24).padStart(2,"0")}}:${{String(s.m).padStart(2,"0")}}`;const r=await post("/schedule/set-day",{{day,time:t}});if(r.ok){{renderSchedule(r.schedule);toast("Schedule set!");}}}}
async function clearDay(day,name){{if(!confirm(`Remove ${{name}}?`))return;const r=await post("/schedule/clear-day",{{day}});if(r.ok){{renderSchedule(r.schedule);toast("Removed");}}}}
async function toggleReading(){{await post("/reading/toggle");refresh();}}
async function bookAdvance(){{await post("/book/advance");refresh();}}
async function bookRemove(){{if(!confirm("Remove this book?"))return;await post("/book/remove");refresh();toast("Book removed");}}
async function uploadBook(){{const form=document.getElementById("book-upload-form");const r=await post("/book/upload",new FormData(form),true);if(r.ok){{refresh();toast(`Uploaded — ${{r.count}} portions`);document.getElementById("book-upload-details").open=false;}}else toast("Upload failed");}}
async function toggleRecording(){{await post("/recording/toggle");refresh();}}
async function toggleReplay(){{await post("/replay/toggle");refresh();}}
async function toggleAnnouncements(){{await post("/announcements/toggle");refresh();}}
async function sheetsSync(){{toast("Syncing…",3000);await fetch("/sheets/sync",{{method:"POST"}});const s=await fetch("/api/state",{{credentials:"include"}}).then(r=>r.json());renderNumbers(s.numbers);renderSheets(s);toast("Synced!");}}

// ── Refresh ───────────────────────────────────────────────────────────────────
async function refresh(){{
  try{{
    const s=await fetch("/api/state",{{credentials:"include"}}).then(r=>r.json());
    renderLastRun(s);renderNumbers(s.numbers);renderSchedule(s.schedule);
    renderReading(s);renderRecording(s);renderAnnouncements(s);renderSheets(s);
    renderHangup();
  }}catch(e){{console.error("Refresh error",e);}}
}}

refresh();
setInterval(()=>{{const b=document.getElementById("trigger-btn");if(b&&b.disabled)refresh();}},8000);

// PWA
let deferredPrompt;
const banner=document.getElementById("install-banner");
window.addEventListener("beforeinstallprompt",e=>{{e.preventDefault();deferredPrompt=e;if(!sessionStorage.getItem("install-dismissed"))banner.style.display="flex";}});
document.getElementById("install-btn").addEventListener("click",async()=>{{if(!deferredPrompt)return;deferredPrompt.prompt();await deferredPrompt.userChoice;deferredPrompt=null;banner.style.display="none";}});
document.getElementById("dismiss-btn").addEventListener("click",()=>{{banner.style.display="none";sessionStorage.setItem("install-dismissed","1");}});
if("serviceWorker"in navigator)navigator.serviceWorker.register("/sw.js").catch(()=>{{}});
</script>
</body></html>"""

# ── Schedule routes ───────────────────────────────────────────────────────────

@app.route("/schedule/set-day", methods=["POST"])
@login_required
def schedule_set_day():
    try:
        data = request.json or {}
        day  = int(request.form.get("day", data.get("day", 0)))
        t    = request.form.get("time", data.get("time", "22:45"))
        h, m = [int(x) for x in t.split(":")]
        if 0 <= day <= 6 and 0 <= h <= 23 and 0 <= m <= 59:
            set_day_schedule(day, h, m)
    except (KeyError, ValueError):
        pass
    return jsonify({"ok": True, "schedule": load_schedule()})

@app.route("/schedule/clear-day", methods=["POST"])
@login_required
def schedule_clear_day():
    try:
        data = request.json or {}
        clear_day_schedule(int(request.form.get("day", data.get("day", 0))))
    except (KeyError, ValueError):
        pass
    return jsonify({"ok": True, "schedule": load_schedule()})

@app.route("/schedule/add", methods=["POST"])
@login_required
def schedule_add():
    try:
        day    = int(request.form["day"])
        time_s = request.form.get("time", "22:45")
        hour, minute = [int(x) for x in time_s.split(":")]
        if 0 <= day <= 6 and 0 <= hour <= 23 and 0 <= minute <= 59:
            add_schedule_entry(day, hour, minute)
    except (KeyError, ValueError):
        pass
    return jsonify({"ok": True, "schedule": load_schedule()})

@app.route("/schedule/remove", methods=["POST"])
@login_required
def schedule_remove():
    try:
        day    = int(request.form["day"])
        hour   = int(request.form["hour"])
        minute = int(request.form["minute"])
        remove_schedule_entry(day, hour, minute)
    except (KeyError, ValueError):
        pass
    return jsonify({"ok": True, "schedule": load_schedule()})

# ── Scheduler ─────────────────────────────────────────────────────────────────

def run_scheduler():
    fired_today: set = set()   # set of (day, hour, minute) fired this calendar day
    last_date = None
    last_minute = None
    while True:
        now   = datetime.now(EASTERN)
        today = now.date()
        if last_date != today:
            fired_today = set()
            last_date   = today
        key = (now.weekday(), now.hour, now.minute)
        # Only check once per minute — skip if we already checked this minute
        if key != last_minute:
            last_minute = key
            for entry in load_schedule():
                ekey = (entry["day"], entry["hour"], entry["minute"])
                if key == ekey and ekey not in fired_today:
                    fired_today.add(ekey)
                    threading.Thread(target=start_conference, daemon=True).start()
                    break
        time.sleep(15)

def _startup_sync():
    time.sleep(5)          # let Flask fully start first
    ok, msg = sync_from_sheets()
    print(f"[startup sync] {'OK' if ok else 'FAIL'}: {msg}", flush=True)

# ── Startup (runs under both gunicorn and direct python) ──────────────────────
# Must be at module level so gunicorn picks it up
init_db()
threading.Thread(target=run_scheduler, daemon=True).start()
threading.Thread(target=_startup_sync, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
