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
call_status_map = {}
inbound_uuid_map = {}
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
    """NCCO for inbound callers with a press-1 prompt to announce their arrival."""
    return [
        {"action": "talk",
         "text": "Joining you into the Shmiras Halashon conference. Press 1 to announce your arrival to the group."},
        {"action": "input", "type": ["dtmf"],
         "dtmf": {"maxDigits": 1, "timeOut": 6},
         "eventUrl": [f"{BASE_URL}/join-announce"]},
        *_conference_ncco()
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

@app.route("/answer", methods=["GET","POST"])
def answer():
    data = request.get_json(silent=True) or request.values
    uuid = data.get("uuid", "")
    with log_lock:
        is_inbound = uuid not in call_status_map
    if is_inbound:
        from_num = data.get("from", "")
        _handle_inbound_announcement(uuid, from_num)
        # Determine call source: replay (missed conference) vs live (joining active conference)
        with log_lock:
            running = last_run.get("running", False)
        if not running and get_replay_enabled() and os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3")):
            source = "inbound-replay"
        else:
            source = "inbound-live"
        clean = _clean(from_num) if from_num else None
        num = clean or from_num or "Unknown"
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
    """Called by Vonage when an inbound caller presses a key on the join prompt."""
    data  = request.get_json(silent=True) or {}
    uuid  = data.get("uuid", "")
    digit = (data.get("dtmf") or {}).get("digits", "") or data.get("digits", "")
    if str(digit).strip() == "1" and uuid and get_announcements_enabled():
        with log_lock:
            num = inbound_uuid_map.get(uuid, "")
        name = get_name(num) if num else ""
        if name:
            threading.Thread(target=announce_join,
                             kwargs={"name": name, "exclude_uuid": uuid, "delay": 4},
                             daemon=True).start()
    return jsonify(_conference_ncco())

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
    n = request.form.get("number", "").strip()
    if n:
        with numbers_lock:
            added, removed, paused = _load_local()
            paused.add(n)
            _save_local(added, removed, paused)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

@app.route("/numbers/unpause", methods=["POST"])
@login_required
def numbers_unpause():
    n = request.form.get("number", "").strip()
    if n:
        with numbers_lock:
            added, removed, paused = _load_local()
            paused.discard(n)
            _save_local(added, removed, paused)
    return jsonify({"ok": True, "numbers": get_all_numbers_with_source()})

@app.route("/numbers/setname", methods=["POST"])
@login_required
def numbers_setname():
    n    = request.form.get("number", "").strip()
    name = request.form.get("name", "").strip()
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
        rows = "<tr><td colspan='4' style='color:#64748b;text-align:center'>No call history yet.</td></tr>"
    return f"""<!DOCTYPE html><html lang='en'><head>
  <meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Call History</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,sans-serif;background:#0f1117;color:#e2e8f0;padding:1.5rem 1rem}}
    .wrap{{max-width:700px;margin:0 auto}}
    h1{{font-size:1.3rem;font-weight:700;margin-bottom:1rem}}
    a{{color:#6366f1;text-decoration:none;font-size:.85rem}}
    table{{width:100%;border-collapse:collapse;font-size:.85rem;margin-top:1rem}}
    th{{text-align:left;padding:.5rem .75rem;color:#64748b;font-size:.75rem;text-transform:uppercase;border-bottom:1px solid #2d3748}}
    td{{padding:.55rem .75rem;border-bottom:1px solid #1e2433}}
    tr:hover td{{background:#1e2433}}
  </style></head><body>
  <div class='wrap'>
    <div style='display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem'>
      <h1>📋 Call History</h1><a href='/status'>← Back</a>
    </div>
    <table><thead><tr><th>Time (ET)</th><th>Number</th><th>Name</th><th>Status</th></tr></thead>
    <tbody>{rows}</tbody></table>
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
    days_json = '["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]'
    return f"""<!DOCTYPE html>
<html lang='en'><head>
  <meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Conference Manager</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#1e2433'/>
  <meta name='apple-mobile-web-app-capable' content='yes'/>
  <meta name='apple-mobile-web-app-status-bar-style' content='black-translucent'/>
  <meta name='apple-mobile-web-app-title' content='Conference'/>
  <link rel='apple-touch-icon' href='/static/icon.svg'/>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f1117;color:#e2e8f0;padding:1.5rem 1rem 3rem;min-height:100vh}}
    .wrap{{max-width:580px;margin:0 auto;display:flex;flex-direction:column;gap:1.75rem}}
    h1{{font-size:1.4rem;font-weight:700;color:#f8fafc}}
    h2{{font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#64748b;margin-bottom:.6rem}}
    .muted{{color:#64748b}}
    .trigger-btn{{width:100%;padding:.85rem;background:#2563eb;color:#fff;border:none;border-radius:10px;font-size:1rem;font-weight:700;cursor:pointer}}
    .trigger-btn:hover:not([disabled]){{background:#1d4ed8}}
    .trigger-btn[disabled]{{background:#1e3a5f;color:#64748b;cursor:not-allowed}}
    .summary{{display:flex;justify-content:space-between;align-items:flex-start;background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.7rem 1rem;font-size:.85rem;margin-bottom:.5rem;gap:.5rem}}
    .live{{color:#4ade80;font-weight:700;margin-left:.35rem}}
    .counts{{font-weight:700;color:#4ade80;white-space:nowrap}}
    .vote-count{{color:#a5b4fc;font-size:.8rem}}
    .vote-done{{color:#4ade80;font-size:.8rem}}
    .toggle-row{{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap}}
    .toggle-btn{{border:none;border-radius:8px;padding:.45rem 1rem;font-size:.85rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .toggle-on{{background:#14532d;color:#86efac}}
    .toggle-on:hover{{background:#166534}}
    .toggle-off{{background:#1e2433;color:#64748b;border:1px solid #2d3748}}
    .toggle-off:hover{{border-color:#6366f1;color:#a5b4fc}}
    .dial-box{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.7rem 1rem;font-size:.9rem;display:flex;align-items:center;justify-content:space-between;gap:.5rem}}
    .dial-num{{font-family:monospace;font-size:1rem;font-weight:700;color:#f8fafc;letter-spacing:.04em}}
    .day-grid{{display:flex;flex-direction:column;gap:.35rem}}
    .day-row{{display:flex;align-items:center;gap:.6rem;padding:.5rem .75rem;border-radius:8px;background:#1a2035;border:1px solid #2d3748;transition:border-color .15s}}
    .day-row.active{{border-color:#3b82f6;background:#1b2a45}}
    .day-name{{min-width:88px;font-size:.88rem;color:#e2e8f0;font-weight:600}}
    .day-form{{display:flex;align-items:center;gap:.45rem;flex:1}}
    .time-inp{{background:#0f172a;border:1px solid #2d3748;color:#e2e8f0;border-radius:6px;padding:.38rem .6rem;font-size:.85rem;color-scheme:dark;flex:1;min-width:90px}}
    .time-inp:focus{{outline:none;border-color:#3b82f6}}
    .set-btn{{background:#1d4ed8;color:#fff;border:none;border-radius:6px;padding:.38rem .8rem;font-size:.82rem;cursor:pointer;white-space:nowrap}}
    .set-btn:hover{{background:#2563eb}}
    .day-clear{{background:none;border:none;color:#f87171;font-size:1.05rem;cursor:pointer;padding:.1rem .35rem;line-height:1}}
    ul{{list-style:none;display:flex;flex-direction:column;gap:.4rem}}
    ul.calls li{{display:flex;align-items:center;gap:.6rem;background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.6rem 1rem;font-size:.85rem;flex-wrap:wrap}}
    .icon{{min-width:1.2rem}}
    .num{{font-family:monospace}}
    .cname{{color:#94a3b8;font-size:.8rem;margin-left:.2rem;flex:1}}
    .stat{{font-weight:600;text-transform:capitalize;font-size:.8rem}}
    .err-text{{color:#f87171;font-size:.75rem}}
    .call-type{{font-size:.72rem;font-weight:600;padding:.15rem .5rem;border-radius:999px;margin-left:auto;white-space:nowrap}}
    .call-type.dialed-out{{background:#1e3a5f;color:#7dd3fc}}
    .call-type.inbound-live{{background:#0c3b2e;color:#6ee7b7}}
    .call-type.inbound-replay{{background:#2e1b4e;color:#c4b5fd}}
    ul.nums li{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.65rem .85rem;display:flex;flex-direction:column;gap:.5rem}}
    .num-info{{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}}
    .nname{{color:#94a3b8;font-size:.8rem;margin-left:auto}}
    .num-actions{{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}}
    .name-input{{flex:1;background:#0f1117;border:1px solid #2d3748;color:#e2e8f0;border-radius:6px;padding:.3rem .6rem;font-size:.8rem;min-width:80px}}
    .name-input:focus{{outline:none;border-color:#3b82f6}}
    .save-btn{{background:#1e3a5f;color:#93c5fd;border:none;border-radius:6px;padding:.3rem .65rem;font-size:.78rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .save-btn:hover{{background:#1d4ed8;color:#fff}}
    .tag{{font-size:.68rem;font-weight:700;padding:.12rem .4rem;border-radius:4px;letter-spacing:.04em}}
    .tag.sheet{{background:#1d3461;color:#93c5fd}}
    .tag.local{{background:#14532d;color:#86efac}}
    .tag.paused{{background:#422006;color:#fb923c}}
    .pause-btn{{background:#292524;color:#fb923c;border:1px solid #422006;border-radius:6px;padding:.3rem .6rem;font-size:.78rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .pause-btn:hover{{background:#422006}}
    .unpause-btn{{background:#14532d;color:#86efac;border:none;border-radius:6px;padding:.3rem .6rem;font-size:.78rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .unpause-btn:hover{{background:#166534}}
    ul.nums li.num-paused{{opacity:.55;border-style:dashed}}
    .add-row{{display:flex;gap:.5rem;flex-wrap:wrap}}
    .add-row input{{flex:1;min-width:100px;background:#1e2433;border:1px solid #2d3748;color:#e2e8f0;border-radius:8px;padding:.6rem .85rem;font-size:.85rem}}
    .add-row input:focus{{outline:none;border-color:#3b82f6}}
    .add-btn{{background:#15803d;color:#fff;border:none;border-radius:8px;padding:.6rem 1rem;font-size:.85rem;font-weight:700;cursor:pointer}}
    .add-btn:hover{{background:#166534}}
    .rm-btn{{background:#7f1d1d;color:#fca5a5;border:none;border-radius:6px;padding:.3rem .6rem;font-size:.78rem;font-weight:700;cursor:pointer}}
    .rm-btn:hover{{background:#991b1b}}
    .book-info{{display:flex;justify-content:space-between;align-items:center;margin-bottom:.5rem;font-size:.85rem}}
    .book-title{{font-weight:700}}
    .book-btns{{display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:.5rem}}
    .sec-btn{{background:#1e2433;color:#e2e8f0;border:1px solid #2d3748;border-radius:8px;padding:.5rem 1rem;font-size:.85rem;font-weight:600;cursor:pointer}}
    .sec-btn:hover{{border-color:#6366f1;color:#a5b4fc}}
    .upload-form{{display:flex;flex-direction:column;gap:.6rem;margin-top:.75rem}}
    .upload-form input{{background:#1e2433;border:1px solid #2d3748;color:#e2e8f0;border-radius:8px;padding:.55rem .85rem;font-size:.85rem;width:100%}}
    .upload-form input[type=file]{{color:#94a3b8}}
    .upload-btn{{background:#4f46e5;color:#fff;border:none;border-radius:8px;padding:.6rem 1rem;font-size:.85rem;font-weight:700;cursor:pointer}}
    .upload-btn:hover{{background:#4338ca}}
    .hint{{font-size:.75rem;color:#475569;line-height:1.5}}
    .footer{{font-size:.73rem;color:#374151;text-align:center}}
    details summary{{cursor:pointer;font-size:.85rem;color:#6366f1;font-weight:600;user-select:none}}
    #install-banner{{display:none;align-items:center;justify-content:space-between;gap:.75rem;background:#1a2744;border:1px solid #2563eb;border-radius:10px;padding:.75rem 1rem;font-size:.85rem}}
    #install-banner span{{color:#93c5fd}}
    .install-btn{{background:#2563eb;color:#fff;border:none;border-radius:8px;padding:.45rem 1rem;font-size:.85rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .install-btn:hover{{background:#1d4ed8}}
    .dismiss-btn{{background:none;border:none;color:#64748b;cursor:pointer;font-size:1rem;padding:.2rem .4rem}}
    .toast{{position:fixed;bottom:1.5rem;left:50%;transform:translateX(-50%);background:#1e2433;border:1px solid #2d3748;color:#e2e8f0;padding:.6rem 1.25rem;border-radius:10px;font-size:.85rem;opacity:0;transition:opacity .3s;pointer-events:none;z-index:999}}
    .toast.show{{opacity:1}}
  </style>
</head>
<body><div class='wrap'>

  <div id='install-banner'>
    <span>Install this app on your device for quick access.</span>
    <div style='display:flex;gap:.5rem;align-items:center'>
      <button class='install-btn' id='install-btn'>Install App</button>
      <button class='dismiss-btn' id='dismiss-btn' title='Dismiss'>✕</button>
    </div>
  </div>

  <div style='display:flex;justify-content:space-between;align-items:center'>
    <h1>Conference Manager</h1>
    <form method='POST' action='/logout'>
      <button style='background:none;border:1px solid #2d3748;color:#64748b;border-radius:8px;padding:.35rem .75rem;font-size:.78rem;cursor:pointer'>Sign out</button>
    </form>
  </div>

  <section>
    <button class='trigger-btn' id='trigger-btn' onclick='triggerConference()'>▶ Start Conference Now</button>
  </section>

  <section>
    <h2>Dial-In Number</h2>
    <div class='dial-box'>
      <span class='muted'>Participants can call in directly:</span>
      <span class='dial-num'>{dial_in_fmt}</span>
    </div>
  </section>

  <section>
    <h2>Last Conference</h2>
    <div id='last-run'><p class='muted'>Loading...</p></div>
  </section>

  <section>
    <h2>Phone Numbers (<span id='num-count'>0</span>)</h2>
    <div class='add-row' style='margin-bottom:.75rem'>
      <input type='tel'  id='new-number' placeholder='Number, e.g. 2025551234'/>
      <input type='text' id='new-name'   placeholder='Name (optional)'/>
      <button class='add-btn' onclick='addNumber()'>+ Add</button>
    </div>
    <ul class='nums' id='numbers-list'><li class='muted'>Loading...</li></ul>
  </section>

  <section>
    <h2>Schedule</h2>
    <div class='day-grid' id='day-grid'><p class='muted'>Loading...</p></div>
    <p class='hint' style='margin-top:.6rem'>Times are Eastern (ET). Changes take effect immediately.</p>
  </section>

  <section>
    <h2>Daily Reading</h2>
    <div id='reading-section'><p class='muted'>Loading...</p></div>
    <details id='book-upload-details'>
      <summary id='book-upload-summary'>Upload a book (.txt)</summary>
      <form id='book-upload-form' class='upload-form'>
        <input type='file'   name='book'              accept='.txt' required/>
        <input type='text'   name='title'             placeholder='Book title (optional)'/>
        <input type='number' name='lines_per_portion' value='30' min='5' max='200'/>
        <p class='hint'>Upload a plain .txt file. Each portion is read aloud via text-to-speech only if all participants vote yes.</p>
        <button type='button' class='upload-btn' onclick='uploadBook()'>Upload &amp; Split</button>
      </form>
    </details>
  </section>

  <section>
    <h2>Recording</h2>
    <div id='recording-section'><p class='muted'>Loading...</p></div>
  </section>

  <section>
    <h2>Join Announcements</h2>
    <div id='announcements-section'><p class='muted'>Loading...</p></div>
  </section>

  <section>
    <h2>Google Sheets Sync</h2>
    <p class='muted' style='font-size:.82rem;margin-bottom:.75rem'>
      Numbers sync automatically from your Google Sheet on startup.
      Column A&nbsp;=&nbsp;name, Column B&nbsp;=&nbsp;number, row&nbsp;1&nbsp;=&nbsp;header.
    </p>
    <div id='sheets-section'><p class='muted'>Loading...</p></div>
  </section>

  <p class='footer'><span class='tag paused'>Paused</span> numbers are skipped on the next call &nbsp;|&nbsp; <a href='/history' style='color:#6366f1;text-decoration:none'>Call History</a> &nbsp;|&nbsp; <a href='/download-code' style='color:#6366f1;text-decoration:none'>⬇ Download Code</a></p>
  <div class='toast' id='toast'></div>
</div>

<script>
const DAYS = {days_json};
const STATUS_ICONS = {{
  connected:["✅","#4ade80"], voicemail:["📵","#fb923c"],
  dialing:["⏳","#facc15"],   busy:["🔴","#f87171"],
  unanswered:["🔕","#94a3b8"],timeout:["🔕","#94a3b8"],
  failed:["❌","#f87171"],    error:["❌","#f87171"],
}};

function toast(msg, dur=2200) {{
  const t = document.getElementById("toast");
  t.textContent = msg; t.classList.add("show");
  setTimeout(() => t.classList.remove("show"), dur);
}}

async function post(url, data={{}}, isForm=false) {{
  const opts = isForm
    ? {{method:"POST", body:data}}
    : {{method:"POST", headers:{{"Content-Type":"application/json"}}, body:JSON.stringify(data)}};
  const r = await fetch(url, opts);
  return r.json().catch(() => ({{ok:false}}));
}}

// ── Render functions ────────────────────────────────────────────────────────

function renderLastRun(s) {{
  const el  = document.getElementById("last-run");
  const btn = document.getElementById("trigger-btn");
  if (s.running) {{ btn.disabled=true; btn.textContent="● Running…"; }}
  else           {{ btn.disabled=false; btn.textContent="▶ Start Conference Now"; }}
  if (!s.run_time) {{
    el.innerHTML = "<p class='muted'>No conference has run yet since the server started.</p>";
    return;
  }}
  const badge = s.running ? "<span class='live'>● Live</span>" : "";
  const allCalls = [...(s.calls||[]), ...(s.inbound_calls||[])];
  const connected = (s.calls||[]).filter(c=>c.status==="connected").length;
  const rows = (s.calls||[]).map(c => {{
    const [icon,color] = STATUS_ICONS[c.status]||["❓","#94a3b8"];
    const name = c.name ? `<span class="cname">${{c.name}}</span>` : "";
    const err  = c.error ? `<span class="err-text">(${{c.error}})</span>` : "";
    return `<li><span class="icon">${{icon}}</span><span class="num">${{c.number}}</span>${{name}}<span class="stat" style="color:${{color}}">${{c.status}}</span><span class="call-type dialed-out">Dialed out</span>${{err}}</li>`;
  }}).join("") + (s.inbound_calls||[]).map(c => {{
    const src = c.source||"inbound-live";
    const name = c.name ? `<span class="cname">${{c.name}}</span>` : "";
    const t    = c.time  ? `<span class="cname">${{c.time}}</span>` : "";
    const [icon,color,label,cls] = src==="inbound-replay"
      ? ["🎧","#a78bfa","Called in — heard replay","inbound-replay"]
      : ["📲","#38bdf8","Called in — joined live","inbound-live"];
    return `<li><span class="icon">${{icon}}</span><span class="num">${{c.number}}</span>${{name}}${{t}}<span class="stat" style="color:${{color}}">connected</span><span class="call-type ${{cls}}">${{label}}</span></li>`;
  }}).join("");
  el.innerHTML = `<div class="summary"><span class="muted">Last run: ${{s.run_time}} ${{badge}}</span><span class="counts">${{connected}}/${{(s.calls||[]).length}} connected</span></div><ul class="calls">${{rows}}</ul>`;
}}

function renderNumbers(numbers) {{
  const count = numbers.filter(r=>!r[3]).length;
  document.getElementById("num-count").textContent = numbers.length;
  const ul = document.getElementById("numbers-list");
  if (!numbers.length) {{
    ul.innerHTML = "<li class='muted' style='border:none;background:none;padding:.5rem 0'>No numbers yet.</li>";
    return;
  }}
  ul.innerHTML = numbers.map(([n, src, name, paused]) => {{
    const li_cls     = paused ? "num-paused" : "";
    const pause_tag  = paused ? "<span class='tag paused'>Paused</span>" : "";
    const src_tag    = src==="sheet" ? "<span class='tag sheet'>Sheet</span>" : "";
    const disp       = name || "<span class='muted'>No name</span>";
    const pause_cls  = paused ? "unpause-btn" : "pause-btn";
    const pause_lbl  = paused ? "Resume" : "Pause";
    const pause_url  = paused ? "/numbers/unpause" : "/numbers/pause";
    return `<li class="${{li_cls}}">
      <div class="num-info"><span class="num">${{n}}</span>${{src_tag}}${{pause_tag}}<span class="nname">${{disp}}</span></div>
      <div class="num-actions">
        <input type="text" class="name-input" value="${{name}}" placeholder="Name" id="name-${{n}}"/>
        <button class="save-btn" onclick="saveName('${{n}}')">Save</button>
        <button class="${{pause_cls}}" onclick="togglePause('${{n}}', ${{paused}})">${{pause_lbl}}</button>
        <button class="rm-btn" onclick="removeNumber('${{n}}')">✕</button>
      </div>
    </li>`;
  }}).join("");
}}

function renderSchedule(schedule) {{
  const grid = document.getElementById("day-grid");
  const byDay = {{}};
  schedule.forEach(e => {{ if(!(e.day in byDay)) byDay[e.day]=e; }});
  grid.innerHTML = DAYS.map((dayName, i) => {{
    const e       = byDay[i];
    const timeVal = e ? `${{String(e.hour).padStart(2,"0")}}:${{String(e.minute).padStart(2,"0")}}` : "";
    const isSet   = !!e;
    const rowCls  = isSet ? "day-row active" : "day-row";
    const setLbl  = isSet ? "Update" : "Set";
    const clearBtn = isSet
      ? `<button class="day-clear" onclick="clearDay(${{i}}, '${{dayName}}')">✕</button>`
      : "";
    return `<div class="${{rowCls}}" id="day-row-${{i}}">
      <span class="day-name">${{dayName}}</span>
      <div class="day-form">
        <input type="time" class="time-inp" id="time-${{i}}" value="${{timeVal}}"/>
        <button class="set-btn" onclick="setDay(${{i}})">${{setLbl}}</button>
        ${{clearBtn}}
      </div>
    </div>`;
  }}).join("");
}}

function renderReading(s) {{
  const el = document.getElementById("reading-section");
  const on = s.reading_enabled;
  const hint = on
    ? "Participants vote by pressing 1. If all vote yes, the reading plays automatically."
    : "Auto-read is disabled.";
  let body = `<div class="toggle-row">
    <button class="toggle-btn ${{on?'toggle-on':'toggle-off'}}" onclick="toggleReading()">${{on?"Auto-Read: On":"Auto-Read: Off"}}</button>
    <span class="muted" style="font-size:.8rem">${{hint}}</span>
  </div>`;
  if (!s.book_total) {{
    body += "<p class='muted' style='margin-top:.75rem'>No book uploaded. Upload a .txt file to enable daily readings.</p>";
    document.getElementById("book-upload-summary").textContent = "Upload a book (.txt)";
  }} else {{
    let vbadge = "";
    if (on && s.expected > 0) {{
      vbadge = s.triggered
        ? "<span class='vote-done'>📖 Reading played this session</span>"
        : `<span class='vote-count'>📖 ${{s.voted}}/${{s.expected}} voted for reading</span>`;
    }}
    body += `<div class="book-info" style="margin-top:.75rem">
      <span class="book-title">${{s.book_title||"Untitled"}}</span>
      <span class="muted">Portion ${{s.book_index+1}} of ${{s.book_total}}</span>
    </div>
    ${{vbadge ? `<div style="margin-bottom:.5rem">${{vbadge}}</div>` : ""}}
    <div class="book-btns">
      <button class="sec-btn" onclick="bookAdvance()">Skip to Next Portion</button>
      <button class="rm-btn" onclick="bookRemove()">Remove Book</button>
    </div>`;
    document.getElementById("book-upload-summary").textContent = "Replace book";
  }}
  el.innerHTML = body;
}}

function renderRecording(s) {{
  const el = document.getElementById("recording-section");
  const rec_on = s.record_enabled, replay_on = s.replay_enabled;
  const rec_hint    = rec_on ? "The next scheduled conference will be recorded automatically." : "Enable to automatically record each scheduled conference call.";
  const replay_hint = replay_on ? "Anyone who calls in after the conference will hear the last recording." : "Enable so callers who missed the conference hear the playback.";
  let info="", dl="";
  if (s.rec_exists && s.rec_meta && s.rec_meta.date) {{
    const kb = (s.rec_meta.size_bytes||0)>>10;
    info = `<p class='muted' style='font-size:.82rem;margin:.4rem 0'>Recorded: ${{s.rec_meta.date}} &nbsp;·&nbsp; ${{kb}} KB</p>`;
    dl   = `<a href='/recordings/audio' class='sec-btn' style='display:inline-block;text-decoration:none;margin-top:.4rem' download='conference.mp3'>⬇ Download</a>`;
  }} else {{
    info = "<p class='muted' style='font-size:.82rem;margin:.4rem 0'>No recording saved yet.</p>";
  }}
  el.innerHTML = `
    <div class="toggle-row">
      <button class="toggle-btn ${{rec_on?'toggle-on':'toggle-off'}}" onclick="toggleRecording()">${{rec_on?"Record Conference: On":"Record Conference: Off"}}</button>
      <span class="muted" style="font-size:.8rem">${{rec_hint}}</span>
    </div>
    <div class="toggle-row" style="margin-top:.6rem">
      <button class="toggle-btn ${{replay_on?'toggle-on':'toggle-off'}}" onclick="toggleReplay()">${{replay_on?"Replay for Late Callers: On":"Replay for Late Callers: Off"}}</button>
      <span class="muted" style="font-size:.8rem">${{replay_hint}}</span>
    </div>
    ${{info}}${{dl}}`;
}}

function renderAnnouncements(s) {{
  const el  = document.getElementById("announcements-section");
  const on  = s.announcements_enabled;
  const hint = on ? "Everyone on the call hears '[Name] has joined' when someone connects." : "Enable to announce each participant's name when they join.";
  el.innerHTML = `<div class="toggle-row">
    <button class="toggle-btn ${{on?'toggle-on':'toggle-off'}}" onclick="toggleAnnouncements()">${{on?"Join Announcements: On":"Join Announcements: Off"}}</button>
    <span class="muted" style="font-size:.8rem">${{hint}}</span>
  </div>`;
}}

function renderSheets(s) {{
  const el = document.getElementById("sheets-section");
  const sid = s.spreadsheet_id || "";
  const msg = s.sheets_msg || "";
  const msgHtml = msg ? `<p style='color:${{s.sheets_ok?"#86efac":"#f87171"}};font-size:.82rem;margin:.5rem 0 0'>${{msg}}</p>` : "";
  el.innerHTML = `<button class="save-btn" style="background:#14532d;color:#86efac;padding:.4rem .9rem" ${{sid?"":"disabled"}} onclick="sheetsSync()">↺ Re-sync from Sheet</button>${{msgHtml}}`;
}}

async function refresh() {{
  try {{
    const s = await fetch("/api/state", {{credentials: "include"}}).then(r=>r.json());
    renderLastRun(s);
    renderNumbers(s.numbers);
    renderSchedule(s.schedule);
    renderReading(s);
    renderRecording(s);
    renderAnnouncements(s);
    renderSheets(s);
  }} catch(e) {{ console.error("Refresh error", e); }}
}}

// ── Actions ─────────────────────────────────────────────────────────────────

async function triggerConference() {{
  const btn = document.getElementById("trigger-btn");
  btn.disabled=true; btn.textContent="● Starting…";
  const r = await post("/trigger");
  if (!r.ok) {{ toast("Already running"); btn.disabled=false; btn.textContent="▶ Start Conference Now"; }}
  else {{ toast("Conference started!"); setTimeout(refresh, 2000); }}
}}

async function addNumber() {{
  const num  = document.getElementById("new-number").value.trim();
  const name = document.getElementById("new-name").value.trim();
  if (!num) return;
  const r = await post("/numbers/add", {{number:num, name}});
  if (r.ok) {{
    document.getElementById("new-number").value = "";
    document.getElementById("new-name").value   = "";
    renderNumbers(r.numbers); toast("Number added");
  }} else {{ toast("Invalid number"); }}
}}

async function removeNumber(n) {{
  if (!confirm(`Remove ${{n}}?`)) return;
  const r = await post("/numbers/remove", {{number:n}});
  if (r.ok) {{ renderNumbers(r.numbers); toast("Removed"); }}
}}

async function togglePause(n, paused) {{
  const r = await post(paused?"/numbers/unpause":"/numbers/pause", {{number:n}});
  if (r.ok) {{ renderNumbers(r.numbers); toast(paused?"Resumed":"Paused"); }}
}}

async function saveName(n) {{
  const name = document.getElementById(`name-${{n}}`).value.trim();
  const r = await post("/numbers/setname", {{number:n, name}});
  if (r.ok) {{ renderNumbers(r.numbers); toast("Name saved"); }}
}}

async function setDay(day) {{
  const t = document.getElementById(`time-${{day}}`).value;
  if (!t) return;
  const r = await post("/schedule/set-day", {{day, time:t}});
  if (r.ok) {{ renderSchedule(r.schedule); toast("Schedule updated"); }}
}}

async function clearDay(day, name) {{
  if (!confirm(`Remove ${{name}}?`)) return;
  const r = await post("/schedule/clear-day", {{day}});
  if (r.ok) {{ renderSchedule(r.schedule); toast("Removed"); }}
}}

async function toggleReading()       {{ await post("/reading/toggle");       refresh(); }}
async function bookAdvance()         {{ await post("/book/advance");          refresh(); }}
async function bookRemove()          {{ if(!confirm("Remove this book?")) return; await post("/book/remove"); refresh(); toast("Book removed"); }}
async function toggleRecording()     {{ await post("/recording/toggle");      refresh(); }}
async function toggleReplay()        {{ await post("/replay/toggle");         refresh(); }}
async function toggleAnnouncements() {{ await post("/announcements/toggle");  refresh(); }}

async function uploadBook() {{
  const form = document.getElementById("book-upload-form");
  const r = await post("/book/upload", new FormData(form), true);
  if (r.ok) {{ refresh(); toast(`Book uploaded — ${{r.count}} portions`); document.getElementById("book-upload-details").open=false; }}
  else {{ toast("Upload failed"); }}
}}

async function sheetsSync() {{
  toast("Syncing...", 3000);
  const r = await fetch("/sheets/sync", {{method:"POST"}}).then(()=>fetch("/api/state").then(x=>x.json()));
  renderNumbers(r.numbers);
  renderSheets(r);
}}

// ── Init ────────────────────────────────────────────────────────────────────
refresh();
setInterval(async () => {{
  const btn = document.getElementById("trigger-btn");
  if (btn && btn.disabled) refresh();
}}, 10000);

// PWA install
let deferredPrompt;
const banner     = document.getElementById("install-banner");
const installBtn = document.getElementById("install-btn");
const dismissBtn = document.getElementById("dismiss-btn");
window.addEventListener("beforeinstallprompt", e => {{
  e.preventDefault(); deferredPrompt = e;
  if (!sessionStorage.getItem("install-dismissed")) banner.style.display="flex";
}});
installBtn.addEventListener("click", async () => {{
  if (!deferredPrompt) return;
  deferredPrompt.prompt();
  await deferredPrompt.userChoice;
  deferredPrompt=null; banner.style.display="none";
}});
dismissBtn.addEventListener("click", () => {{
  banner.style.display="none";
  sessionStorage.setItem("install-dismissed","1");
}});
window.addEventListener("appinstalled", () => {{ banner.style.display="none"; }});

if ("serviceWorker" in navigator) navigator.serviceWorker.register("/sw.js").catch(()=>{{}});
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
    while True:
        now   = datetime.now(EASTERN)
        today = now.date()
        if last_date != today:
            fired_today = set()
            last_date   = today
        key = (now.weekday(), now.hour, now.minute)
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

if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    if get_spreadsheet_id():
        threading.Thread(target=_startup_sync, daemon=True).start()
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
