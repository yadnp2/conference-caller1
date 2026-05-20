import vonage, threading, time, os, json, functools, uuid as _uuid, requests, queue as _queue
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, redirect, session, url_for
from werkzeug.security import generate_password_hash, check_password_hash
from vonage_voice.models import CreateCallRequest, ToPhone, Phone, TtsStreamOptions
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
app.secret_key = os.environ["SESSION_SECRET"]

VONAGE_APP_ID   = os.environ["VONAGE_APP_ID"]
FROM_NUMBER     = os.environ["FROM_NUMBER"]
BASE_URL        = os.environ["BASE_URL"]
CONFERENCE_NAME = "DailyConference"
EASTERN         = ZoneInfo("America/New_York")
BASE_DIR        = os.path.dirname(__file__)
RECORDINGS_DIR  = os.path.join(BASE_DIR, "recordings")
os.makedirs(RECORDINGS_DIR, exist_ok=True)

_raw_key     = os.environ["VONAGE_PRIVATE_KEY"]
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
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS numbers (
                    number TEXT PRIMARY KEY,
                    name TEXT DEFAULT '',
                    paused BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS schedule (
                    id SERIAL PRIMARY KEY,
                    day INT NOT NULL,
                    hour INT NOT NULL,
                    minute INT NOT NULL,
                    UNIQUE(day, hour, minute)
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
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
                CREATE TABLE IF NOT EXISTS recording_meta (
                    id SERIAL PRIMARY KEY,
                    url TEXT,
                    date TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    size_bytes INT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS book (
                    id SERIAL PRIMARY KEY,
                    title TEXT DEFAULT '',
                    portions JSONB DEFAULT '[]',
                    current_index INT DEFAULT 0
                );
            """)
        conn.commit()
    print("Database initialized.")

# ── Settings ──────────────────────────────────────────────────────────────────

settings_lock = threading.Lock()

def get_setting(key, default="false"):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception:
        return default

def set_setting(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """, (key, str(value)))
            conn.commit()
    except Exception as e:
        print(f"set_setting error: {e}")

def get_reading_enabled():
    return get_setting("reading_enabled", "false") == "true"

def set_reading_enabled(value: bool):
    set_setting("reading_enabled", "true" if value else "false")

def get_record_enabled():
    return get_setting("record_enabled", "true") == "true"

def set_record_enabled(value: bool):
    set_setting("record_enabled", "true" if value else "false")

def get_replay_enabled():
    return get_setting("replay_enabled", "true") == "true"

def set_replay_enabled(value: bool):
    set_setting("replay_enabled", "true" if value else "false")

def get_announcements_enabled():
    return get_setting("announcements_enabled", "true") == "true"

def set_announcements_enabled(value: bool):
    set_setting("announcements_enabled", "true" if value else "false")

# ── Auth ──────────────────────────────────────────────────────────────────────

def account_exists():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users LIMIT 1")
                return cur.fetchone() is not None
    except Exception:
        return False

def create_account(username, password):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s) ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash",
                (username.strip().lower(), generate_password_hash(password))
            )
        conn.commit()

def check_credentials(username, password):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT password_hash FROM users WHERE username=%s", (username.strip().lower(),))
                row = cur.fetchone()
                return row and check_password_hash(row[0], password)
    except Exception:
        return False

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
        if check_credentials(request.form.get("username",""), request.form.get("password","")):
            session["logged_in"] = True
            session.permanent = True
            return redirect(request.args.get("next") or "/status")
        error = "<div class='err'>Incorrect username or password.</div>"
    next_h = f"<input type='hidden' name='next' value='{request.args.get('next','')}'>" if request.args.get("next") else ""
    return f"""<!DOCTYPE html><html lang='en'><head>
  <meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Sign In — Conference Manager</title>
  <style>{_AUTH_CSS}</style></head><body>
  <div class='card'>
    <div><h1>Conference Manager</h1><p class='sub'>Sign in to continue</p></div>
    {error}
    <form method='POST'>{next_h}
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
  <meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Create Account — Conference Manager</title>
  <style>{_AUTH_CSS}</style></head><body>
  <div class='card'>
    <div><h1>Create Your Account</h1><p class='sub'>Set up your admin account.</p></div>
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

# ── Numbers ───────────────────────────────────────────────────────────────────

def get_numbers():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT number, name, paused FROM numbers ORDER BY created_at")
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"get_numbers error: {e}")
        return []

def get_active_numbers():
    return [r["number"] for r in get_numbers() if not r["paused"]]

def get_name(number):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM numbers WHERE number=%s", (number,))
                row = cur.fetchone()
                return row[0] if row else ""
    except Exception:
        return ""

def add_number(number, name=""):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO numbers (number, name) VALUES (%s, %s)
                ON CONFLICT (number) DO UPDATE SET name=EXCLUDED.name, paused=FALSE
            """, (number, name))
        conn.commit()

def remove_number(number):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM numbers WHERE number=%s", (number,))
        conn.commit()

def set_number_name(number, name):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE numbers SET name=%s WHERE number=%s", (name, number))
        conn.commit()

def pause_number(number, paused=True):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE numbers SET paused=%s WHERE number=%s", (paused, number))
        conn.commit()

# ── Schedule ──────────────────────────────────────────────────────────────────

DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

def load_schedule():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT day, hour, minute FROM schedule ORDER BY day, hour, minute")
                return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []

def add_schedule_entry(day, hour, minute):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO schedule (day, hour, minute) VALUES (%s, %s, %s)
                ON CONFLICT (day, hour, minute) DO NOTHING
            """, (day, hour, minute))
        conn.commit()

def remove_schedule_entry(day, hour, minute):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule WHERE day=%s AND hour=%s AND minute=%s", (day, hour, minute))
        conn.commit()

def fmt_schedule_entry(e):
    h, m = e["hour"], e["minute"]
    ampm = "AM" if h < 12 else "PM"
    h12  = h % 12 or 12
    return f"{DAYS[e['day']]}  {h12}:{m:02d} {ampm} ET"

# ── Call logs ─────────────────────────────────────────────────────────────────

current_run_id = None
run_lock = threading.Lock()

def start_run_log():
    global current_run_id
    now = datetime.now(EASTERN)
    with run_lock:
        current_run_id = now.strftime("%Y%m%d%H%M%S")
    return current_run_id

def log_call(run_id, number, name, status, uuid=None, error=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO call_logs (run_time, number, name, status, uuid, error)
                    VALUES (NOW(), %s, %s, %s, %s, %s)
                    RETURNING id
                """, (number, name, status, uuid, error))
                row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
    except Exception as e:
        print(f"log_call error: {e}")
        return None

def update_call_log(log_id, status):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE call_logs SET status=%s WHERE id=%s", (status, log_id))
            conn.commit()
    except Exception as e:
        print(f"update_call_log error: {e}")

def get_last_run_calls():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get calls from the most recent run (last 2 hours)
                cur.execute("""
                    SELECT number, name, status, uuid, error, run_time
                    FROM call_logs
                    WHERE run_time >= NOW() - INTERVAL '2 hours'
                    ORDER BY id DESC
                    LIMIT 100
                """)
                rows = cur.fetchall()
                if not rows:
                    return None, []
                run_time = rows[0]["run_time"].astimezone(EASTERN).strftime("%A %b %d at %-I:%M %p %Z")
                return run_time, [dict(r) for r in rows]
    except Exception as e:
        print(f"get_last_run_calls error: {e}")
        return None, []

def get_call_history(limit=50):
    """Get full call history for the logs page."""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT number, name, status, uuid, error,
                           run_time AT TIME ZONE 'America/New_York' as run_time_et
                    FROM call_logs
                    ORDER BY id DESC
                    LIMIT %s
                """, (limit,))
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"get_call_history error: {e}")
        return []

# ── Recording metadata ────────────────────────────────────────────────────────

def load_recording_meta():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM recording_meta ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception:
        return {}

def save_recording_meta(data):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO recording_meta (url, date, start_time, end_time, size_bytes)
                    VALUES (%s, %s, %s, %s, %s)
                """, (data.get("url"), data.get("date"), data.get("start_time"),
                      data.get("end_time"), data.get("size_bytes", 0)))
            conn.commit()
    except Exception as e:
        print(f"save_recording_meta error: {e}")

def _vonage_jwt():
    import jwt as pyjwt
    now = int(time.time())
    payload = {"application_id": VONAGE_APP_ID, "iat": now,
               "jti": str(_uuid.uuid4()), "exp": now + 300}
    key = _private_key.encode() if isinstance(_private_key, str) else _private_key
    return pyjwt.encode(payload, key, algorithm="RS256")

def download_recording(url):
    try:
        token = _vonage_jwt()
        resp  = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if resp.status_code == 200:
            with open(os.path.join(RECORDINGS_DIR, "latest.mp3"), "wb") as f:
                f.write(resp.content)
            return True
        print(f"Recording download failed: {resp.status_code}")
    except Exception as e:
        print(f"Recording download error: {e}")
    return False

# ── Book management ───────────────────────────────────────────────────────────

book_lock = threading.Lock()

def load_book():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM book ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    return {"portions": row["portions"], "current_index": row["current_index"], "title": row["title"], "id": row["id"]}
    except Exception as e:
        print(f"load_book error: {e}")
    return {"portions": [], "current_index": 0, "title": ""}

def save_book(data):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                if data.get("id"):
                    cur.execute("UPDATE book SET title=%s, portions=%s, current_index=%s WHERE id=%s",
                                (data["title"], json.dumps(data["portions"]), data["current_index"], data["id"]))
                else:
                    cur.execute("DELETE FROM book")
                    cur.execute("INSERT INTO book (title, portions, current_index) VALUES (%s, %s, %s)",
                                (data["title"], json.dumps(data["portions"]), data["current_index"]))
            conn.commit()
    except Exception as e:
        print(f"save_book error: {e}")

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
    if triggered or not expected or not votes or votes < expected:
        return
    if votes != expected:
        return
    with vote_lock:
        if reading_session["triggered"]:
            return
        reading_session["triggered"] = True
        first_uuid = next(iter(votes))
    reading = get_todays_reading()
    if not reading:
        return
    print("All participants voted yes — playing reading.")
    try:
        client.voice.play_tts_into_call(first_uuid, TtsStreamOptions(text=reading, language="en-US"))
    except Exception as e:
        print(f"Failed to play reading: {e}")

# ── Call state ────────────────────────────────────────────────────────────────

last_run   = {"time": None, "calls": [], "running": False}
call_status_map = {}   # uuid → {number, name, status, log_id}
inbound_uuid_map = {}
log_lock = threading.Lock()

# ── Announcement queue ────────────────────────────────────────────────────────

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
                    client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US"))
                except Exception as e:
                    print(f"Announce failed for {u}: {e}")
            time.sleep(max(3.0, len(text) / 10) + 1.0)
        except Exception as e:
            print(f"Announcement worker error: {e}")
        finally:
            _ann_queue.task_done()

threading.Thread(target=_announcement_worker, daemon=True).start()

def announce_join(name, exclude_uuid=None, delay=0):
    if not name:
        return
    if delay:
        time.sleep(delay)
    _ann_queue.put((name, exclude_uuid))

# ── Dialing ───────────────────────────────────────────────────────────────────

def dial(number):
    name = get_name(number)
    try:
        response = client.voice.create_call(CreateCallRequest(
            to=[ToPhone(number=number)],
            from_=Phone(number=FROM_NUMBER),
            answer_url=[f"{BASE_URL}/answer"],
            event_url=[f"{BASE_URL}/event"],
            machine_detection="hangup",
        ))
        uuid    = getattr(response, "uuid", None)
        log_id  = log_call(None, number, name, "dialing", uuid=uuid)
        entry   = {"number": number, "name": name, "status": "dialing", "uuid": uuid, "log_id": log_id}
        with log_lock:
            if uuid:
                call_status_map[uuid] = entry
            last_run["calls"].append(entry)
    except Exception as e:
        log_call(None, number, name, "error", error=str(e))
        with log_lock:
            last_run["calls"].append({"number": number, "name": name, "status": "error", "error": str(e)})

def _play_participant_summary():
    time.sleep(12)
    with log_lock:
        connected_names = [e["name"] for e in last_run.get("calls", [])
                           if e.get("status") == "connected" and e.get("name")]
        uuids = [u for u, e in call_status_map.items() if e.get("status") == "connected"]
    if not connected_names or not uuids:
        return
    if len(connected_names) == 1:
        text = f"Welcome. {connected_names[0]} has joined the call."
    elif len(connected_names) == 2:
        text = f"Welcome everyone. {connected_names[0]} and {connected_names[1]} have joined the call."
    else:
        names_list = ", ".join(connected_names[:-1]) + ", and " + connected_names[-1]
        text = f"Welcome everyone. The following participants have joined: {names_list}."
    for u in uuids:
        try:
            client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US"))
        except Exception as e:
            print(f"Summary announcement failed for {u}: {e}")

def start_conference():
    with log_lock:
        if last_run["running"]:
            return
        last_run["running"] = True
        last_run["time"] = datetime.now(EASTERN).strftime("%A %b %d at %-I:%M %p %Z")
        last_run["calls"] = []
        call_status_map.clear()
    _reset_reading_session()
    advance_reading()
    print("Starting conference...")
    try:
        for number in get_active_numbers():
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
    return [
        {"action": "talk", "text": "Joining the conference. Press 1 to vote for today's reading to be read aloud."},
        {"action": "input", "type": ["dtmf"], "dtmf": {"maxDigits": 1, "timeOut": 8},
         "eventUrl": [f"{BASE_URL}/reading-vote"]},
        *_conference_ncco()
    ]

def _plain_ncco():
    return [
        {"action": "talk", "text": "Joining you into the conference."},
        *_conference_ncco()
    ]

def _inbound_join_ncco():
    return [
        {"action": "talk", "text": "Joining the conference. Press 1 to announce your arrival."},
        {"action": "input", "type": ["dtmf"], "dtmf": {"maxDigits": 1, "timeOut": 6},
         "eventUrl": [f"{BASE_URL}/join-announce"]},
        *_conference_ncco()
    ]

def _replay_ncco():
    meta = load_recording_meta()
    date_str = meta.get("date", "a previous session")
    return [
        {"action": "talk", "text": f"The conference from {date_str} is now playing."},
        {"action": "stream", "streamUrl": [f"{BASE_URL}/recordings/audio"], "level": 0},
        {"action": "talk", "text": "You have reached the end of the recording. Goodbye."},
    ]

def _answer_ncco(uuid=None, inbound=False):
    if inbound:
        with log_lock:
            running = last_run.get("running", False)
        if not running and get_replay_enabled():
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
        _handle_inbound_announcement(uuid, data.get("from", ""))
    return jsonify(_answer_ncco(uuid=uuid, inbound=is_inbound))

@app.route("/inbound", methods=["GET","POST"])
def inbound():
    data = request.get_json(silent=True) or request.values
    uuid = data.get("uuid", "")
    _handle_inbound_announcement(uuid, data.get("from", ""))
    return jsonify(_answer_ncco(uuid=uuid, inbound=True))

@app.route("/join-announce", methods=["GET","POST"])
def join_announce():
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
    data = request.get_json(silent=True) or {}
    uuid = data.get("uuid", "")
    dtmf = (data.get("dtmf") or {}).get("digits", "") or data.get("digits", "")
    if str(dtmf).strip() == "1" and uuid:
        threading.Thread(target=_record_vote, args=(uuid,), daemon=True).start()
    return jsonify(_conference_ncco())

@app.route("/event", methods=["GET","POST"])
def event():
    data   = request.get_json(silent=True) or {}
    uuid   = data.get("uuid", "")
    status = data.get("status", "")
    print(f"Event: {status} -> {data.get('to','')}")
    with log_lock:
        if uuid in call_status_map:
            entry = call_status_map[uuid]
            log_id = entry.get("log_id")
            if status == "answered":
                entry["status"] = "connected"
                if log_id:
                    threading.Thread(target=update_call_log, args=(log_id, "connected"), daemon=True).start()
                if get_reading_enabled() and get_todays_reading():
                    threading.Thread(target=_mark_answered, args=(uuid,), daemon=True).start()
            elif status == "machine":
                entry["status"] = "voicemail"
                if log_id:
                    threading.Thread(target=update_call_log, args=(log_id, "voicemail"), daemon=True).start()
            elif status in ("completed","busy","cancelled","failed","rejected","unanswered","timeout"):
                if entry["status"] not in ("connected","voicemail"):
                    entry["status"] = status
                    if log_id:
                        threading.Thread(target=update_call_log, args=(log_id, status), daemon=True).start()
    return "OK", 200

@app.route("/recording", methods=["GET","POST"])
def recording_webhook():
    data       = request.get_json(silent=True) or request.values.to_dict()
    rec_url    = data.get("recording_url") or data.get("url")
    start_time = data.get("start_time", "")
    size_bytes = data.get("size", 0)
    if rec_url:
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            date_str = dt.astimezone(EASTERN).strftime("%A %B %-d, %Y at %-I:%M %p ET")
        except Exception:
            date_str = start_time or "unknown"
        def _do_download():
            ok = download_recording(rec_url)
            if ok:
                save_recording_meta({"url": rec_url, "date": date_str,
                                     "start_time": start_time, "size_bytes": int(size_bytes)})
        threading.Thread(target=_do_download, daemon=True).start()
    return "OK", 200

@app.route("/recordings/audio")
def recording_audio():
    from flask import send_from_directory
    path = os.path.join(RECORDINGS_DIR, "latest.mp3")
    if not os.path.exists(path):
        return "No recording available", 404
    return send_from_directory(RECORDINGS_DIR, "latest.mp3", mimetype="audio/mpeg")

@app.route("/recording/toggle", methods=["POST"])
@login_required
def recording_toggle():
    set_record_enabled(not get_record_enabled())
    return redirect("/status")

@app.route("/replay/toggle", methods=["POST"])
@login_required
def replay_toggle():
    set_replay_enabled(not get_replay_enabled())
    return redirect("/status")

@app.route("/announcements/toggle", methods=["POST"])
@login_required
def announcements_toggle():
    set_announcements_enabled(not get_announcements_enabled())
    return redirect("/status")

# ── Call History Page ─────────────────────────────────────────────────────────

@app.route("/history")
@login_required
def history():
    calls = get_call_history(limit=200)
    STATUS_ICONS = {
        "connected":  ("✅", "#4ade80"),
        "voicemail":  ("📵", "#fb923c"),
        "dialing":    ("⏳", "#facc15"),
        "busy":       ("🔴", "#f87171"),
        "unanswered": ("🔕", "#94a3b8"),
        "timeout":    ("🔕", "#94a3b8"),
        "failed":     ("❌", "#f87171"),
        "error":      ("❌", "#f87171"),
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
  <title>Call History — Conference Manager</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f1117;color:#e2e8f0;padding:1.5rem 1rem}}
    .wrap{{max-width:700px;margin:0 auto}}
    h1{{font-size:1.3rem;font-weight:700;margin-bottom:1rem}}
    a{{color:#6366f1;text-decoration:none;font-size:.85rem}}
    table{{width:100%;border-collapse:collapse;font-size:.85rem;margin-top:1rem}}
    th{{text-align:left;padding:.5rem .75rem;color:#64748b;font-size:.75rem;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #2d3748}}
    td{{padding:.55rem .75rem;border-bottom:1px solid #1e2433}}
    tr:hover td{{background:#1e2433}}
  </style></head><body>
  <div class='wrap'>
    <div style='display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem'>
      <h1>📋 Call History</h1>
      <a href='/status'>← Back</a>
    </div>
    <table>
      <thead><tr><th>Time (ET)</th><th>Number</th><th>Name</th><th>Status</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div></body></html>"""

# ── Number management ─────────────────────────────────────────────────────────

@app.route("/numbers/add", methods=["POST"])
@login_required
def numbers_add():
    n    = _clean(request.form.get("number", ""))
    name = request.form.get("name", "").strip()
    if n:
        add_number(n, name)
    return redirect("/status")

@app.route("/numbers/remove", methods=["POST"])
@login_required
def numbers_remove():
    n = request.form.get("number", "").strip()
    if n:
        remove_number(n)
    return redirect("/status")

@app.route("/numbers/pause", methods=["POST"])
@login_required
def numbers_pause():
    n = request.form.get("number", "").strip()
    if n:
        pause_number(n, True)
    return redirect("/status")

@app.route("/numbers/unpause", methods=["POST"])
@login_required
def numbers_unpause():
    n = request.form.get("number", "").strip()
    if n:
        pause_number(n, False)
    return redirect("/status")

@app.route("/numbers/setname", methods=["POST"])
@login_required
def numbers_setname():
    n    = request.form.get("number", "").strip()
    name = request.form.get("name", "").strip()
    if n:
        set_number_name(n, name)
    return redirect("/status")

# ── Schedule routes ───────────────────────────────────────────────────────────

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
    return redirect("/status")

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
    return redirect("/status")

# ── Reading / Book routes ─────────────────────────────────────────────────────

@app.route("/reading/toggle", methods=["POST"])
@login_required
def reading_toggle():
    set_reading_enabled(not get_reading_enabled())
    return redirect("/status")

@app.route("/book/upload", methods=["POST"])
@login_required
def book_upload():
    f = request.files.get("book")
    if not f:
        return redirect("/status")
    title = request.form.get("title", f.filename).strip()
    lpp   = int(request.form.get("lines_per_portion", 30))
    text  = f.read().decode("utf-8", errors="replace")
    count = upload_book(text, title=title, lines_per_portion=lpp)
    print(f"Book '{title}' uploaded — {count} portions")
    return redirect("/status")

@app.route("/book/advance", methods=["POST"])
@login_required
def book_advance():
    advance_reading()
    return redirect("/status")

@app.route("/book/remove", methods=["POST"])
@login_required
def book_remove():
    with book_lock:
        save_book({"portions": [], "current_index": 0, "title": ""})
    return redirect("/status")

# ── Trigger ───────────────────────────────────────────────────────────────────

@app.route("/trigger", methods=["POST"])
@login_required
def trigger():
    with log_lock:
        if last_run["running"]:
            return ("A conference is already in progress.", 409)
    threading.Thread(target=start_conference, daemon=True).start()
    return redirect("/status")

# ── Root ──────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def root():
    return redirect("/status")

# ── PWA ───────────────────────────────────────────────────────────────────────

@app.route("/manifest.json")
def manifest():
    from flask import Response
    data = {"name": "Conference Manager", "short_name": "Conference",
            "start_url": "/status", "display": "standalone",
            "background_color": "#0f1117", "theme_color": "#1e2433",
            "icons": [{"src": "/static/icon.svg", "sizes": "any", "type": "image/svg+xml"}]}
    return Response(json.dumps(data), mimetype="application/manifest+json")

@app.route("/sw.js")
def service_worker():
    from flask import Response
    sw = "self.addEventListener('fetch', e => {});"
    return Response(sw, mimetype="application/javascript")

# ── Status page ───────────────────────────────────────────────────────────────

STATUS_ICONS = {
    "connected":  ("✅", "#4ade80"),
    "voicemail":  ("📵", "#fb923c"),
    "dialing":    ("⏳", "#facc15"),
    "busy":       ("🔴", "#f87171"),
    "unanswered": ("🔕", "#94a3b8"),
    "timeout":    ("🔕", "#94a3b8"),
    "failed":     ("❌", "#f87171"),
    "error":      ("❌", "#f87171"),
}

@app.route("/status")
@login_required
def status():
    with log_lock:
        run_time = last_run["time"]
        calls    = list(last_run["calls"])
        running  = last_run["running"]

    with vote_lock:
        voted     = len(reading_session["votes"])
        expected  = len(reading_session["expected"])
        triggered = reading_session["triggered"]

    reading_on = get_reading_enabled()
    numbers    = get_numbers()
    book       = load_book()
    portions   = book.get("portions", [])
    book_title = book.get("title", "")
    book_idx   = book.get("current_index", 0)
    book_total = len(portions)

    raw = FROM_NUMBER.lstrip("1") if FROM_NUMBER.startswith("1") else FROM_NUMBER
    dial_in_fmt = (f"({raw[0:3]}) {raw[3:6]}-{raw[6:10]}" if len(raw) >= 10 else FROM_NUMBER)

    if not run_time:
        run_body = "<p class='muted'>No conference has run yet since the server started.</p>"
    else:
        rows = ""
        for c in calls:
            s = c.get("status", "unknown")
            icon, color = STATUS_ICONS.get(s, ("❓", "#94a3b8"))
            name = c.get("name", "")
            label = f"<span class='cname'>{name}</span>" if name else ""
            err  = f"<span class='err'>({c['error']})</span>" if c.get("error") else ""
            rows += (f"<li><span class='icon'>{icon}</span>"
                     f"<span class='num'>{c['number']}</span>{label}"
                     f"<span class='stat' style='color:{color}'>{s}</span>{err}</li>")
        total     = len(calls)
        connected = sum(1 for c in calls if c.get("status") == "connected")
        badge     = "<span class='live'>● Live</span>" if running else ""
        run_body  = f"""
        <div class='summary'>
          <span class='muted'>Last run: {run_time} {badge}</span>
          <span class='counts'>{connected}/{total} connected</span>
        </div>
        <ul class='calls'>{rows}</ul>
        <a href='/history' style='font-size:.8rem;color:#6366f1;display:block;margin-top:.5rem'>View full call history →</a>"""

    num_rows = ""
    for r in numbers:
        n, name, is_paused = r["number"], r["name"], r["paused"]
        pause_tag    = "<span class='tag paused'>Paused</span>" if is_paused else ""
        disp         = name if name else "<span class='muted'>No name</span>"
        li_cls       = "num-paused" if is_paused else ""
        pause_action = "/numbers/unpause" if is_paused else "/numbers/pause"
        pause_label  = "Resume" if is_paused else "Pause"
        pause_cls    = "unpause-btn" if is_paused else "pause-btn"
        num_rows += f"""
        <li class='{li_cls}'>
          <div class='num-info'><span class='num'>{n}</span>{pause_tag}<span class='nname'>{disp}</span></div>
          <div class='num-actions'>
            <form method='POST' action='/numbers/setname' style='display:flex;gap:.35rem'>
              <input type='hidden' name='number' value='{n}'/>
              <input type='text' name='name' class='name-input' value='{name}' placeholder='Name'/>
              <button class='save-btn'>Save</button>
            </form>
            <form method='POST' action='{pause_action}'>
              <input type='hidden' name='number' value='{n}'/>
              <button class='{pause_cls}'>{pause_label}</button>
            </form>
            <form method='POST' action='/numbers/remove'>
              <input type='hidden' name='number' value='{n}'/>
              <button class='rm-btn' onclick="return confirm('Remove {n}?')">✕</button>
            </form>
          </div>
        </li>"""
    if not num_rows:
        num_rows = "<li class='muted' style='border:none;background:none;padding:.5rem 0'>No numbers yet.</li>"

    toggle_label = "Auto-Read: On" if reading_on else "Auto-Read: Off"
    toggle_cls   = "toggle-on" if reading_on else "toggle-off"
    toggle_hint  = "Participants vote by pressing 1. If all vote yes, the reading plays automatically." if reading_on else "Auto-read is disabled."
    reading_toggle_html = f"""
        <div class='toggle-row'>
          <form method='POST' action='/reading/toggle'>
            <button class='toggle-btn {toggle_cls}'>{toggle_label}</button>
          </form>
          <span class='muted' style='font-size:.8rem'>{toggle_hint}</span>
        </div>"""

    if not portions:
        book_body = reading_toggle_html + "<p class='muted' style='margin-top:.75rem'>No book uploaded.</p>"
    else:
        vbadge = ""
        if reading_on and expected > 0:
            vbadge = "<span style='color:#4ade80;font-size:.8rem'>📖 Reading played</span>" if triggered else f"<span style='color:#a5b4fc;font-size:.8rem'>📖 {voted}/{expected} voted</span>"
        book_body = reading_toggle_html + f"""
        <div class='book-info' style='margin-top:.75rem'>
          <span class='book-title'>{book_title or "Untitled"}</span>
          <span class='muted'>Portion {book_idx + 1} of {book_total}</span>
        </div>
        {f"<div style='margin-bottom:.5rem'>{vbadge}</div>" if vbadge else ""}
        <div class='book-btns'>
          <form method='POST' action='/book/advance'><button class='sec-btn'>Skip to Next Portion</button></form>
          <form method='POST' action='/book/remove' onsubmit="return confirm('Remove this book?')"><button class='rm-btn'>Remove Book</button></form>
        </div>"""

    record_on = get_record_enabled()
    replay_on = get_replay_enabled()
    rec_meta  = load_recording_meta()
    rec_path  = os.path.join(RECORDINGS_DIR, "latest.mp3")
    rec_exists = os.path.exists(rec_path)
    if rec_exists and rec_meta:
        rec_info_html = f"<p class='muted' style='font-size:.82rem;margin:.4rem 0'>Recorded: {rec_meta.get('date','unknown')}</p>"
        rec_dl_html   = "<a href='/recordings/audio' class='sec-btn' style='display:inline-block;text-decoration:none;margin-top:.4rem' download='conference.mp3'>⬇ Download</a>"
    else:
        rec_info_html = "<p class='muted' style='font-size:.82rem;margin:.4rem 0'>No recording saved yet.</p>"
        rec_dl_html   = ""

    rec_toggle_cls    = "toggle-on" if record_on else "toggle-off"
    rec_toggle_label  = "Record: On" if record_on else "Record: Off"
    rec_toggle_hint   = "Conference will be recorded automatically." if record_on else "Enable to record conferences."
    replay_toggle_cls   = "toggle-on" if replay_on else "toggle-off"
    replay_toggle_label = "Replay for Late Callers: On" if replay_on else "Replay for Late Callers: Off"
    replay_toggle_hint  = "Late callers hear the last recording." if replay_on else "Enable so late callers hear playback."
    ann_on = get_announcements_enabled()
    ann_toggle_cls   = "toggle-on" if ann_on else "toggle-off"
    ann_toggle_label = "Join Announcements: On" if ann_on else "Join Announcements: Off"
    ann_toggle_hint  = "Everyone hears '[Name] has joined' when someone connects." if ann_on else "Enable to announce participants."

    schedule   = load_schedule()
    sched_rows = ""
    for e in schedule:
        sched_rows += f"""
        <li class='sched-item'>
          <span class='sched-label'>{fmt_schedule_entry(e)}</span>
          <form method='POST' action='/schedule/remove'>
            <input type='hidden' name='day'    value='{e["day"]}'/>
            <input type='hidden' name='hour'   value='{e["hour"]}'/>
            <input type='hidden' name='minute' value='{e["minute"]}'/>
            <button class='rm-btn' onclick="return confirm('Remove this schedule?')">✕</button>
          </form>
        </li>"""
    if not sched_rows:
        sched_rows = "<li class='muted' style='padding:.4rem 0;border:none;background:none'>No scheduled calls.</li>"

    day_opts     = "".join(f"<option value='{i}'>{d}</option>" for i, d in enumerate(DAYS))
    btn_label    = "● Running…" if running else "▶ Start Conference Now"
    btn_disabled = "disabled" if running else ""

    return f"""<!DOCTYPE html>
<html lang='en'><head>
  <meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Conference Manager</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#1e2433'/>
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
    .toggle-row{{display:flex;align-items:center;gap:.75rem;flex-wrap:wrap}}
    .toggle-btn{{border:none;border-radius:8px;padding:.45rem 1rem;font-size:.85rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .toggle-on{{background:#14532d;color:#86efac}}
    .toggle-on:hover{{background:#166534}}
    .toggle-off{{background:#1e2433;color:#64748b;border:1px solid #2d3748}}
    .toggle-off:hover{{border-color:#6366f1;color:#a5b4fc}}
    .dial-box{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.7rem 1rem;font-size:.9rem;display:flex;align-items:center;justify-content:space-between;gap:.5rem}}
    .dial-num{{font-family:monospace;font-size:1rem;font-weight:700;color:#f8fafc;letter-spacing:.04em}}
    ul.sched li.sched-item{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.55rem .85rem;display:flex;align-items:center;justify-content:space-between;gap:.5rem}}
    .sched-label{{font-size:.88rem;font-family:monospace;color:#e2e8f0}}
    .sched-add{{display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.65rem}}
    .sched-add select,.sched-add input[type=time]{{background:#1e2433;border:1px solid #2d3748;color:#e2e8f0;border-radius:8px;padding:.55rem .75rem;font-size:.85rem}}
    .sched-add select{{flex:1;min-width:120px}}
    .sched-add input[type=time]{{flex:1;min-width:100px;color-scheme:dark}}
    ul{{list-style:none;display:flex;flex-direction:column;gap:.4rem}}
    ul.calls li{{display:flex;align-items:center;gap:.6rem;background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.6rem 1rem;font-size:.85rem}}
    .icon{{min-width:1.2rem}}
    .num{{font-family:monospace}}
    .cname{{color:#94a3b8;font-size:.8rem;margin-left:.2rem;flex:1}}
    .stat{{font-weight:600;text-transform:capitalize;font-size:.8rem}}
    .err{{color:#f87171;font-size:.75rem}}
    ul.nums li{{background:#1e2433;border:1px solid #2d3748;border-radius:8px;padding:.65rem .85rem;display:flex;flex-direction:column;gap:.5rem}}
    .num-info{{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}}
    .nname{{color:#94a3b8;font-size:.8rem;margin-left:auto}}
    .num-actions{{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}}
    .name-input{{flex:1;background:#0f1117;border:1px solid #2d3748;color:#e2e8f0;border-radius:6px;padding:.3rem .6rem;font-size:.8rem;min-width:80px}}
    .name-input:focus{{outline:none;border-color:#3b82f6}}
    .save-btn{{background:#1e3a5f;color:#93c5fd;border:none;border-radius:6px;padding:.3rem .65rem;font-size:.78rem;font-weight:700;cursor:pointer;white-space:nowrap}}
    .save-btn:hover{{background:#1d4ed8;color:#fff}}
    .tag{{font-size:.68rem;font-weight:700;padding:.12rem .4rem;border-radius:4px;letter-spacing:.04em}}
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
  </style>
</head>
<body><div class='wrap'>
  <div style='display:flex;justify-content:space-between;align-items:center'>
    <h1>Conference Manager</h1>
    <form method='POST' action='/logout'>
      <button style='background:none;border:1px solid #2d3748;color:#64748b;border-radius:8px;padding:.35rem .75rem;font-size:.78rem;cursor:pointer'>Sign out</button>
    </form>
  </div>

  <section>
    <form method='POST' action='/trigger' onsubmit="this.querySelector('button').disabled=true;this.querySelector('button').textContent='● Starting…'">
      <button class='trigger-btn' {btn_disabled}>{btn_label}</button>
    </form>
  </section>

  <section>
    <h2>Dial-In Number</h2>
    <div class='dial-box'>
      <span class='muted'>Participants can call in directly:</span>
      <span class='dial-num'>{dial_in_fmt}</span>
    </div>
  </section>

  <section><h2>Last Conference</h2>{run_body}</section>

  <section>
    <h2>Phone Numbers ({len(numbers)})</h2>
    <form method='POST' action='/numbers/add' style='margin-bottom:.75rem'>
      <div class='add-row'>
        <input type='tel'  name='number' placeholder='Number, e.g. 2025551234' required/>
        <input type='text' name='name'   placeholder='Name (optional)'/>
        <button class='add-btn'>+ Add</button>
      </div>
    </form>
    <ul class='nums'>{num_rows}</ul>
  </section>

  <section>
    <h2>Schedule</h2>
    <ul class='sched'>{sched_rows}</ul>
    <form method='POST' action='/schedule/add' class='sched-add'>
      <select name='day'>{day_opts}</select>
      <input type='time' name='time' required/>
      <button class='add-btn'>+ Add</button>
    </form>
    <p class='hint' style='margin-top:.5rem'>Times are Eastern (ET). Changes take effect immediately.</p>
  </section>

  <section>
    <h2>Daily Reading</h2>
    {book_body}
    <details>
      <summary>{'Replace book' if portions else 'Upload a book (.txt)'}</summary>
      <form method='POST' action='/book/upload' enctype='multipart/form-data' class='upload-form'>
        <input type='file' name='book' accept='.txt' required/>
        <input type='text' name='title' placeholder='Book title (optional)'/>
        <input type='number' name='lines_per_portion' value='30' min='5' max='200'/>
        <p class='hint'>Upload a plain .txt file. Each portion is read aloud via text-to-speech only if all participants vote yes.</p>
        <button class='upload-btn'>Upload &amp; Split</button>
      </form>
    </details>
  </section>

  <section>
    <h2>Recording</h2>
    <div class='toggle-row'>
      <form method='POST' action='/recording/toggle'>
        <button class='toggle-btn {rec_toggle_cls}'>{rec_toggle_label}</button>
      </form>
      <span class='muted' style='font-size:.8rem'>{rec_toggle_hint}</span>
    </div>
    <div class='toggle-row' style='margin-top:.6rem'>
      <form method='POST' action='/replay/toggle'>
        <button class='toggle-btn {replay_toggle_cls}'>{replay_toggle_label}</button>
      </form>
      <span class='muted' style='font-size:.8rem'>{replay_toggle_hint}</span>
    </div>
    {rec_info_html}{rec_dl_html}
  </section>

  <section>
    <h2>Join Announcements</h2>
    <div class='toggle-row'>
      <form method='POST' action='/announcements/toggle'>
        <button class='toggle-btn {ann_toggle_cls}'>{ann_toggle_label}</button>
      </form>
      <span class='muted' style='font-size:.8rem'>{ann_toggle_hint}</span>
    </div>
  </section>

  <p class='footer'><span class='tag paused'>Paused</span> numbers are skipped on the next call</p>
</div></body></html>"""

# ── Scheduler ─────────────────────────────────────────────────────────────────

def run_scheduler():
    fired_today: set = set()
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
        time.sleep(30)

# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
