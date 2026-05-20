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

# running:           dial-out loop is still sending calls
# conference_active: at least one person is currently connected in the conference
# pending_calls:     number of outbound calls not yet in a final state
# summary_fired:     whether the welcome announcement has already played this session
last_run   = {"time": None, "calls": [], "running": False,
              "conference_active": False, "pending_calls": 0, "summary_fired": False}
call_status_map  = {}   # uuid → {number, name, status, log_id}
inbound_uuid_map = {}
log_lock = threading.Lock()

FINAL_STATUSES = {"connected", "voicemail", "completed", "busy", "cancelled",
                  "failed", "rejected", "unanswered", "timeout", "error"}

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
    """Wait until every outbound call has reached a final state, then announce
    only the people who are actually connected in the conference."""
    MAX_WAIT = 120   # seconds — absolute ceiling in case some events never arrive
    POLL     = 1     # check every second
    waited   = 0
    while waited < MAX_WAIT:
        time.sleep(POLL)
        waited += POLL
        with log_lock:
            pending = last_run.get("pending_calls", 0)
            dialing_still = last_run.get("running", False)
        # Keep waiting while dial-out is still sending calls OR calls are pending
        if dialing_still or pending > 0:
            continue
        break   # everyone has reached a final state

    with log_lock:
        # Mark summary as fired so it doesn't run again this session
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
            client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US"))
        except Exception as e:
            print(f"Summary announcement failed for {u}: {e}")

def start_conference():
    with log_lock:
        if last_run["running"]:
            return
        last_run["running"]          = True
        last_run["conference_active"] = False
        last_run["pending_calls"]     = 0
        last_run["summary_fired"]     = False
        last_run["time"]  = datetime.now(EASTERN).strftime("%A %b %d at %-I:%M %p %Z")
        last_run["calls"] = []
        call_status_map.clear()
    _reset_reading_session()
    advance_reading()
    print("Starting conference...")
    numbers = get_active_numbers()
    # Set pending count before dialing so the summary waiter sees the right number
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
            conf_active = last_run.get("conference_active", False)
        # Only play recording if conference is NOT currently active
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
            prev_status = entry.get("status", "dialing")

            if status == "answered":
                entry["status"] = "connected"
                last_run["conference_active"] = True
                # Only decrement pending if this was still in a non-final state
                if prev_status not in FINAL_STATUSES:
                    last_run["pending_calls"] = max(0, last_run["pending_calls"] - 1)
                if log_id:
                    threading.Thread(target=update_call_log, args=(log_id, "connected"), daemon=True).start()
                if get_reading_enabled() and get_todays_reading():
                    threading.Thread(target=_mark_answered, args=(uuid,), daemon=True).start()

            elif status == "machine":
                entry["status"] = "voicemail"
                if prev_status not in FINAL_STATUSES:
                    last_run["pending_calls"] = max(0, last_run["pending_calls"] - 1)
                if log_id:
                    threading.Thread(target=update_call_log, args=(log_id, "voicemail"), daemon=True).start()

            elif status in ("completed","busy","cancelled","failed","rejected","unanswered","timeout"):
                if entry["status"] == "connected":
                    # Someone left the conference — check if anyone is still connected
                    still_connected = any(
                        e.get("status") == "connected" and e_uuid != uuid
                        for e_uuid, e in call_status_map.items()
                    )
                    last_run["conference_active"] = still_connected
                else:
                    entry["status"] = status
                    if prev_status not in FINAL_STATUSES:
                        last_run["pending_calls"] = max(0, last_run["pending_calls"] - 1)
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
    n    = _clean(request.form.get("number", "") or (request.json or {}).get("number", ""))
    name = (request.form.get("name", "") or (request.json or {}).get("name", "")).strip()
    if n:
        add_number(n, name)
        return jsonify({"ok": True, "numbers": get_numbers()})
    return jsonify({"ok": False, "error": "Invalid number"}), 400

@app.route("/numbers/remove", methods=["POST"])
@login_required
def numbers_remove():
    n = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    if n:
        remove_number(n)
    return jsonify({"ok": True, "numbers": get_numbers()})

@app.route("/numbers/pause", methods=["POST"])
@login_required
def numbers_pause():
    n = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    if n:
        pause_number(n, True)
    return jsonify({"ok": True, "numbers": get_numbers()})

@app.route("/numbers/unpause", methods=["POST"])
@login_required
def numbers_unpause():
    n = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    if n:
        pause_number(n, False)
    return jsonify({"ok": True, "numbers": get_numbers()})

@app.route("/numbers/setname", methods=["POST"])
@login_required
def numbers_setname():
    n    = (request.form.get("number", "") or (request.json or {}).get("number", "")).strip()
    name = (request.form.get("name", "") or (request.json or {}).get("name", "")).strip()
    if n:
        set_number_name(n, name)
    return jsonify({"ok": True, "numbers": get_numbers()})

# ── Schedule routes ───────────────────────────────────────────────────────────

@app.route("/schedule/add", methods=["POST"])
@login_required
def schedule_add():
    try:
        data   = request.json or {}
        day    = int(request.form.get("day", data.get("day", 0)))
        time_s = request.form.get("time", data.get("time", "22:45"))
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
        data   = request.json or {}
        day    = int(request.form.get("day",    data.get("day",    0)))
        hour   = int(request.form.get("hour",   data.get("hour",   0)))
        minute = int(request.form.get("minute", data.get("minute", 0)))
        remove_schedule_entry(day, hour, minute)
    except (KeyError, ValueError):
        pass
    return jsonify({"ok": True, "schedule": load_schedule()})

# ── Reading / Book routes ─────────────────────────────────────────────────────

@app.route("/reading/toggle", methods=["POST"])
@login_required
def reading_toggle():
    set_reading_enabled(not get_reading_enabled())
    return jsonify({"ok": True, "value": get_reading_enabled()})

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

# ── Trigger ───────────────────────────────────────────────────────────────────

@app.route("/trigger", methods=["POST"])
@login_required
def trigger():
    with log_lock:
        if last_run["running"]:
            return jsonify({"ok": False, "error": "Already running"}), 409
    threading.Thread(target=start_conference, daemon=True).start()
    return jsonify({"ok": True})

# ── Root ──────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def root():
    return redirect("/status")

# ── API state endpoint (used by AJAX to refresh page sections) ────────────────

@app.route("/api/state")
@login_required
def api_state():
    with log_lock:
        run_time = last_run["time"]
        calls    = list(last_run["calls"])
        running  = last_run["running"]
    with vote_lock:
        voted     = len(reading_session["votes"])
        expected  = len(reading_session["expected"])
        triggered = reading_session["triggered"]
    rec_meta  = load_recording_meta()
    rec_exists = os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3"))
    book = load_book()
    return jsonify({
        "running":               running,
        "run_time":              run_time,
        "calls":                 calls,
        "numbers":               get_numbers(),
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
    })

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

@app.route("/status")
@login_required
def status():
    raw = FROM_NUMBER.lstrip("1") if FROM_NUMBER.startswith("1") else FROM_NUMBER
    dial_in_fmt = (f"({raw[0:3]}) {raw[3:6]}-{raw[6:10]}" if len(raw) >= 10 else FROM_NUMBER)
    days_json = json.dumps(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"])
    return f"""<!DOCTYPE html>
<html lang='en'><head>
  <meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
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
    .err-text{{color:#f87171;font-size:.75rem}}
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
    .toast{{position:fixed;bottom:1.5rem;left:50%;transform:translateX(-50%);background:#1e2433;border:1px solid #2d3748;color:#e2e8f0;padding:.6rem 1.25rem;border-radius:10px;font-size:.85rem;opacity:0;transition:opacity .3s;pointer-events:none;z-index:999}}
    .toast.show{{opacity:1}}
  </style>
</head>
<body><div class='wrap'>

  <div style='display:flex;justify-content:space-between;align-items:center'>
    <h1>Conference Manager</h1>
    <form method='POST' action='/logout'>
      <button style='background:none;border:1px solid #2d3748;color:#64748b;border-radius:8px;padding:.35rem .75rem;font-size:.78rem;cursor:pointer'>Sign out</button>
    </form>
  </div>

  <!-- TRIGGER -->
  <section>
    <button class='trigger-btn' id='trigger-btn' onclick='triggerConference()'>▶ Start Conference Now</button>
  </section>

  <!-- DIAL-IN -->
  <section>
    <h2>Dial-In Number</h2>
    <div class='dial-box'>
      <span class='muted'>Participants can call in directly:</span>
      <span class='dial-num'>{dial_in_fmt}</span>
    </div>
  </section>

  <!-- LAST CONFERENCE -->
  <section>
    <h2>Last Conference</h2>
    <div id='last-run'><p class='muted'>Loading...</p></div>
  </section>

  <!-- NUMBERS -->
  <section>
    <h2>Phone Numbers (<span id='num-count'>0</span>)</h2>
    <div class='add-row' style='margin-bottom:.75rem'>
      <input type='tel'  id='new-number' placeholder='Number, e.g. 2025551234'/>
      <input type='text' id='new-name'   placeholder='Name (optional)'/>
      <button class='add-btn' onclick='addNumber()'>+ Add</button>
    </div>
    <ul class='nums' id='numbers-list'><li class='muted'>Loading...</li></ul>
  </section>

  <!-- SCHEDULE -->
  <section>
    <h2>Schedule</h2>
    <ul class='sched' id='schedule-list'><li class='muted'>Loading...</li></ul>
    <div class='sched-add'>
      <select id='sched-day'></select>
      <input type='time' id='sched-time' value='22:45'/>
      <button class='add-btn' onclick='addSchedule()'>+ Add</button>
    </div>
    <p class='hint' style='margin-top:.5rem'>Times are Eastern (ET). Changes take effect immediately.</p>
  </section>

  <!-- DAILY READING -->
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

  <!-- RECORDING -->
  <section>
    <h2>Recording</h2>
    <div id='recording-section'><p class='muted'>Loading...</p></div>
  </section>

  <!-- JOIN ANNOUNCEMENTS -->
  <section>
    <h2>Join Announcements</h2>
    <div id='announcements-section'><p class='muted'>Loading...</p></div>
  </section>

  <p class='footer'><span class='tag paused'>Paused</span> numbers are skipped on the next call</p>
  <div class='toast' id='toast'></div>
</div>

<script>
const DAYS = {days_json};
const STATUS_ICONS = {{
  connected:  ["✅","#4ade80"],
  voicemail:  ["📵","#fb923c"],
  dialing:    ["⏳","#facc15"],
  busy:       ["🔴","#f87171"],
  unanswered: ["🔕","#94a3b8"],
  timeout:    ["🔕","#94a3b8"],
  failed:     ["❌","#f87171"],
  error:      ["❌","#f87171"],
}};

// ── Toast ──────────────────────────────────────────────────────────────────
function toast(msg, dur=2000) {{
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(() => t.classList.remove("show"), dur);
}}

// ── Generic AJAX POST ──────────────────────────────────────────────────────
async function post(url, data={{}}, isForm=false) {{
  let opts;
  if (isForm) {{
    opts = {{ method:"POST", body: data }};
  }} else {{
    opts = {{ method:"POST", headers:{{"Content-Type":"application/json"}}, body:JSON.stringify(data) }};
  }}
  const r = await fetch(url, opts);
  return r.json();
}}

// ── Render helpers ─────────────────────────────────────────────────────────
function toggleBtn(on, label_on, label_off, cls_extra="") {{
  const cls = on ? "toggle-on" : "toggle-off";
  const lbl = on ? label_on : label_off;
  return `<button class="toggle-btn ${{cls}} ${{cls_extra}}" ${{cls_extra}}>
    ${{lbl}}
  </button>`;
}}

function renderNumbers(numbers) {{
  document.getElementById("num-count").textContent = numbers.length;
  const ul = document.getElementById("numbers-list");
  if (!numbers.length) {{
    ul.innerHTML = "<li class='muted' style='border:none;background:none;padding:.5rem 0'>No numbers yet.</li>";
    return;
  }}
  ul.innerHTML = numbers.map(r => {{
    const paused = r.paused;
    const li_cls = paused ? "num-paused" : "";
    const pause_tag = paused ? "<span class='tag paused'>Paused</span>" : "";
    const disp = r.name || "<span class='muted'>No name</span>";
    return `<li class="${{li_cls}}">
      <div class="num-info">
        <span class="num">${{r.number}}</span>${{pause_tag}}
        <span class="nname">${{disp}}</span>
      </div>
      <div class="num-actions">
        <input type="text" class="name-input" value="${{r.name}}" placeholder="Name" id="name-${{r.number}}"/>
        <button class="save-btn" onclick="saveName('${{r.number}}')">Save</button>
        <button class="${{paused?'unpause-btn':'pause-btn'}}" onclick="togglePause('${{r.number}}', ${{paused}})">${{paused?"Resume":"Pause"}}</button>
        <button class="rm-btn" onclick="removeNumber('${{r.number}}')">✕</button>
      </div>
    </li>`;
  }}).join("");
}}

function renderSchedule(schedule) {{
  const ul = document.getElementById("schedule-list");
  if (!schedule.length) {{
    ul.innerHTML = "<li class='muted' style='padding:.4rem 0;border:none;background:none'>No scheduled calls.</li>";
    return;
  }}
  ul.innerHTML = schedule.map(e => {{
    const h=e.hour, m=e.minute;
    const ampm = h<12?"AM":"PM";
    const h12  = h%12||12;
    const label = `${{DAYS[e.day]}}  ${{h12}}:${{String(m).padStart(2,"0")}} ${{ampm}} ET`;
    return `<li class="sched-item">
      <span class="sched-label">${{label}}</span>
      <button class="rm-btn" onclick="removeSchedule(${{e.day}},${{e.hour}},${{e.minute}})">✕</button>
    </li>`;
  }}).join("");
}}

function renderLastRun(s) {{
  const el = document.getElementById("last-run");
  const btn = document.getElementById("trigger-btn");
  if (s.running) {{
    btn.disabled = true;
    btn.textContent = "● Running…";
  }} else {{
    btn.disabled = false;
    btn.textContent = "▶ Start Conference Now";
  }}
  if (!s.run_time) {{
    el.innerHTML = "<p class='muted'>No conference has run yet since the server started.</p>";
    return;
  }}
  const badge = s.running ? "<span class='live'>● Live</span>" : "";
  const calls = s.calls || [];
  const connected = calls.filter(c=>c.status==="connected").length;
  const rows = calls.map(c => {{
    const [icon,color] = STATUS_ICONS[c.status] || ["❓","#94a3b8"];
    const name = c.name ? `<span class="cname">${{c.name}}</span>` : "";
    const err  = c.error ? `<span class="err-text">(${{c.error}})</span>` : "";
    return `<li><span class="icon">${{icon}}</span><span class="num">${{c.number}}</span>${{name}}<span class="stat" style="color:${{color}}">${{c.status}}</span>${{err}}</li>`;
  }}).join("");
  el.innerHTML = `
    <div class="summary">
      <span class="muted">Last run: ${{s.run_time}} ${{badge}}</span>
      <span class="counts">${{connected}}/${{calls.length}} connected</span>
    </div>
    <ul class="calls">${{rows}}</ul>
    <a href="/history" style="font-size:.8rem;color:#6366f1;display:block;margin-top:.5rem">View full call history →</a>`;
}}

function renderReading(s) {{
  const el = document.getElementById("reading-section");
  const on = s.reading_enabled;
  const toggle_hint = on
    ? "Participants vote by pressing 1. If all vote yes, the reading plays automatically."
    : "Auto-read is disabled.";
  let body = `<div class="toggle-row">
    <button class="toggle-btn ${{on?'toggle-on':'toggle-off'}}" onclick="toggleReading()">${{on?"Auto-Read: On":"Auto-Read: Off"}}</button>
    <span class="muted" style="font-size:.8rem">${{toggle_hint}}</span>
  </div>`;
  if (!s.book_total) {{
    body += "<p class='muted' style='margin-top:.75rem'>No book uploaded.</p>";
    document.getElementById("book-upload-summary").textContent = "Upload a book (.txt)";
  }} else {{
    let vbadge = "";
    if (on && s.expected > 0) {{
      vbadge = s.triggered
        ? "<span style='color:#4ade80;font-size:.8rem'>📖 Reading played</span>"
        : `<span style='color:#a5b4fc;font-size:.8rem'>📖 ${{s.voted}}/${{s.expected}} voted</span>`;
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
  const rec_on    = s.record_enabled;
  const replay_on = s.replay_enabled;
  const rec_hint   = rec_on ? "Conference will be recorded automatically." : "Enable to record conferences.";
  const replay_hint = replay_on ? "Late callers hear the last recording." : "Enable so late callers hear playback.";
  let rec_info = "<p class='muted' style='font-size:.82rem;margin:.4rem 0'>No recording saved yet.</p>";
  let rec_dl   = "";
  if (s.rec_exists && s.rec_meta && s.rec_meta.date) {{
    rec_info = `<p class='muted' style='font-size:.82rem;margin:.4rem 0'>Recorded: ${{s.rec_meta.date}}</p>`;
    rec_dl   = `<a href='/recordings/audio' class='sec-btn' style='display:inline-block;text-decoration:none;margin-top:.4rem' download='conference.mp3'>⬇ Download</a>`;
  }}
  el.innerHTML = `
    <div class="toggle-row">
      <button class="toggle-btn ${{rec_on?'toggle-on':'toggle-off'}}" onclick="toggleRecording()">${{rec_on?"Record: On":"Record: Off"}}</button>
      <span class="muted" style="font-size:.8rem">${{rec_hint}}</span>
    </div>
    <div class="toggle-row" style="margin-top:.6rem">
      <button class="toggle-btn ${{replay_on?'toggle-on':'toggle-off'}}" onclick="toggleReplay()">${{replay_on?"Replay for Late Callers: On":"Replay for Late Callers: Off"}}</button>
      <span class="muted" style="font-size:.8rem">${{replay_hint}}</span>
    </div>
    ${{rec_info}}${{rec_dl}}`;
}}

function renderAnnouncements(s) {{
  const el = document.getElementById("announcements-section");
  const on = s.announcements_enabled;
  const hint = on
    ? "Everyone hears '[Name] has joined' when someone connects."
    : "Enable to announce participants.";
  el.innerHTML = `<div class="toggle-row">
    <button class="toggle-btn ${{on?'toggle-on':'toggle-off'}}" onclick="toggleAnnouncements()">${{on?"Join Announcements: On":"Join Announcements: Off"}}</button>
    <span class="muted" style="font-size:.8rem">${{hint}}</span>
  </div>`;
}}

// ── Full state refresh ─────────────────────────────────────────────────────
async function refresh() {{
  try {{
    const s = await fetch("/api/state").then(r=>r.json());
    renderLastRun(s);
    renderNumbers(s.numbers);
    renderSchedule(s.schedule);
    renderReading(s);
    renderRecording(s);
    renderAnnouncements(s);
  }} catch(e) {{
    console.error("Refresh error", e);
  }}
}}

// ── Actions ────────────────────────────────────────────────────────────────
async function triggerConference() {{
  const btn = document.getElementById("trigger-btn");
  btn.disabled = true;
  btn.textContent = "● Starting…";
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
    renderNumbers(r.numbers);
    toast("Number added");
  }} else {{ toast("Invalid number"); }}
}}

async function removeNumber(n) {{
  if (!confirm(`Remove ${{n}}?`)) return;
  const r = await post("/numbers/remove", {{number:n}});
  if (r.ok) {{ renderNumbers(r.numbers); toast("Removed"); }}
}}

async function togglePause(n, currently_paused) {{
  const url = currently_paused ? "/numbers/unpause" : "/numbers/pause";
  const r = await post(url, {{number:n}});
  if (r.ok) {{ renderNumbers(r.numbers); toast(currently_paused?"Resumed":"Paused"); }}
}}

async function saveName(n) {{
  const name = document.getElementById(`name-${{n}}`).value.trim();
  const r = await post("/numbers/setname", {{number:n, name}});
  if (r.ok) {{ renderNumbers(r.numbers); toast("Name saved"); }}
}}

async function addSchedule() {{
  const day  = document.getElementById("sched-day").value;
  const time = document.getElementById("sched-time").value;
  if (!time) return;
  const r = await post("/schedule/add", {{day:parseInt(day), time}});
  if (r.ok) {{ renderSchedule(r.schedule); toast("Schedule added"); }}
}}

async function removeSchedule(day, hour, minute) {{
  if (!confirm("Remove this schedule?")) return;
  const r = await post("/schedule/remove", {{day, hour, minute}});
  if (r.ok) {{ renderSchedule(r.schedule); toast("Removed"); }}
}}

async function toggleReading() {{
  await post("/reading/toggle");
  refresh();
}}

async function bookAdvance() {{
  await post("/book/advance");
  refresh();
}}

async function bookRemove() {{
  if (!confirm("Remove this book?")) return;
  await post("/book/remove");
  refresh();
  toast("Book removed");
}}

async function uploadBook() {{
  const form = document.getElementById("book-upload-form");
  const data = new FormData(form);
  const r = await post("/book/upload", data, true);
  if (r.ok) {{ refresh(); toast(`Book uploaded — ${{r.count}} portions`); document.getElementById("book-upload-details").open=false; }}
  else {{ toast("Upload failed"); }}
}}

async function toggleRecording() {{
  await post("/recording/toggle");
  refresh();
}}

async function toggleReplay() {{
  await post("/replay/toggle");
  refresh();
}}

async function toggleAnnouncements() {{
  await post("/announcements/toggle");
  refresh();
}}

// ── Init ───────────────────────────────────────────────────────────────────
// Populate day selector
const dayEl = document.getElementById("sched-day");
DAYS.forEach((d,i) => {{
  const o = document.createElement("option");
  o.value = i; o.textContent = d;
  dayEl.appendChild(o);
}});

// Initial load
refresh();

// Auto-refresh every 10 seconds when conference is running
setInterval(async () => {{
  const btn = document.getElementById("trigger-btn");
  if (btn.disabled) refresh();
}}, 10000);

// Service worker
if ("serviceWorker" in navigator) {{
  navigator.serviceWorker.register("/sw.js").catch(()=>{{}});
}}
</script>
</body></html>"""

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
