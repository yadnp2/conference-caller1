import vonage, threading, time, os, json, functools, uuid as _uuid, requests, gspread, psycopg2, schedule as _schedule
from psycopg2.extras import RealDictCursor
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, redirect, session, url_for, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from google.oauth2.service_account import Credentials as _SACredentials
from vonage_voice.models import CreateCallRequest, ToPhone, Phone, TtsStreamOptions

app = Flask(__name__)
app.secret_key = os.environ["SESSION_SECRET"]

VONAGE_APP_ID   = os.environ["VONAGE_APP_ID"]
FROM_NUMBER     = os.environ["FROM_NUMBER"]
BASE_URL        = os.environ["BASE_URL"].rstrip("/")
CONFERENCE_NAME = "DailyConference"
EASTERN         = ZoneInfo("America/New_York")
RECORDINGS_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recordings")
os.makedirs(RECORDINGS_DIR, exist_ok=True)

_raw_key     = os.environ["VONAGE_PRIVATE_KEY"]
_private_key = _raw_key.replace("\\n", "\n")

client = vonage.Vonage(vonage.Auth(application_id=VONAGE_APP_ID, private_key=_private_key))

# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS members (
                    number TEXT PRIMARY KEY,
                    name TEXT DEFAULT '',
                    paused BOOLEAN DEFAULT FALSE,
                    source TEXT DEFAULT 'sheet'
                );
                CREATE TABLE IF NOT EXISTS schedule (
                    day INT,
                    hour INT,
                    minute INT,
                    PRIMARY KEY (day, hour, minute)
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT 'true'
                );
                CREATE TABLE IF NOT EXISTS call_logs (
                    id SERIAL PRIMARY KEY,
                    run_time TIMESTAMPTZ DEFAULT NOW(),
                    number TEXT NOT NULL,
                    name TEXT DEFAULT '',
                    status TEXT NOT NULL,
                    uuid TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS recording_meta (
                    id SERIAL PRIMARY KEY,
                    url TEXT,
                    date TEXT,
                    size_bytes INT DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            for key in ('record_enabled', 'replay_enabled', 'announcements_enabled'):
                cur.execute("INSERT INTO settings (key,value) VALUES (%s,'true') ON CONFLICT DO NOTHING", (key,))
        conn.commit()
    print("DB initialized.")

def get_setting(key, default="true"):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
                r = cur.fetchone()
                return r[0] if r else default
    except:
        return default

def set_setting(key, value):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", (key, str(value)))
        conn.commit()

# ── AUTH ──────────────────────────────────────────────────────────────────────

def account_exists():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users LIMIT 1")
                return cur.fetchone() is not None
    except:
        return False

def create_account(username, password):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users(username,password_hash) VALUES(%s,%s) ON CONFLICT(username) DO UPDATE SET password_hash=EXCLUDED.password_hash",
                        (username.strip().lower(), generate_password_hash(password)))
        conn.commit()

def check_credentials(username, password):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT password_hash FROM users WHERE username=%s", (username.strip().lower(),))
                r = cur.fetchone()
                return r and check_password_hash(r[0], password)
    except:
        return False

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

# ── MEMBERS ───────────────────────────────────────────────────────────────────

def _clean(n):
    n = str(n).strip().replace(" ","").replace("-","").replace("(","").replace(")","")
    if n.isdigit() and len(n) >= 10:
        return n if n.startswith("1") else "1" + n
    return None

def get_members():
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT number, name, paused, source FROM members ORDER BY name, number")
            return [dict(r) for r in cur.fetchall()]

def get_active_numbers():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT number FROM members WHERE paused=FALSE")
            return [r[0] for r in cur.fetchall()]

def is_approved_member(number):
    if not number:
        return False
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM members WHERE number=%s", (number,))
                return cur.fetchone() is not None
    except:
        return False

def get_name(number):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM members WHERE number=%s", (number,))
                r = cur.fetchone()
                return r[0] if r else ""
    except:
        return ""

def add_member(number, name="", source="local"):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO members(number,name,source) VALUES(%s,%s,%s) ON CONFLICT(number) DO UPDATE SET name=EXCLUDED.name, source=EXCLUDED.source, paused=FALSE",
                        (number, name, source))
        conn.commit()

def remove_member(number):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM members WHERE number=%s", (number,))
        conn.commit()

def set_member_paused(number, paused):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE members SET paused=%s WHERE number=%s", (paused, number))
        conn.commit()

def set_member_name(number, name):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE members SET name=%s WHERE number=%s", (name, number))
        conn.commit()

# ── GOOGLE SHEETS SYNC ────────────────────────────────────────────────────────

def sync_from_sheets():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","")
    sid = os.environ.get("SPREADSHEET_ID","").strip()
    if not raw or not sid:
        return False, "Missing GOOGLE_SERVICE_ACCOUNT_JSON or SPREADSHEET_ID"
    try:
        info  = json.loads(raw)
        creds = _SACredentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        gc    = gspread.authorize(creds)
        ws    = gc.open_by_key(sid).sheet1
        rows  = ws.get("A2:B") or []
    except Exception as e:
        return False, f"Sheet error: {e}"
    imported = 0
    for row in rows:
        name   = (row[0] if row else "").strip()
        raw_n  = (row[1] if len(row) > 1 else "").strip()
        clean  = _clean(raw_n)
        if not clean:
            continue
        add_member(clean, name, source="sheet")
        imported += 1
    return True, f"Synced {imported} number(s) from Google Sheet"

# ── SCHEDULE ──────────────────────────────────────────────────────────────────

DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

def load_schedule():
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT day,hour,minute FROM schedule ORDER BY day,hour,minute")
            return [dict(r) for r in cur.fetchall()]

def set_day_schedule(day, hour, minute):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule WHERE day=%s", (day,))
            cur.execute("INSERT INTO schedule(day,hour,minute) VALUES(%s,%s,%s)", (day,hour,minute))
        conn.commit()

def clear_day_schedule(day):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule WHERE day=%s", (day,))
        conn.commit()

# ── CALL LOGS ─────────────────────────────────────────────────────────────────

def log_call(number, name, status, uuid=None, error=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO call_logs(number,name,status,uuid,error) VALUES(%s,%s,%s,%s,%s) RETURNING id",
                            (number, name, status, uuid, error))
                r = cur.fetchone()
            conn.commit()
            return r[0] if r else None
    except Exception as e:
        print(f"log_call error: {e}")
        return None

def update_log(log_id, status):
    if not log_id: return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE call_logs SET status=%s WHERE id=%s", (status, log_id))
            conn.commit()
    except: pass

def get_call_history(limit=200):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT number,name,status,run_time AT TIME ZONE 'America/New_York' as run_time_et
                               FROM call_logs ORDER BY id DESC LIMIT %s""", (limit,))
                return [dict(r) for r in cur.fetchall()]
    except: return []

# ── RECORDING ─────────────────────────────────────────────────────────────────

def save_recording_meta(url, date_str, size=0):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO recording_meta(url,date,size_bytes) VALUES(%s,%s,%s)", (url,date_str,size))
            conn.commit()
    except: pass

def get_latest_recording_meta():
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT date,size_bytes FROM recording_meta ORDER BY id DESC LIMIT 1")
                r = cur.fetchone()
                return dict(r) if r else {}
    except: return {}

def download_recording(url):
    try:
        import jwt as pyjwt
        now     = int(time.time())
        payload = {"application_id": VONAGE_APP_ID, "iat": now, "jti": str(_uuid.uuid4()), "exp": now+300}
        key     = _private_key.encode() if isinstance(_private_key, str) else _private_key
        token   = pyjwt.encode(payload, key, algorithm="RS256")
        resp    = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if resp.status_code == 200:
            with open(os.path.join(RECORDINGS_DIR, "latest.mp3"), "wb") as f:
                f.write(resp.content)
            return True
    except Exception as e:
        print(f"Recording download error: {e}")
    return False

# ── CALL STATE ────────────────────────────────────────────────────────────────

lock            = threading.Lock()
call_map        = {}   # uuid → {number, name, status, log_id}
session_blocked = set()
last_run        = {"time": None, "calls": [], "running": False,
                   "conference_active": False, "pending": 0, "summary_fired": False}

FINAL = {"connected","voicemail","completed","busy","cancelled","failed","rejected","unanswered","timeout","error"}

# ── ANNOUNCEMENT ──────────────────────────────────────────────────────────────

def _play_summary():
    """Wait until all outbound calls settle, then announce who joined."""
    MAX_WAIT = 120
    waited   = 0
    while waited < MAX_WAIT:
        time.sleep(1)
        waited += 1
        with lock:
            still_dialing = last_run.get("running", False)
            pending       = last_run.get("pending", 0)
        if still_dialing or pending > 0:
            continue
        break

    with lock:
        if last_run.get("summary_fired"):
            return
        last_run["summary_fired"] = True
        names = [e["name"] for e in last_run["calls"] if e.get("status")=="connected" and e.get("name")]
        uuids = [u for u,e in call_map.items() if e.get("status")=="connected"]

    if not names or not uuids:
        print("Summary: no connected participants.")
        return

    if len(names) == 1:
        text = f"Welcome. {names[0]} has joined the call."
    elif len(names) == 2:
        text = f"Welcome everyone. {names[0]} and {names[1]} have joined."
    else:
        text = f"Welcome everyone. {', '.join(names[:-1])}, and {names[-1]} have joined."

    print(f"Summary: {text}")
    for u in uuids:
        try:
            client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US", level=1.0))
        except Exception as e:
            print(f"Summary TTS error {u}: {e}")

# ── DIAL OUT ──────────────────────────────────────────────────────────────────

def dial(number):
    name = get_name(number)
    try:
        resp = client.voice.create_call(CreateCallRequest(
            to=[ToPhone(number=number)],
            from_=Phone(number=FROM_NUMBER),
            answer_url=[f"{BASE_URL}/answer"],
            event_url=[f"{BASE_URL}/event"],
            machine_detection="hangup",
        ))
        uuid   = getattr(resp, "uuid", None)
        log_id = log_call(number, name, "dialing", uuid=uuid)
        entry  = {"number": number, "name": name, "status": "dialing", "uuid": uuid, "log_id": log_id}
        with lock:
            if uuid: call_map[uuid] = entry
            last_run["calls"].append(entry)
    except Exception as e:
        log_call(number, name, "error", error=str(e))
        with lock:
            last_run["calls"].append({"number": number, "name": name, "status": "error", "error": str(e)})

def start_conference():
    with lock:
        if last_run["running"]:
            return
        last_run.update({"running": True, "conference_active": False, "pending": 0,
                         "summary_fired": False, "time": datetime.now(EASTERN).strftime("%A %b %d at %-I:%M %p %Z"),
                         "calls": []})
        call_map.clear()
    session_blocked.clear()
    numbers = get_active_numbers()
    with lock:
        last_run["pending"] = len(numbers)
    print(f"Starting conference — dialing {len(numbers)} members...")
    try:
        for number in numbers:
            dial(number)
            time.sleep(2)
    finally:
        with lock:
            last_run["running"] = False
        if get_setting("announcements_enabled") == "true":
            threading.Thread(target=_play_summary, daemon=True).start()

# ── VONAGE WEBHOOKS ───────────────────────────────────────────────────────────

def _conference_ncco():
    ncco = {"action": "conversation", "name": CONFERENCE_NAME, "startOnEnter": True, "endOnExit": False}
    if get_setting("record_enabled") == "true":
        ncco["record"]    = True
        ncco["eventUrl"]  = [f"{BASE_URL}/recording"]
    return [ncco]

@app.route("/answer", methods=["GET","POST"])
def answer():
    data = request.get_json(silent=True) or request.values
    uuid = data.get("uuid", "")

    with lock:
        is_outbound = uuid in call_map

    if is_outbound:
        # Outbound call answered — just join the conference
        return jsonify([{"action": "talk", "text": "Please hold, joining the conference."}, *_conference_ncco()])

    # Inbound call — check if member is approved
    from_raw = data.get("from", "")
    number   = _clean(from_raw)

    if not number:
        return jsonify([{"action": "talk", "text": "Sorry, calls with a hidden number cannot join. Goodbye."}])

    if not is_approved_member(number):
        return jsonify([{"action": "talk", "text": "Sorry, your number is not registered for this conference. Goodbye."}])

    if number in session_blocked:
        return jsonify([{"action": "talk", "text": "You cannot join this conference session. Goodbye."}])

    # Check if conference is still active
    with lock:
        conf_active = last_run.get("conference_active", False)

    # If conference is over and recording exists, play recording
    if not conf_active and get_setting("replay_enabled") == "true":
        if os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3")):
            meta = get_latest_recording_meta()
            return jsonify([
                {"action": "talk", "text": f"The conference has ended. Playing the recording from {meta.get('date','the last session')}."},
                {"action": "stream", "streamUrl": [f"{BASE_URL}/recordings/audio"], "level": 0},
                {"action": "talk", "text": "Recording complete. Goodbye."},
            ])

    # Conference is active or no recording — join live
    return jsonify([
        {"action": "talk", "text": "Press 1 to join the conference."},
        {"action": "input", "type": ["dtmf"], "dtmf": {"maxDigits": 1, "timeOut": 6},
         "eventUrl": [f"{BASE_URL}/join-press"]},
    ])

@app.route("/join-press", methods=["GET","POST"])
def join_press():
    data  = request.get_json(silent=True) or {}
    digit = (data.get("dtmf") or {}).get("digits","") or data.get("digits","")
    if str(digit).strip() == "1":
        uuid     = data.get("uuid","")
        from_raw = data.get("from","") or data.get("to","")
        number   = _clean(from_raw)
        name     = get_name(number) if number else ""
        if name and get_setting("announcements_enabled") == "true":
            def announce():
                time.sleep(4)
                with lock:
                    uuids = [u for u,e in call_map.items() if e.get("status")=="connected" and u != uuid]
                for u in uuids:
                    try:
                        client.voice.play_tts_into_call(u, TtsStreamOptions(text=f"{name} has joined.", language="en-US", level=1.0))
                    except: pass
            threading.Thread(target=announce, daemon=True).start()
        return jsonify(_conference_ncco())
    return jsonify([{"action": "talk", "text": "Goodbye."}])

@app.route("/event", methods=["GET","POST"])
def event():
    data   = request.get_json(silent=True) or {}
    uuid   = data.get("uuid","")
    status = data.get("status","")
    with lock:
        if uuid in call_map:
            entry   = call_map[uuid]
            log_id  = entry.get("log_id")
            prev    = entry.get("status","dialing")
            if status == "answered":
                entry["status"] = "connected"
                last_run["conference_active"] = True
                if prev not in FINAL:
                    last_run["pending"] = max(0, last_run["pending"] - 1)
                threading.Thread(target=update_log, args=(log_id,"connected"), daemon=True).start()
            elif status == "machine":
                entry["status"] = "voicemail"
                if prev not in FINAL:
                    last_run["pending"] = max(0, last_run["pending"] - 1)
                threading.Thread(target=update_log, args=(log_id,"voicemail"), daemon=True).start()
            elif status in ("completed","busy","failed","rejected","unanswered","timeout","cancelled"):
                if entry["status"] == "connected":
                    still = any(e.get("status")=="connected" and u!=uuid for u,e in call_map.items())
                    last_run["conference_active"] = still
                else:
                    entry["status"] = status
                    if prev not in FINAL:
                        last_run["pending"] = max(0, last_run["pending"] - 1)
                    threading.Thread(target=update_log, args=(log_id, status), daemon=True).start()
    return "OK", 200

@app.route("/recording", methods=["GET","POST"])
def recording_webhook():
    data = request.get_json(silent=True) or {}
    url  = data.get("recording_url") or data.get("url")
    if url:
        date_str = datetime.now(EASTERN).strftime("%A %B %-d at %-I:%M %p ET")
        def _dl():
            ok = download_recording(url)
            if ok:
                save_recording_meta(url, date_str, int(data.get("size",0)))
        threading.Thread(target=_dl, daemon=True).start()
    return "OK", 200

@app.route("/recordings/audio")
def recording_audio():
    path = os.path.join(RECORDINGS_DIR, "latest.mp3")
    if not os.path.exists(path):
        return "No recording", 404
    return send_from_directory(RECORDINGS_DIR, "latest.mp3", mimetype="audio/mpeg")

# ── HANGUP ────────────────────────────────────────────────────────────────────

def _hangup(uuid):
    try:
        client.voice.hangup(uuid)
        return True
    except Exception as e:
        print(f"Hangup error {uuid}: {e}")
        return False

@app.route("/api/live-calls")
@login_required
def live_calls():
    with lock:
        calls = [{"uuid":u,"number":e.get("number",""),"name":e.get("name",""),
                  "status":e.get("status",""),"blocked":e.get("number","") in session_blocked}
                 for u,e in call_map.items() if e.get("status") in ("dialing","connected","answered")]
    # If call_map is empty but we know conference was active, query Vonage directly
    if not calls:
        try:
            import jwt as pyjwt
            now     = int(time.time())
            payload = {"application_id": VONAGE_APP_ID, "iat": now, "jti": str(_uuid.uuid4()), "exp": now+300}
            key     = _private_key.encode() if isinstance(_private_key, str) else _private_key
            token   = pyjwt.encode(payload, key, algorithm="RS256")
            resp    = requests.get("https://api.nexmo.com/v1/calls?status=answered&page_size=20",
                                   headers={"Authorization": f"Bearer {token}"}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for c in data.get("_embedded", {}).get("calls", []):
                    uuid   = c.get("uuid","")
                    number = c.get("to",{}).get("number","")
                    name   = get_name(number) if number else ""
                    calls.append({"uuid": uuid, "number": number, "name": name,
                                  "status": "connected", "blocked": number in session_blocked})
        except Exception as e:
            print(f"Vonage live-calls query error: {e}")
    return jsonify({"calls": calls})

@app.route("/hangup/all", methods=["POST"])
@login_required
def hangup_all():
    data  = request.json or {}
    block = data.get("block", False)
    with lock:
        targets = [(u, e.copy()) for u,e in call_map.items() if e.get("status") in ("dialing","connected","answered")]
    hung = []
    for uuid, entry in targets:
        if _hangup(uuid):
            hung.append(entry.get("number", uuid))
            if block and entry.get("number"):
                session_blocked.add(entry["number"])
    return jsonify({"ok": True, "hung_up": hung})

@app.route("/hangup/one", methods=["POST"])
@login_required
def hangup_one():
    data   = request.json or {}
    uuid   = data.get("uuid","")
    block  = data.get("block", False)
    if not uuid: return jsonify({"ok": False}), 400
    with lock:
        entry = call_map.get(uuid, {})
    number = entry.get("number","")
    ok = _hangup(uuid)
    if ok and block and number:
        session_blocked.add(number)
    return jsonify({"ok": ok})

# ── MEMBER ROUTES ─────────────────────────────────────────────────────────────

@app.route("/numbers/add", methods=["POST"])
@login_required
def numbers_add():
    d = request.json or {}
    n = _clean(d.get("number",""))
    if not n: return jsonify({"ok": False}), 400
    add_member(n, d.get("name","").strip(), source="local")
    return jsonify({"ok": True, "members": get_members()})

@app.route("/numbers/remove", methods=["POST"])
@login_required
def numbers_remove():
    d = request.json or {}
    n = d.get("number","").strip()
    if n: remove_member(n)
    return jsonify({"ok": True, "members": get_members()})

@app.route("/numbers/pause", methods=["POST"])
@login_required
def numbers_pause():
    d = request.json or {}
    n = d.get("number","").strip()
    if n: set_member_paused(n, True)
    return jsonify({"ok": True, "members": get_members()})

@app.route("/numbers/unpause", methods=["POST"])
@login_required
def numbers_unpause():
    d = request.json or {}
    n = d.get("number","").strip()
    if n: set_member_paused(n, False)
    return jsonify({"ok": True, "members": get_members()})

@app.route("/numbers/setname", methods=["POST"])
@login_required
def numbers_setname():
    d    = request.json or {}
    n    = d.get("number","").strip()
    name = d.get("name","").strip()
    if n: set_member_name(n, name)
    return jsonify({"ok": True, "members": get_members()})

# ── SCHEDULE ROUTES ───────────────────────────────────────────────────────────

@app.route("/schedule/set-day", methods=["POST"])
@login_required
def schedule_set_day():
    d = request.json or {}
    try:
        day  = int(d.get("day", 0))
        t    = d.get("time","00:00")
        h, m = [int(x) for x in t.split(":")]
        if 0 <= day <= 6 and 0 <= h <= 23 and 0 <= m <= 59:
            set_day_schedule(day, h, m)
    except: pass
    return jsonify({"ok": True, "schedule": load_schedule()})

@app.route("/schedule/clear-day", methods=["POST"])
@login_required
def schedule_clear_day():
    d = request.json or {}
    try:
        clear_day_schedule(int(d.get("day",0)))
    except: pass
    return jsonify({"ok": True, "schedule": load_schedule()})

# ── SETTINGS ROUTES ───────────────────────────────────────────────────────────

@app.route("/settings/toggle", methods=["POST"])
@login_required
def settings_toggle():
    d   = request.json or {}
    key = d.get("key","")
    if key not in ("record_enabled","replay_enabled","announcements_enabled"):
        return jsonify({"ok": False}), 400
    new = "false" if get_setting(key) == "true" else "true"
    set_setting(key, new)
    return jsonify({"ok": True, "value": new == "true"})

# ── SHEETS SYNC ROUTE ─────────────────────────────────────────────────────────

@app.route("/sheets/sync", methods=["POST"])
@login_required
def sheets_sync():
    ok, msg = sync_from_sheets()
    return jsonify({"ok": ok, "msg": msg, "members": get_members()})

# ── TRIGGER ───────────────────────────────────────────────────────────────────

@app.route("/trigger", methods=["POST"])
@login_required
def trigger():
    with lock:
        if last_run["running"]:
            return jsonify({"ok": False, "error": "Already running"}), 409
    threading.Thread(target=start_conference, daemon=True).start()
    return jsonify({"ok": True})

# ── API STATE ─────────────────────────────────────────────────────────────────

@app.route("/api/state")
@login_required
def api_state():
    with lock:
        run_time = last_run["time"]
        calls    = list(last_run["calls"])
        running  = last_run["running"]
    rec_meta   = get_latest_recording_meta()
    rec_exists = os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3"))
    return jsonify({
        "running":               running,
        "run_time":              run_time,
        "calls":                 calls,
        "members":               get_members(),
        "schedule":              load_schedule(),
        "record_enabled":        get_setting("record_enabled") == "true",
        "replay_enabled":        get_setting("replay_enabled") == "true",
        "announcements_enabled": get_setting("announcements_enabled") == "true",
        "rec_meta":              rec_meta,
        "rec_exists":            rec_exists,
    })

# ── CALL HISTORY ──────────────────────────────────────────────────────────────

@app.route("/history")
@login_required
def history():
    calls = get_call_history()
    rows = ""
    STATUS_COLORS = {"connected":"#22c55e","voicemail":"#f97316","dialing":"#fbbf24",
                     "busy":"#ef4444","unanswered":"#8899bb","failed":"#ef4444","error":"#ef4444"}
    for c in calls:
        color = STATUS_COLORS.get(c.get("status",""), "#8899bb")
        try:
            dt = c["run_time_et"]
            ts = dt.strftime("%-m/%-d %-I:%M %p") if hasattr(dt,"strftime") else str(dt)
        except: ts = ""
        rows += f"<tr><td>{ts}</td><td style='font-family:monospace'>{c['number']}</td><td>{c.get('name','')}</td><td style='color:{color}'>{c.get('status','')}</td></tr>"
    if not rows:
        rows = "<tr><td colspan='4' style='color:#64748b;text-align:center'>No history yet.</td></tr>"
    return f"""<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Call History</title>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap' rel='stylesheet'/>
  <style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:'Inter',sans-serif;background:#0a0f1e;color:#f0f4ff;padding:1.5rem 1rem}}
  .wrap{{max-width:700px;margin:0 auto}}h1{{font-size:1.2rem;font-weight:700;margin-bottom:1rem}}
  a{{color:#3b82f6;text-decoration:none;font-size:.82rem}}table{{width:100%;border-collapse:collapse;font-size:.83rem;margin-top:.75rem}}
  th{{text-align:left;padding:.5rem .75rem;color:#64748b;font-size:.72rem;text-transform:uppercase;border-bottom:1px solid #1f2d45}}
  td{{padding:.55rem .75rem;border-bottom:1px solid #111827}}tr:hover td{{background:#111827}}</style></head>
  <body><div class='wrap'><div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem'>
  <h1>📋 Call History</h1><a href='/status'>← Back</a></div>
  <table><thead><tr><th>Time (ET)</th><th>Number</th><th>Name</th><th>Status</th></tr></thead>
  <tbody>{rows}</tbody></table></div></body></html>"""

# ── AUTH PAGES ────────────────────────────────────────────────────────────────

_AUTH_CSS = """*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:#0a0f1e;color:#f0f4ff;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:1.5rem}
.card{background:#111827;border:1px solid #1f2d45;border-radius:14px;padding:2rem 1.75rem;width:100%;max-width:360px;display:flex;flex-direction:column;gap:1.25rem}
h1{font-size:1.2rem;font-weight:700}
.sub{font-size:.82rem;color:#64748b}
label{font-size:.75rem;font-weight:600;color:#8899bb;display:block;margin-bottom:.3rem}
input{width:100%;background:#0a0f1e;border:1px solid #1f2d45;color:#f0f4ff;border-radius:8px;padding:.6rem .8rem;font-size:.88rem;font-family:'Inter',sans-serif}
input:focus{outline:none;border-color:#3b82f6}
.btn{width:100%;padding:.75rem;background:linear-gradient(135deg,#1d4ed8,#3b82f6);color:#fff;border:none;border-radius:10px;font-size:.95rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}
.err{background:#450a0a;border:1px solid #7f1d1d;color:#fca5a5;border-radius:8px;padding:.55rem .8rem;font-size:.82rem}"""

@app.route("/login", methods=["GET","POST"])
def login():
    if not account_exists(): return redirect(url_for("setup"))
    error = ""
    if request.method == "POST":
        if check_credentials(request.form.get("username",""), request.form.get("password","")):
            session["logged_in"] = True
            session.permanent = True
            return redirect(request.args.get("next") or "/status")
        error = "<div class='err'>Incorrect username or password.</div>"
    return f"""<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Sign In</title>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap' rel='stylesheet'/>
  <style>{_AUTH_CSS}</style></head><body>
  <div class='card'><div><h1>Conference Manager</h1><p class='sub'>Sign in to continue</p></div>
  {error}<form method='POST'>
  <div><label>Username</label><input name='username' type='text' required autocomplete='username'/></div>
  <div><label>Password</label><input name='password' type='password' required autocomplete='current-password'/></div>
  <button class='btn'>Sign In</button></form></div></body></html>"""

@app.route("/setup", methods=["GET","POST"])
def setup():
    if account_exists(): return redirect(url_for("login"))
    error = ""
    if request.method == "POST":
        u = request.form.get("username","").strip()
        p = request.form.get("password","")
        c = request.form.get("confirm","")
        if not u or not p: error = "<div class='err'>Username and password required.</div>"
        elif p != c:       error = "<div class='err'>Passwords do not match.</div>"
        elif len(p) < 6:   error = "<div class='err'>Password must be at least 6 characters.</div>"
        else:
            create_account(u, p)
            session["logged_in"] = True
            return redirect("/status")
    return f"""<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Create Account</title>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap' rel='stylesheet'/>
  <style>{_AUTH_CSS}</style></head><body>
  <div class='card'><div><h1>Create Account</h1><p class='sub'>Set up your admin account</p></div>
  {error}<form method='POST'>
  <div><label>Username</label><input name='username' type='text' required/></div>
  <div><label>Password</label><input name='password' type='password' required/></div>
  <div><label>Confirm Password</label><input name='confirm' type='password' required/></div>
  <button class='btn'>Create Account</button></form></div></body></html>"""

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
@login_required
def root():
    return redirect("/status")

# ── STATUS PAGE ───────────────────────────────────────────────────────────────

@app.route("/status")
@login_required
def status():
    raw = FROM_NUMBER.lstrip("1") if FROM_NUMBER.startswith("1") else FROM_NUMBER
    dial_in = f"({raw[0:3]}) {raw[3:6]}-{raw[6:10]}" if len(raw) >= 10 else FROM_NUMBER
    return f"""<!DOCTYPE html>
<html lang='en'><head>
  <meta charset='UTF-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>Conference Manager</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#0a0f1e'/>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap' rel='stylesheet'/>
  <style>
    :root{{--bg:#0a0f1e;--surface:#111827;--surface2:#1a2235;--border:#1f2d45;--border2:#2a3a55;
          --text:#f0f4ff;--text2:#8899bb;--text3:#4a5f80;--blue:#3b82f6;--blue2:#1d4ed8;
          --green:#22c55e;--orange:#f97316;--red:#ef4444;--purple:#a78bfa;--yellow:#fbbf24;
          --r:12px;--rs:8px;--shadow:0 4px 24px rgba(0,0,0,.4)}}
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:4rem}}
    .topbar{{background:var(--surface);border-bottom:1px solid var(--border);padding:.85rem 1.5rem;
             display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}}
    .brand{{display:flex;align-items:center;gap:.6rem}}
    .brand-icon{{width:28px;height:28px;background:linear-gradient(135deg,#3b82f6,#6366f1);border-radius:8px;
                 display:flex;align-items:center;justify-content:center;font-size:.9rem}}
    .brand-name{{font-size:1rem;font-weight:700}}
    .signout{{background:none;border:1px solid var(--border2);color:var(--text2);border-radius:var(--rs);
              padding:.35rem .85rem;font-size:.78rem;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif}}
    .signout:hover{{border-color:var(--blue);color:var(--text)}}
    .page{{max-width:640px;margin:0 auto;padding:1.5rem 1rem;display:flex;flex-direction:column;gap:1.25rem}}
    .card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);
           padding:1.25rem;box-shadow:var(--shadow)}}
    .card-hdr{{display:flex;align-items:center;justify-content:space-between;margin-bottom:.85rem}}
    .card-title{{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text3)}}
    .trigger-btn{{width:100%;padding:.95rem;background:linear-gradient(135deg,#2563eb,#4f46e5);color:#fff;
                  border:none;border-radius:var(--r);font-size:1rem;font-weight:700;cursor:pointer;
                  font-family:'Inter',sans-serif;box-shadow:0 4px 20px rgba(59,130,246,.3);transition:all .2s}}
    .trigger-btn:hover:not([disabled]){{transform:translateY(-1px);box-shadow:0 6px 28px rgba(59,130,246,.45)}}
    .trigger-btn[disabled]{{background:linear-gradient(135deg,#1e3a5f,#2a2f6e);color:var(--text3);
                            cursor:not-allowed;box-shadow:none;transform:none}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
    .live-dot{{display:inline-block;width:8px;height:8px;background:var(--green);border-radius:50%;
               margin-right:.4rem;animation:pulse 1.5s infinite}}
    .dialin{{background:var(--surface2);border:1px solid var(--border);border-radius:var(--rs);
             padding:.8rem 1rem;display:flex;align-items:center;justify-content:space-between}}
    .dialin-num{{font-size:1.05rem;font-weight:700;letter-spacing:.06em}}
    .run-meta{{display:flex;justify-content:space-between;background:var(--surface2);border:1px solid var(--border);
               border-radius:var(--rs);padding:.6rem .85rem;font-size:.8rem;color:var(--text2);margin-bottom:.6rem}}
    .run-counts{{font-weight:700;color:var(--green)}}
    .call-row{{display:flex;align-items:center;gap:.55rem;padding:.5rem .75rem;background:var(--surface2);
               border:1px solid var(--border);border-radius:var(--rs);font-size:.82rem;margin-bottom:.3rem}}
    .call-num{{font-weight:600}}
    .call-name{{color:var(--text2);font-size:.75rem;flex:1}}
    .call-stat{{font-weight:600;font-size:.75rem;text-transform:capitalize}}
    .sec-label{{font-size:.69rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;padding:.3rem 0 .2rem}}
    .num-row{{display:flex;align-items:center;gap:.45rem;padding:.55rem .75rem;background:var(--surface2);
              border:1px solid var(--border);border-radius:var(--rs);margin-bottom:.3rem;transition:border-color .15s}}
    .num-row.paused{{opacity:.55;border-style:dashed}}
    .num-main{{flex:1;min-width:0}}
    .num-phone{{font-weight:600;font-size:.85rem}}
    .num-name-disp{{font-size:.74rem;color:var(--text2);margin-top:.08rem}}
    .tag{{font-size:.64rem;font-weight:700;padding:.1rem .35rem;border-radius:4px;margin-left:.25rem}}
    .tag-sheet{{background:rgba(59,130,246,.15);color:#7dd3fc}}
    .tag-paused{{background:rgba(249,115,22,.15);color:var(--orange)}}
    .num-actions{{display:flex;gap:.3rem;flex-shrink:0;flex-wrap:wrap}}
    .name-inp{{background:var(--bg);border:1px solid var(--border2);color:var(--text);border-radius:6px;
               padding:.28rem .5rem;font-size:.76rem;font-family:'Inter',sans-serif;width:85px}}
    .name-inp:focus{{outline:none;border-color:var(--blue)}}
    .btn{{border:none;border-radius:6px;padding:.28rem .6rem;font-size:.74rem;font-weight:600;
          cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .btn-save{{background:rgba(59,130,246,.15);color:var(--blue);border:1px solid rgba(59,130,246,.3)}}
    .btn-pause{{background:rgba(249,115,22,.1);color:var(--orange);border:1px solid rgba(249,115,22,.25)}}
    .btn-resume{{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.25)}}
    .btn-rm{{background:rgba(239,68,68,.1);color:var(--red);border:1px solid rgba(239,68,68,.2)}}
    .add-row{{display:flex;gap:.45rem;flex-wrap:wrap;margin-bottom:.75rem}}
    .add-inp{{flex:1;min-width:100px;background:var(--surface2);border:1px solid var(--border2);color:var(--text);
              border-radius:var(--rs);padding:.55rem .75rem;font-size:.83rem;font-family:'Inter',sans-serif}}
    .add-inp:focus{{outline:none;border-color:var(--blue)}}
    .btn-add{{background:linear-gradient(135deg,#15803d,#16a34a);color:#fff;border:none;border-radius:var(--rs);
              padding:.55rem 1rem;font-size:.83rem;font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .day-grid{{display:flex;flex-direction:column;gap:.4rem}}
    .day-row{{display:flex;align-items:center;gap:.5rem;padding:.55rem .75rem;background:var(--surface2);
              border:1px solid var(--border);border-radius:var(--rs);flex-wrap:wrap;transition:border-color .2s}}
    .day-row.active{{border-color:var(--blue);background:rgba(59,130,246,.05)}}
    .day-name{{min-width:80px;font-size:.84rem;font-weight:600}}
    .day-form{{display:flex;align-items:center;gap:.35rem;flex:1;flex-wrap:wrap}}
    .spin-wrap{{display:flex;flex-direction:column;align-items:center;gap:1px}}
    .spin-btn{{background:none;border:none;color:var(--text3);font-size:.6rem;cursor:pointer;
               padding:.04rem .45rem;font-family:'Inter',sans-serif}}
    .spin-btn:hover{{color:var(--text)}}
    .spin-val{{background:var(--bg);border:1px solid var(--border2);color:var(--text);border-radius:6px;
               padding:.26rem 0;font-size:.93rem;font-weight:700;text-align:center;width:2.3rem;
               font-family:'Inter',sans-serif}}
    .sep{{color:var(--text3);font-size:.95rem;font-weight:700;padding:0 .05rem}}
    .ampm-grp{{display:flex;border:1px solid var(--border2);border-radius:6px;overflow:hidden}}
    .ampm-opt{{background:var(--bg);color:var(--text3);border:none;padding:.26rem .5rem;font-size:.78rem;
               font-weight:700;cursor:pointer;font-family:'Inter',sans-serif;transition:all .15s}}
    .ampm-opt.sel{{background:var(--blue);color:#fff}}
    .btn-set{{background:linear-gradient(135deg,var(--blue2),var(--blue));color:#fff;border:none;
              border-radius:6px;padding:.3rem .8rem;font-size:.78rem;font-weight:700;cursor:pointer;
              font-family:'Inter',sans-serif}}
    .btn-clr{{background:none;border:none;color:var(--red);font-size:.95rem;cursor:pointer;
              padding:.1rem .3rem;opacity:.7}}
    .btn-clr:hover{{opacity:1}}
    .toggle-row{{display:flex;align-items:center;gap:.65rem;flex-wrap:wrap;padding:.45rem 0}}
    .toggle-row+.toggle-row{{border-top:1px solid var(--border)}}
    .toggle-btn{{border:none;border-radius:20px;padding:.35rem .9rem;font-size:.8rem;font-weight:700;
                 cursor:pointer;font-family:'Inter',sans-serif;white-space:nowrap}}
    .ton{{background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3)}}
    .toff{{background:var(--surface2);color:var(--text3);border:1px solid var(--border2)}}
    .thint{{font-size:.74rem;color:var(--text3);flex:1;line-height:1.4}}
    .rec-info{{font-size:.76rem;color:var(--text2);margin:.45rem 0 0;padding:.45rem .7rem;
               background:var(--surface2);border:1px solid var(--border);border-radius:var(--rs)}}
    .btn-dl{{display:inline-flex;align-items:center;gap:.3rem;background:rgba(59,130,246,.12);color:var(--blue);
             border:1px solid rgba(59,130,246,.25);border-radius:var(--rs);padding:.38rem .8rem;
             font-size:.78rem;font-weight:600;font-family:'Inter',sans-serif;text-decoration:none;margin-top:.45rem}}
    .hup-all-row{{display:flex;gap:.45rem;flex-wrap:wrap;margin-bottom:.6rem}}
    .btn-hup-all{{flex:1;padding:.6rem;background:rgba(239,68,68,.12);color:var(--red);
                  border:1px solid rgba(239,68,68,.25);border-radius:var(--rs);font-size:.83rem;
                  font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .btn-hup-blk{{flex:1;padding:.6rem;background:rgba(249,115,22,.1);color:var(--orange);
                  border:1px solid rgba(249,115,22,.22);border-radius:var(--rs);font-size:.83rem;
                  font-weight:700;cursor:pointer;font-family:'Inter',sans-serif}}
    .live-row{{display:flex;align-items:center;justify-content:space-between;gap:.45rem;padding:.5rem .75rem;
               background:var(--surface2);border:1px solid var(--border);border-radius:var(--rs);
               font-size:.81rem;margin-bottom:.3rem}}
    .live-acts{{display:flex;gap:.3rem}}
    .btn-hup{{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.22);
              border-radius:6px;padding:.24rem .55rem;font-size:.73rem;font-weight:700;cursor:pointer;
              font-family:'Inter',sans-serif}}
    .btn-hup-b{{background:rgba(249,115,22,.1);color:var(--orange);border:1px solid rgba(249,115,22,.2);
                border-radius:6px;padding:.24rem .55rem;font-size:.73rem;font-weight:700;cursor:pointer;
                font-family:'Inter',sans-serif}}
    .btn-sync{{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.25);
               border-radius:var(--rs);padding:.42rem .9rem;font-size:.8rem;font-weight:700;cursor:pointer;
               font-family:'Inter',sans-serif}}
    .footer{{display:flex;justify-content:center;gap:1.25rem;padding:1rem 0;font-size:.74rem}}
    .footer a{{color:var(--text3);transition:color .15s}}
    .footer a:hover{{color:var(--blue)}}
    .toast{{position:fixed;bottom:1.75rem;left:50%;transform:translateX(-50%);background:var(--surface);
            border:1px solid var(--border2);color:var(--text);padding:.55rem 1.3rem;border-radius:999px;
            font-size:.81rem;font-weight:500;opacity:0;transition:opacity .25s;pointer-events:none;
            z-index:999;box-shadow:var(--shadow);white-space:nowrap}}
    .toast.show{{opacity:1}}
  </style>
</head>
<body>
<div class='topbar'>
  <div class='brand'>
    <div class='brand-icon'>📞</div>
    <span class='brand-name'>Conference Manager</span>
  </div>
  <form method='POST' action='/logout' style='margin:0'>
    <button class='signout'>Sign out</button>
  </form>
</div>
<div class='page'>

  <!-- START -->
  <div class='card'>
    <button class='trigger-btn' id='trigger-btn' onclick='triggerConference()'>▶&nbsp; Start Conference Now</button>
  </div>

  <!-- HANGUP -->
  <div class='card'>
    <div class='card-hdr'><span class='card-title'>🔴 Active Call Controls</span></div>
    <div id='hangup-controls'><p style='color:var(--text3);font-size:.82rem'>No active calls.</p></div>
  </div>

  <!-- DIAL IN -->
  <div class='card'>
    <div class='card-hdr'><span class='card-title'>Dial-In Number</span></div>
    <div class='dialin'>
      <span style='font-size:.79rem;color:var(--text2)'>Members call in:</span>
      <span class='dialin-num'>{dial_in}</span>
    </div>
  </div>

  <!-- LAST CONFERENCE -->
  <div class='card'>
    <div class='card-hdr'>
      <span class='card-title'>Last Conference</span>
      <a href='/history' style='font-size:.74rem;color:var(--blue)'>History →</a>
    </div>
    <div id='last-run'><p style='color:var(--text3);font-size:.82rem'>Loading...</p></div>
  </div>

  <!-- MEMBERS -->
  <div class='card'>
    <div class='card-hdr'>
      <span class='card-title'>Phone Numbers</span>
      <span id='num-count' style='font-size:.72rem;background:rgba(59,130,246,.15);color:var(--blue);padding:.18rem .5rem;border-radius:999px;font-weight:700'>0</span>
    </div>
    <div class='add-row'>
      <input class='add-inp' type='tel'  id='new-num'  placeholder='Number e.g. 2025551234'/>
      <input class='add-inp' type='text' id='new-name' placeholder='Name (optional)'/>
      <button class='btn-add' onclick='addNumber()'>+ Add</button>
    </div>
    <div id='members-list'><p style='color:var(--text3);font-size:.82rem'>Loading...</p></div>
  </div>

  <!-- SCHEDULE -->
  <div class='card'>
    <div class='card-hdr'><span class='card-title'>Schedule</span></div>
    <div class='day-grid' id='day-grid'><p style='color:var(--text3);font-size:.82rem'>Loading...</p></div>
    <p style='font-size:.72rem;color:var(--text3);margin-top:.5rem'>Times are Eastern (ET). Press Set to save.</p>
  </div>

  <!-- RECORDING -->
  <div class='card'>
    <div class='card-hdr'><span class='card-title'>Recording</span></div>
    <div id='rec-section'><p style='color:var(--text3);font-size:.82rem'>Loading...</p></div>
  </div>

  <!-- ANNOUNCEMENTS -->
  <div class='card'>
    <div class='card-hdr'><span class='card-title'>Join Announcements</span></div>
    <div id='ann-section'><p style='color:var(--text3);font-size:.82rem'>Loading...</p></div>
  </div>

  <!-- SHEETS -->
  <div class='card'>
    <div class='card-hdr'><span class='card-title'>Google Sheets Sync</span></div>
    <p style='font-size:.77rem;color:var(--text2);margin-bottom:.7rem'>Syncs automatically on startup. Column A = name, Column B = number.</p>
    <div id='sheets-section'><p style='color:var(--text3);font-size:.82rem'>Loading...</p></div>
  </div>

  <div class='footer'>
    <a href='/history'>📋 Call History</a>
  </div>
</div>
<div class='toast' id='toast'></div>

<script>
const DAYS=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
const STAT_COLORS={{connected:"#22c55e",voicemail:"#f97316",dialing:"#fbbf24",
  busy:"#ef4444",unanswered:"#8899bb",timeout:"#8899bb",failed:"#ef4444",error:"#ef4444"}};
const STAT_ICONS={{connected:"✅",voicemail:"📵",dialing:"⏳",busy:"🔴",
  unanswered:"🔕",timeout:"🔕",failed:"❌",error:"❌"}};

function toast(msg,dur=2400){{
  const t=document.getElementById("toast");
  t.textContent=msg;t.classList.add("show");
  setTimeout(()=>t.classList.remove("show"),dur);
}}

async function api(url,data,isForm){{
  const opts=isForm?{{method:"POST",body:data}}:
    {{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify(data||{{}})}};
  return fetch(url,opts).then(r=>r.json()).catch(()=>({{ok:false}}));
}}

// ── Last Run ────────────────────────────────────────────────────────────────
function renderLastRun(s){{
  const el=document.getElementById("last-run");
  const btn=document.getElementById("trigger-btn");
  if(s.running){{btn.disabled=true;btn.innerHTML='<span class="live-dot"></span>Conference in Progress';}}
  else{{btn.disabled=false;btn.innerHTML="▶&nbsp; Start Conference Now";}}
  if(!s.run_time){{el.innerHTML='<p style="color:var(--text3);font-size:.82rem">No conference run yet.</p>';return;}}
  const badge=s.running?'<span style="color:var(--green);font-size:.73rem;font-weight:700">● Live</span>':'';
  const connected=(s.calls||[]).filter(c=>c.status==="connected").length;
  const rows=(s.calls||[]).map(c=>{{
    const icon=STAT_ICONS[c.status]||"❓";
    const color=STAT_COLORS[c.status]||"#8899bb";
    const name=c.name?`<span class="call-name">${{c.name}}</span>`:'';
    const err=c.error?`<span style="color:var(--red);font-size:.72rem"> (${{c.error}})</span>`:'';
    return `<div class="call-row"><span>${{icon}}</span><span class="call-num">${{c.number}}</span>${{name}}<span class="call-stat" style="color:${{color}}">${{c.status}}</span>${{err}}</div>`;
  }}).join("");
  el.innerHTML=`<div class="run-meta"><span>${{s.run_time}} ${{badge}}</span><span class="run-counts">${{connected}}/${{(s.calls||[]).length}} connected</span></div>${{rows}}`;
}}

// ── Members ─────────────────────────────────────────────────────────────────
function renderMembers(members){{
  document.getElementById("num-count").textContent=members.length;
  const el=document.getElementById("members-list");
  if(!members.length){{el.innerHTML='<p style="color:var(--text3);font-size:.82rem">No numbers yet.</p>';return;}}
  const active=members.filter(m=>!m.paused);
  const paused=members.filter(m=>m.paused);
  function row(m){{
    const srcTag=m.source==="sheet"?`<span class="tag tag-sheet">Sheet</span>`:'';
    const pauseTag=m.paused?`<span class="tag tag-paused">Paused</span>`:'';
    const disp=m.name||`<span style="color:var(--text3);font-size:.73rem">No name</span>`;
    return `<div class="num-row${{m.paused?' paused':''}}">
      <div class="num-main">
        <div style="display:flex;align-items:center">${{srcTag}}${{pauseTag}}<span class="num-phone" style="margin-left:.2rem">${{m.number}}</span></div>
        <div class="num-name-disp">${{disp}}</div>
      </div>
      <div class="num-actions">
        <input class="name-inp" type="text" value="${{m.name}}" placeholder="Name" id="nm-${{m.number}}"/>
        <button class="btn btn-save" onclick="saveName('${{m.number}}')">Save</button>
        <button class="btn ${{m.paused?'btn-resume':'btn-pause'}}" onclick="togglePause('${{m.number}}',${{m.paused}})">${{m.paused?'Resume':'Pause'}}</button>
        <button class="btn btn-rm" onclick="removeMember('${{m.number}}')">✕</button>
      </div>
    </div>`;
  }}
  let html='';
  if(active.length) html+=`<div class="sec-label" style="color:var(--green)">✅ Will be called (${{active.length}})</div>`+active.map(row).join('');
  if(paused.length) html+=`<div class="sec-label" style="color:var(--orange);margin-top:.4rem">⏸ Paused — skipped (${{paused.length}})</div>`+paused.map(row).join('');
  el.innerHTML=html;
}}

// ── Schedule Spinners ────────────────────────────────────────────────────────
const SS=Array.from({{length:7}},()=>({{h:12,m:0,ap:"AM"}}));
function to24(s){{let h=s.h%12;if(s.ap==="PM")h+=12;return h;}}
function loadSS(day,h24,m){{
  const s=SS[day];s.m=m;
  if(h24===0){{s.h=12;s.ap="AM";}}
  else if(h24<12){{s.h=h24;s.ap="AM";}}
  else if(h24===12){{s.h=12;s.ap="PM";}}
  else{{s.h=h24-12;s.ap="PM";}}
}}
function updSpin(day){{
  const s=SS[day];
  const h=document.getElementById(`sh-${{day}}`);
  const m=document.getElementById(`sm-${{day}}`);
  if(h)h.textContent=String(s.h).padStart(2,"0");
  if(m)m.textContent=String(s.m).padStart(2,"0");
  ["AM","PM"].forEach(v=>{{
    const el=document.getElementById(`ap-${{day}}-${{v}}`);
    if(el)el.className="ampm-opt"+(s.ap===v?" sel":"");
  }});
}}
function spinH(day,d){{SS[day].h=(SS[day].h-1+d+12)%12+1;updSpin(day);}}
function spinM(day,d){{SS[day].m=((SS[day].m+d)+60)%60;updSpin(day);}}
function setAP(day,v){{SS[day].ap=v;updSpin(day);}}
function spinnerHTML(day){{
  const s=SS[day];
  return `<div class="spin-wrap"><button class="spin-btn" onclick="spinH(${{day}},1)">▲</button>
    <div class="spin-val" id="sh-${{day}}">${{String(s.h).padStart(2,"0")}}</div>
    <button class="spin-btn" onclick="spinH(${{day}},-1)">▼</button></div>
    <span class="sep">:</span>
    <div class="spin-wrap"><button class="spin-btn" onclick="spinM(${{day}},1)">▲</button>
    <div class="spin-val" id="sm-${{day}}">${{String(s.m).padStart(2,"0")}}</div>
    <button class="spin-btn" onclick="spinM(${{day}},-1)">▼</button></div>
    <div class="ampm-grp">
      <button class="ampm-opt${{s.ap==="AM"?" sel":""}}" id="ap-${{day}}-AM" onclick="setAP(${{day}},'AM')">AM</button>
      <button class="ampm-opt${{s.ap==="PM"?" sel":""}}" id="ap-${{day}}-PM" onclick="setAP(${{day}},'PM')">PM</button>
    </div>`;
}}
function renderSchedule(schedule){{
  const grid=document.getElementById("day-grid");
  const byDay={{}};schedule.forEach(e=>{{if(!(e.day in byDay))byDay[e.day]=e;}});
  DAYS.forEach((_,i)=>{{
    if(byDay[i])loadSS(i,byDay[i].hour,byDay[i].minute);
    else{{SS[i].h=12;SS[i].m=0;SS[i].ap="AM";}}
  }});
  grid.innerHTML=DAYS.map((name,i)=>{{
    const isSet=!!byDay[i];
    const clearBtn=isSet?`<button class="btn-clr" onclick="clearDay(${{i}})">✕</button>`:"";
    return `<div class="day-row${{isSet?' active':''}}" id="day-${{i}}">
      <span class="day-name">${{name}}</span>
      <div class="day-form">${{spinnerHTML(i)}}
        <button class="btn-set" onclick="setDay(${{i}})">${{isSet?'Update':'Set'}}</button>
        ${{clearBtn}}
      </div></div>`;
  }}).join("");
}}

// ── Recording ────────────────────────────────────────────────────────────────
function renderRec(s){{
  const el=document.getElementById("rec-section");
  const recOn=s.record_enabled,repOn=s.replay_enabled;
  let info='',dl='';
  if(s.rec_exists&&s.rec_meta&&s.rec_meta.date){{
    const kb=(s.rec_meta.size_bytes||0)>>10;
    info=`<div class="rec-info">Recorded: ${{s.rec_meta.date}} · ${{kb}} KB</div>`;
    dl=`<a href="/recordings/audio" class="btn-dl" download="conference.mp3">⬇ Download</a>`;
  }}else{{
    info='<div class="rec-info" style="color:var(--text3)">No recording yet.</div>';
  }}
  el.innerHTML=`<div class="toggle-row">
    <button class="toggle-btn ${{recOn?'ton':'toff'}}" onclick="toggleSetting('record_enabled')">${{recOn?'Record: On':'Record: Off'}}</button>
    <span class="thint">${{recOn?'Conference will be recorded.':'Enable to record.'}}</span>
  </div>
  <div class="toggle-row">
    <button class="toggle-btn ${{repOn?'ton':'toff'}}" onclick="toggleSetting('replay_enabled')">${{repOn?'Replay for Late Callers: On':'Replay: Off'}}</button>
    <span class="thint">${{repOn?'Late callers hear the last recording if conference is over.':'Enable for replay.'}}</span>
  </div>
  ${{info}}${{dl}}`;
}}

// ── Announcements ─────────────────────────────────────────────────────────────
function renderAnn(s){{
  const el=document.getElementById("ann-section");
  const on=s.announcements_enabled;
  el.innerHTML=`<div class="toggle-row">
    <button class="toggle-btn ${{on?'ton':'toff'}}" onclick="toggleSetting('announcements_enabled')">${{on?'Announcements: On':'Announcements: Off'}}</button>
    <span class="thint">${{on?"Plays who joined after everyone answers or doesn't answer.":'Enable join announcements.'}}</span>
  </div>`;
}}

// ── Sheets ────────────────────────────────────────────────────────────────────
function renderSheets(msg,ok){{
  const el=document.getElementById("sheets-section");
  const msgHtml=msg?`<span style="color:${{ok?'var(--green)':'var(--red)'}};font-size:.77rem;margin-left:.5rem">${{msg}}</span>`:'';
  el.innerHTML=`<div style="display:flex;align-items:center;flex-wrap:wrap;gap:.5rem">
    <button class="btn-sync" onclick="sheetsSync()">↺ Re-sync from Sheet</button>${{msgHtml}}</div>`;
}}

// ── Hangup ────────────────────────────────────────────────────────────────────
async function renderHangup(){{
  const ctl=document.getElementById("hangup-controls");
  const r=await fetch("/api/live-calls",{{credentials:"include"}}).then(x=>x.json()).catch(()=>({{calls:[]}}));
  const calls=r.calls||[];
  if(!calls.length){{ctl.innerHTML='<p style="color:var(--text3);font-size:.82rem">No active calls.</p>';return;}}
  const conn=calls.filter(c=>c.status==="connected").length;
  const ring=calls.filter(c=>c.status==="dialing").length;
  let html=`<div class="hup-all-row">
    <button class="btn-hup-all" onclick="hupAll(false)">🔴 Hang Up All (${{calls.length}})</button>
    <button class="btn-hup-blk" onclick="hupAll(true)">🚫 Hang Up + Block All</button>
  </div>
  <p style="font-size:.72rem;color:var(--text3);margin-bottom:.45rem">${{conn}} connected · ${{ring}} ringing</p>`;
  html+=calls.map(c=>{{
    const color=c.status==="connected"?"var(--green)":"var(--yellow)";
    return `<div class="live-row">
      <div><span style="font-weight:600">${{c.number}}</span>
        ${{c.name?`<span style="color:var(--text2);font-size:.76rem"> — ${{c.name}}</span>`:''}}
        <span style="color:${{color}};font-size:.7rem;margin-left:.3rem">● ${{c.status==="connected"?"Connected":"Ringing"}}</span>
        ${{c.blocked?'<span style="color:var(--orange);font-size:.7rem"> · Blocked</span>':''}}
      </div>
      <div class="live-acts">
        <button class="btn-hup" onclick="hupOne('${{c.uuid}}',false)">Hang Up</button>
        <button class="btn-hup-b" onclick="hupOne('${{c.uuid}}',true)">+ Block</button>
      </div>
    </div>`;
  }}).join("");
  ctl.innerHTML=html;
}}

async function hupAll(block){{
  if(!confirm(block?"Hang up all and block from calling back?":"Hang up all?"))return;
  const r=await api("/hangup/all",{{block}});
  if(r.ok){{toast(`Hung up ${{r.hung_up.length}}`);setTimeout(renderHangup,1500);}}
  else toast("Failed");
}}
async function hupOne(uuid,block){{
  if(!confirm(block?"Hang up and block?":"Hang up?"))return;
  const r=await api("/hangup/one",{{uuid,block}});
  if(r.ok){{toast(block?"Hung up + blocked":"Hung up");setTimeout(renderHangup,1500);}}
  else toast("Failed");
}}

// ── Actions ───────────────────────────────────────────────────────────────────
async function triggerConference(){{
  const btn=document.getElementById("trigger-btn");
  btn.disabled=true;btn.innerHTML='<span class="live-dot"></span>Starting…';
  const r=await api("/trigger");
  if(!r.ok){{toast("Already running");btn.disabled=false;btn.innerHTML="▶&nbsp; Start Conference Now";}}
  else{{toast("Conference started!");setTimeout(refresh,2000);}}
}}
async function addNumber(){{
  const num=document.getElementById("new-num").value.trim();
  const name=document.getElementById("new-name").value.trim();
  if(!num)return;
  const r=await api("/numbers/add",{{number:num,name}});
  if(r.ok){{document.getElementById("new-num").value="";document.getElementById("new-name").value="";renderMembers(r.members);toast("Added");}}
  else toast("Invalid number");
}}
async function removeMember(n){{if(!confirm(`Remove ${{n}}?`))return;const r=await api("/numbers/remove",{{number:n}});if(r.ok){{renderMembers(r.members);toast("Removed");}}}}
async function togglePause(n,p){{const r=await api(p?"/numbers/unpause":"/numbers/pause",{{number:n}});if(r.ok){{renderMembers(r.members);toast(p?"Resumed":"Paused");}}}}
async function saveName(n){{const name=document.getElementById(`nm-${{n}}`).value.trim();const r=await api("/numbers/setname",{{number:n,name}});if(r.ok){{renderMembers(r.members);toast("Saved");}}}}
async function setDay(day){{const s=SS[day];const h24=to24(s);const t=`${{String(h24).padStart(2,"0")}}:${{String(s.m).padStart(2,"0")}}`;const r=await api("/schedule/set-day",{{day,time:t}});if(r.ok){{renderSchedule(r.schedule);toast("Schedule set!");}}}}
async function clearDay(day){{if(!confirm("Remove this schedule?"))return;const r=await api("/schedule/clear-day",{{day}});if(r.ok){{renderSchedule(r.schedule);toast("Removed");}}}}
async function toggleSetting(key){{await api("/settings/toggle",{{key}});refresh();}}
async function sheetsSync(){{
  toast("Syncing…",3000);
  const r=await api("/sheets/sync");
  renderMembers(r.members||[]);
  renderSheets(r.msg||"",r.ok);
  toast(r.ok?"Synced!":"Sync failed");
}}

// ── Refresh ───────────────────────────────────────────────────────────────────
async function refresh(){{
  try{{
    const s=await fetch("/api/state",{{credentials:"include"}}).then(r=>r.json());
    renderLastRun(s);
    renderMembers(s.members||[]);
    renderSchedule(s.schedule||[]);
    renderRec(s);
    renderAnn(s);
    renderSheets("",true);
    renderHangup();
  }}catch(e){{console.error("Refresh error",e);}}
}}

refresh();
setInterval(()=>{{const b=document.getElementById("trigger-btn");if(b&&b.disabled)refresh();}},8000);
if("serviceWorker"in navigator)navigator.serviceWorker.register("/sw.js").catch(()=>{{}});
</script>
</body></html>"""

# ── PWA ───────────────────────────────────────────────────────────────────────

@app.route("/manifest.json")
def manifest():
    from flask import Response
    data = {"name":"Conference Manager","short_name":"Conference","start_url":"/status",
            "display":"standalone","background_color":"#0a0f1e","theme_color":"#111827"}
    return Response(json.dumps(data), mimetype="application/manifest+json")

@app.route("/sw.js")
def sw():
    from flask import Response
    return Response("self.addEventListener('fetch',e=>{});", mimetype="application/javascript")

# ── SCHEDULER ─────────────────────────────────────────────────────────────────

def run_scheduler():
    last_minute = None
    fired_today = set()
    last_date   = None
    while True:
        now   = datetime.now(EASTERN)
        today = now.date()
        if last_date != today:
            fired_today.clear()
            last_date = today
        key = (now.weekday(), now.hour, now.minute)
        if key != last_minute:
            last_minute = key
            for e in load_schedule():
                ekey = (e["day"], e["hour"], e["minute"])
                if key == ekey and ekey not in fired_today:
                    fired_today.add(ekey)
                    threading.Thread(target=start_conference, daemon=True).start()
                    break
        time.sleep(15)

def _startup_sync():
    time.sleep(6)
    ok, msg = sync_from_sheets()
    print(f"[startup sync] {'OK' if ok else 'FAIL'}: {msg}", flush=True)

# ── START ─────────────────────────────────────────────────────────────────────

init_db()
threading.Thread(target=run_scheduler, daemon=True).start()
threading.Thread(target=_startup_sync, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
