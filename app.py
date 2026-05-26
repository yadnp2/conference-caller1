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
                CREATE TABLE IF NOT EXISTS scheduler_log (
                    day INT,
                    hour INT,
                    minute INT,
                    fired_date DATE NOT NULL,
                    PRIMARY KEY (day, hour, minute, fired_date)
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
        creds = _SACredentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        gc    = gspread.authorize(creds)
        ws    = gc.open_by_key(sid).sheet1
        rows  = ws.get("A2:B") or []
    except Exception as e:
        return False, f"Sheet error: {e}"

    # Build set of numbers from sheet
    sheet_numbers = {}
    for row in rows:
        name  = (row[0] if row else "").strip()
        raw_n = (row[1] if len(row) > 1 else "").strip()
        clean = _clean(raw_n)
        if clean:
            sheet_numbers[clean] = name

    if not sheet_numbers:
        return False, "Sheet appears empty — sync cancelled to avoid deleting all members"

    with get_db() as conn:
        with conn.cursor() as cur:
            # Upsert everyone in the sheet
            for number, name in sheet_numbers.items():
                cur.execute(
                    "INSERT INTO members(number,name,source) VALUES(%s,%s,'sheet') "
                    "ON CONFLICT(number) DO UPDATE SET name=EXCLUDED.name, source='sheet'",
                    (number, name))
            # Remove sheet members no longer in sheet
            cur.execute("SELECT number FROM members WHERE source='sheet'")
            db_sheet_nums = {r[0] for r in cur.fetchall()}
            to_remove = db_sheet_nums - set(sheet_numbers.keys())
            for number in to_remove:
                cur.execute("DELETE FROM members WHERE number=%s AND source='sheet'", (number,))
        conn.commit()

    removed = len(to_remove) if to_remove else 0
    msg = f"Synced {len(sheet_numbers)} number(s)"
    if removed:
        msg += f", removed {removed}"
    return True, msg


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
            client.voice.play_tts_into_call(u, TtsStreamOptions(text=text, language="en-US", style=2, level=1.0))
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
    # Vonage sends answer webhooks as GET with URL params OR POST JSON
    data = request.get_json(silent=True) or {}
    if not data:
        data = request.values.to_dict()
    uuid     = data.get("uuid", "")
    to_num   = data.get("to", "")
    from_num = data.get("from", "")
    print(f"[answer] uuid={uuid} to={to_num} from={from_num}", flush=True)

    # Determine if outbound or inbound:
    # Outbound: Vonage is calling a member (to=member number, from=our Vonage number)
    # Inbound:  A member is calling us (to=our Vonage number, from=member number)
    clean_to   = _clean(to_num)   if to_num   else None
    clean_from = _clean(from_num) if from_num else None
    vonage_num = _clean(FROM_NUMBER)

    with lock:
        in_call_map = uuid in call_map

    # It's outbound if: uuid is in call_map OR the 'to' number is a member (not our Vonage number)
    is_outbound = in_call_map or (clean_to and clean_to != vonage_num and is_approved_member(clean_to))

    if is_outbound:
        # Outbound call answered — just join the conference
        return jsonify([{"action": "talk", "style": 2, "text": "Please hold, joining you to the Shmiras HaLashon conference."}, *_conference_ncco()])

    # Inbound call — check if member is approved
    from_raw = from_num  # already extracted above
    number   = clean_from

    if not number:
        return jsonify([{"action": "talk", "style": 2, "text": "Sorry, calls with a hidden number cannot join. Goodbye."}])

    if not is_approved_member(number):
        return jsonify([{"action": "talk", "style": 2, "text": "Sorry, your number is not registered for this conference. Goodbye."}])

    if number in session_blocked:
        return jsonify([{"action": "talk", "style": 2, "text": "You cannot join this conference session. Goodbye."}])

    # Check if conference is still active
    with lock:
        conf_active = last_run.get("conference_active", False)

    # If conference is over and recording exists, play recording
    if not conf_active and get_setting("replay_enabled") == "true":
        meta = get_latest_recording_meta()
        has_recording = bool(meta.get("url")) or os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3"))
        if has_recording:
            log_call(number, get_name(number), "heard-recording")
            return jsonify([
                {"action": "talk", "style": 2, "text": f"The conference has ended. Playing the recording from {meta.get('date','the last session')}."},
                {"action": "stream", "streamUrl": [f"{BASE_URL}/recordings/audio"], "level": 0},
                {"action": "talk", "style": 2, "text": "Recording complete. Goodbye."},
            ])

    # Log as inbound-pending (will update to joined or missed in join-press)
    log_call(number, get_name(number), "inbound-pending")
    # Conference is active or no recording — join live
    return jsonify([
        {"action": "talk", "style": 2, "text": "Press 1 to join the Shmiras HaLashon conference."},
        {"action": "input", "type": ["dtmf"], "dtmf": {"maxDigits": 1, "timeOut": 6},
         "eventUrl": [f"{BASE_URL}/join-press"]},
    ])

@app.route("/join-press", methods=["GET","POST"])
def join_press():
    data  = request.get_json(silent=True) or {}
    if not data:
        data = request.values.to_dict()
    digit = (data.get("dtmf") or {}).get("digits","") or data.get("digits","")
    if str(digit).strip() == "1":
        uuid     = data.get("uuid","")
        from_raw = data.get("from","") or data.get("to","")
        number   = _clean(from_raw)
        name     = get_name(number) if number else ""
        # Log as inbound-joined
        if number:
            log_call(number, name, "inbound-joined")
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
    # They didn't press 1 — log as declined
    from_raw = data.get("from","") or data.get("to","")
    number   = _clean(from_raw)
    if number:
        log_call(number, get_name(number), "inbound-declined")
    return jsonify([{"action": "talk", "style": 2, "text": "Goodbye."}])

@app.route("/event", methods=["GET","POST"])
def event():
    # Vonage sends events as GET params OR POST JSON — handle both
    data   = request.get_json(silent=True) or {}
    if not data:
        data = request.values.to_dict()
    uuid   = data.get("uuid","")
    status = data.get("status","")
    if uuid or status:
        print(f"[event] uuid={uuid} status={status}", flush=True)
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
                    entry["status"] = status
                    still = any(e.get("status")=="connected" and u!=uuid for u,e in call_map.items())
                    last_run["conference_active"] = still
                    print(f"[event] {uuid} completed, conference_active={still}", flush=True)
                    threading.Thread(target=update_log, args=(log_id, status), daemon=True).start()
                else:
                    entry["status"] = status
                    if prev not in FINAL:
                        last_run["pending"] = max(0, last_run["pending"] - 1)
                    threading.Thread(target=update_log, args=(log_id, status), daemon=True).start()
    return "OK", 200

@app.route("/recording", methods=["GET","POST"])
def recording_webhook():
    data = request.get_json(silent=True) or {}
    if not data:
        data = request.values.to_dict()
    url  = data.get("recording_url") or data.get("url")
    if url:
        date_str = datetime.now(EASTERN).strftime("%A %B %-d at %-I:%M %p ET")
        size = int(data.get("size", 0))
        # Save meta immediately so URL is available for streaming
        save_recording_meta(url, date_str, size)
        print(f"[recording] saved meta url={url[:60]}... size={size}", flush=True)
        # Also try to download to disk as backup
        def _dl():
            ok = download_recording(url)
            print(f"[recording] download {'OK' if ok else 'FAILED'}", flush=True)
        threading.Thread(target=_dl, daemon=True).start()
    return "OK", 200

@app.route("/recordings/audio")
def recording_audio():
    # Try disk first
    path = os.path.join(RECORDINGS_DIR, "latest.mp3")
    if os.path.exists(path) and os.path.getsize(path) > 1000:
        return send_from_directory(RECORDINGS_DIR, "latest.mp3", mimetype="audio/mpeg")
    # Fallback: stream from Vonage URL using JWT auth
    try:
        meta = get_latest_recording_meta()
        rec_url = meta.get("url","")
        if rec_url:
            import jwt as pyjwt
            now     = int(time.time())
            payload = {"application_id": VONAGE_APP_ID, "iat": now,
                       "jti": str(_uuid.uuid4()), "exp": now+300}
            key   = _private_key.encode() if isinstance(_private_key, str) else _private_key
            token = pyjwt.encode(payload, key, algorithm="RS256")
            resp  = requests.get(rec_url, headers={"Authorization": f"Bearer {token}"}, timeout=30, stream=True)
            if resp.status_code == 200:
                from flask import Response as _Resp
                return _Resp(resp.iter_content(chunk_size=8192), mimetype="audio/mpeg")
    except Exception as e:
        print(f"[recording audio] stream error: {e}", flush=True)
    return "No recording available", 404

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
    # If local call_map is empty, query Vonage directly (handles server restart mid-conference)
    if not calls:
        try:
            import jwt as pyjwt
            now     = int(time.time())
            payload = {"application_id": VONAGE_APP_ID, "iat": now,
                       "jti": str(_uuid.uuid4()), "exp": now+300}
            key     = _private_key.encode() if isinstance(_private_key, str) else _private_key
            token   = pyjwt.encode(payload, key, algorithm="RS256")
            resp    = requests.get(
                "https://api.nexmo.com/v1/calls?status=answered&page_size=20",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5)   # 5-second timeout — never freezes
            if resp.status_code == 200:
                for c in resp.json().get("_embedded", {}).get("calls", []):
                    number = c.get("to", {}).get("number", "")
                    calls.append({
                        "uuid":    c.get("uuid", ""),
                        "number":  number,
                        "name":    get_name(number),
                        "status":  "connected",
                        "blocked": number in session_blocked,
                    })
        except Exception as e:
            print(f"[live-calls] Vonage query error: {e}", flush=True)
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

@app.route("/trigger/test", methods=["POST"])
@login_required
def trigger_test():
    """Run a test conference with specific numbers only — ignores the real member list."""
    data    = request.json or {}
    numbers = [_clean(n) for n in data.get("numbers", []) if _clean(n)]
    if not numbers:
        return jsonify({"ok": False, "error": "No valid numbers provided"}), 400
    with lock:
        if last_run["running"]:
            return jsonify({"ok": False, "error": "Conference already running"}), 409

    def run_test():
        with lock:
            last_run.update({
                "running": True, "conference_active": False, "pending": 0,
                "summary_fired": False,
                "time": datetime.now(EASTERN).strftime("TEST — %A %b %d at %-I:%M %p %Z"),
                "calls": []
            })
            call_map.clear()
        session_blocked.clear()
        with lock:
            last_run["pending"] = len(numbers)
        print(f"[test] Starting test conference with {numbers}", flush=True)
        try:
            for number in numbers:
                name = get_name(number) or "Test"
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
                    entry  = {"number": number, "name": name, "status": "dialing",
                              "uuid": uuid, "log_id": log_id}
                    with lock:
                        if uuid: call_map[uuid] = entry
                        last_run["calls"].append(entry)
                except Exception as e:
                    log_call(number, name, "error", error=str(e))
                    with lock:
                        last_run["calls"].append({"number": number, "name": name,
                                                   "status": "error", "error": str(e)})
                time.sleep(2)
        finally:
            with lock:
                last_run["running"] = False
            if get_setting("announcements_enabled") == "true":
                threading.Thread(target=_play_summary, daemon=True).start()

    threading.Thread(target=run_test, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/trigger/stop", methods=["POST"])
@login_required
def trigger_stop():
    """Force-reset the running flag if a conference got stuck."""
    with lock:
        last_run["running"]           = False
        last_run["conference_active"] = False
        last_run["pending"]           = 0
        last_run["summary_fired"]     = True
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
    # Recording exists if we have a URL in DB (even if disk file was wiped)
    rec_exists = bool(rec_meta.get("url")) or os.path.exists(os.path.join(RECORDINGS_DIR, "latest.mp3"))
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
    STATUS_COLORS = {
        "connected":       "#22c55e",
        "voicemail":       "#f97316",
        "dialing":         "#fbbf24",
        "busy":            "#ef4444",
        "unanswered":      "#8899bb",
        "failed":          "#ef4444",
        "error":           "#ef4444",
        "inbound-joined":  "#22c55e",
        "inbound-pending": "#fbbf24",
        "inbound-declined":"#8899bb",
        "heard-recording": "#a78bfa",
    }
    STATUS_ICONS = {
        "connected":       "✅",
        "voicemail":       "📵",
        "dialing":         "⏳",
        "busy":            "🔴",
        "unanswered":      "🔕",
        "failed":          "❌",
        "error":           "❌",
        "inbound-joined":  "📲",
        "inbound-pending": "⏳",
        "inbound-declined":"📵",
        "heard-recording": "🎧",
    }
    for c in calls:
        color = STATUS_COLORS.get(c.get("status",""), "#8899bb")
        try:
            dt = c["run_time_et"]
            ts = dt.strftime("%-m/%-d %-I:%M %p") if hasattr(dt,"strftime") else str(dt)
        except: ts = ""
        icon = STATUS_ICONS.get(c.get("status",""), "❓")
        rows += f"<tr><td>{ts}</td><td style='font-family:monospace'>{c['number']}</td><td>{c.get('name','')}</td><td style='color:{color}'>{icon} {c.get('status','')}</td></tr>"
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
  <title>Shmiras HaLashon Conference</title>
  <link rel='manifest' href='/manifest.json'/>
  <meta name='theme-color' content='#0d1117'/>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap' rel='stylesheet'/>
  <style>
    :root{{
      --bg:#f9f9f8;--s1:#ffffff;--s2:#f5f5f3;--s3:#ededeb;
      --b1:#e8e8e5;--b2:#d5d5d0;--b3:#c0c0ba;
      --t1:#1a1a18;--t2:#6b6b66;--t3:#9b9b96;
      --blue:#2563eb;--blue-dim:#1d4ed8;--blue-glow:rgba(37,99,235,.1);
      --green:#16a34a;--green-dim:#15803d;
      --orange:#ea580c;--red:#dc2626;--purple:#7c3aed;--yellow:#d97706;
      --r:6px;--r2:8px;--r3:12px;
    }}
    *{{box-sizing:border-box;margin:0;padding:0;-webkit-font-smoothing:antialiased}}
    body{{font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--t1);
          font-size:14px;line-height:1.5;min-height:100vh}}
    a{{color:var(--blue);text-decoration:none}}

    /* NAV */
    nav{{background:rgba(13,17,23,.95);border-bottom:1px solid var(--b1);
         padding:0 24px;height:56px;display:flex;align-items:center;
         justify-content:space-between;position:sticky;top:0;z-index:100;
         backdrop-filter:blur(12px)}}
    .nav-left{{display:flex;align-items:center;gap:12px}}
    .nav-logo{{width:28px;height:28px;background:var(--blue-dim);border-radius:6px;
               display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}}
    .nav-title{{font-size:14px;font-weight:600;color:var(--t1)}}
    .nav-sub{{font-size:12px;color:var(--t3);border-left:1px solid var(--b1);
              margin-left:8px;padding-left:12px}}
    .nav-right{{display:flex;align-items:center;gap:8px}}
    .nav-link{{font-size:13px;color:var(--t2);padding:6px 10px;border-radius:var(--r);
               cursor:pointer;background:none;border:none;font-family:inherit;
               transition:color .15s,background .15s}}
    .nav-link:hover{{color:var(--t1);background:var(--s3)}}
    .nav-btn{{font-size:13px;font-weight:500;color:var(--t1);padding:5px 12px;
              border-radius:var(--r);cursor:pointer;background:var(--s3);
              border:1px solid var(--b2);font-family:inherit;transition:all .15s}}
    .nav-btn:hover{{background:var(--s2);border-color:var(--b3)}}

    /* LAYOUT */
    .layout{{display:grid;grid-template-columns:240px 1fr 280px;gap:0;height:calc(100vh - 56px);overflow:hidden}}
    .sidebar{{background:var(--s2);border-right:1px solid var(--b1);overflow-y:auto;padding:16px 0}}
    .main{{overflow-y:auto;padding:24px}}
    .rightbar{{background:var(--s2);border-left:1px solid var(--b1);overflow-y:auto;padding:16px}}
    @media(max-width:1100px){{
      .layout{{grid-template-columns:200px 1fr}}
      .rightbar{{display:none}}
    }}
    @media(max-width:768px){{
      .layout{{grid-template-columns:1fr;height:auto;overflow:visible}}
      .sidebar{{border-right:none;border-bottom:1px solid var(--b1);padding:12px}}
      .main{{padding:16px}}
    }}

    /* SIDEBAR */
    .sidebar-section{{margin-bottom:4px}}
    .sidebar-label{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;
                    color:var(--t3);padding:8px 16px 4px}}
    .sidebar-item{{display:flex;align-items:center;gap:8px;padding:7px 16px;font-size:13px;
                   color:var(--t2);cursor:pointer;border-radius:0;transition:all .12s;
                   border:none;background:none;width:100%;text-align:left;font-family:inherit}}
    .sidebar-item:hover{{color:var(--t1);background:var(--s2)}}
    .sidebar-item.active{{color:var(--blue);background:rgba(37,99,235,.06);
                          border-right:2px solid var(--blue)}}
    .sidebar-item svg{{width:15px;height:15px;flex-shrink:0;opacity:.7}}
    .sidebar-item.active svg{{opacity:1;color:var(--blue)}}
    .sidebar-badge{{margin-left:auto;font-size:10px;font-weight:600;padding:1px 6px;
                    border-radius:10px;background:var(--s3);color:var(--t2);border:1px solid var(--b1)}}
    .sidebar-badge.live{{background:rgba(63,185,80,.15);color:var(--green)}}

    /* STATUS BAR */
    .status-bar{{display:flex;align-items:center;gap:8px;padding:10px 16px;
                 background:var(--s2);border-radius:var(--r2);border:1px solid var(--b1);
                 margin-bottom:20px}}
    .status-dot{{width:7px;height:7px;border-radius:50%;background:var(--t3);flex-shrink:0}}
    .status-dot.live{{background:var(--green);box-shadow:0 0 6px rgba(63,185,80,.5)}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
    .status-dot.live{{animation:pulse 2s infinite}}
    .status-text{{font-size:13px;color:var(--t2);flex:1}}
    .status-meta{{font-size:12px;color:var(--t3)}}

    /* PANELS */
    .panel{{background:var(--s1);border:1px solid var(--b1);border-radius:var(--r3);
            margin-bottom:16px;overflow:hidden}}
    .panel-header{{padding:14px 16px;border-bottom:1px solid var(--b1);
                   display:flex;align-items:center;justify-content:space-between}}
    .panel-title{{font-size:13px;font-weight:600;color:var(--t1);
                  display:flex;align-items:center;gap:8px}}
    .panel-title svg{{width:14px;height:14px;color:var(--t3)}}
    .panel-action{{font-size:12px;color:var(--blue);cursor:pointer;background:none;border:none;
                   font-family:inherit;padding:4px 8px;border-radius:var(--r);transition:background .12s}}
    .panel-action:hover{{background:var(--blue-glow)}}
    .panel-body{{padding:16px}}

    /* BUTTONS */
    .btn{{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;border-radius:var(--r2);
          font-size:13px;font-weight:500;cursor:pointer;border:none;font-family:inherit;
          transition:all .15s;white-space:nowrap}}
    .btn-primary{{background:var(--blue-dim);color:#fff}}
    .btn-primary:hover{{background:#1a7ff5}}
    .btn-primary:disabled{{background:var(--s3);color:var(--t3);cursor:not-allowed}}
    .btn-success{{background:var(--green-dim);color:#fff}}
    .btn-success:hover{{background:#2da843}}
    .btn-danger{{background:rgba(248,81,73,.12);color:var(--red);border:1px solid rgba(248,81,73,.2)}}
    .btn-danger:hover{{background:rgba(248,81,73,.2)}}
    .btn-secondary{{background:var(--s1);color:var(--t1);border:1px solid var(--b1)}}
    .btn-secondary:hover{{background:var(--s3);border-color:var(--b2)}}
    .btn-ghost{{background:none;color:var(--t2);border:1px solid var(--b1)}}
    .btn-ghost:hover{{background:var(--s3);color:var(--t1)}}
    .btn-full{{width:100%;justify-content:center;padding:10px 14px;font-size:14px}}

    /* LAUNCH BUTTON */
    .launch-btn{{width:100%;padding:12px 20px;background:var(--blue-dim);color:#fff;
                 border:none;border-radius:var(--r2);font-size:15px;font-weight:600;
                 cursor:pointer;font-family:inherit;transition:all .2s;
                 display:flex;align-items:center;justify-content:center;gap:8px}}
    .launch-btn:hover:not(:disabled){{background:#1a7ff5;transform:translateY(-1px)}}
    .launch-btn:disabled{{background:var(--s3);color:var(--t3);cursor:not-allowed;transform:none}}

    /* STOP BTN */
    #stop-btn{{display:none;width:100%;padding:8px;background:rgba(248,81,73,.08);
               color:var(--red);border:1px solid rgba(248,81,73,.2);border-radius:var(--r2);
               font-size:13px;font-weight:500;cursor:pointer;font-family:inherit;
               margin-top:8px;transition:background .15s}}
    #stop-btn:hover{{background:rgba(248,81,73,.15)}}

    /* METRICS */
    .metric-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:14px}}
    .metric{{background:var(--s3);border:1px solid var(--b1);border-radius:var(--r2);
             padding:12px 14px}}
    .metric-val{{font-size:22px;font-weight:700;line-height:1}}
    .metric-lbl{{font-size:11px;color:var(--t3);margin-top:3px;text-transform:uppercase;
                 letter-spacing:.04em}}
    .metric-val.green{{color:var(--green)}}
    .metric-val.blue{{color:var(--blue)}}

    /* CALL LIST */
    .call-item{{display:flex;align-items:center;gap:10px;padding:8px 12px;
                background:var(--s3);border:1px solid var(--b1);border-radius:var(--r);
                margin-bottom:6px;font-size:13px}}
    .call-status-dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0}}
    .call-num{{font-weight:500;flex:1}}
    .call-name{{color:var(--t2);font-size:12px}}
    .call-badge{{font-size:11px;font-weight:500;padding:2px 8px;border-radius:10px}}
    .badge-connected{{background:rgba(63,185,80,.12);color:var(--green)}}
    .badge-dialing{{background:rgba(227,179,65,.12);color:var(--yellow)}}
    .badge-voicemail{{background:rgba(240,136,62,.12);color:var(--orange)}}
    .badge-failed{{background:rgba(248,81,73,.12);color:var(--red)}}
    .badge-inbound{{background:rgba(56,139,253,.12);color:var(--blue)}}
    .badge-recording{{background:rgba(210,168,255,.12);color:var(--purple)}}

    /* MEMBERS */
    .member-item{{display:flex;align-items:center;gap:10px;padding:8px 0;
                  border-bottom:1px solid var(--b1)}}
    .member-item:last-child{{border-bottom:none}}
    .member-avatar{{width:28px;height:28px;border-radius:50%;background:var(--blue-glow);
                    border:1px solid var(--b2);display:flex;align-items:center;
                    justify-content:center;font-size:11px;font-weight:600;color:var(--blue);
                    flex-shrink:0;text-transform:uppercase}}
    .member-info{{flex:1;min-width:0}}
    .member-name{{font-size:13px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
    .member-num{{font-size:11px;color:var(--t3);font-family:monospace}}
    .member-actions{{display:flex;gap:5px;flex-shrink:0}}
    .member-tag{{font-size:10px;font-weight:600;padding:1px 6px;border-radius:4px;
                 text-transform:uppercase;letter-spacing:.04em}}
    .tag-sheet{{background:rgba(56,139,253,.12);color:var(--blue)}}
    .tag-paused{{background:rgba(240,136,62,.12);color:var(--orange)}}
    .icon-btn{{background:none;border:1px solid var(--b1);color:var(--t2);border-radius:5px;
               padding:4px 7px;font-size:12px;cursor:pointer;font-family:inherit;transition:all .12s}}
    .icon-btn:hover{{background:var(--s3);color:var(--t1);border-color:var(--b2)}}
    .icon-btn.danger:hover{{background:rgba(248,81,73,.1);color:var(--red);border-color:rgba(248,81,73,.3)}}
    .name-field{{background:var(--s1);border:1px solid var(--b1);color:var(--t1);
                 border-radius:5px;padding:4px 8px;font-size:12px;font-family:inherit;width:90px}}
    .name-field:focus{{outline:none;border-color:var(--blue)}}
    .member-item.paused{{opacity:.45}}

    /* ADD ROW */
    .add-row{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}}
    .field{{flex:1;min-width:100px;background:var(--s1);border:1px solid var(--b1);
            color:var(--t1);border-radius:var(--r2);padding:7px 12px;font-size:13px;
            font-family:inherit}}
    .field:focus{{outline:none;border-color:var(--blue)}}

    /* SCHEDULE */
    .sched-row{{display:flex;align-items:center;gap:8px;padding:8px 0;
                border-bottom:1px solid var(--b1)}}
    .sched-row:last-child{{border-bottom:none}}
    .sched-day{{font-size:13px;font-weight:500;width:78px;flex-shrink:0}}
    .sched-time{{font-size:13px;color:var(--t2);flex:1;font-family:monospace}}
    .sched-set{{color:var(--green);font-size:11px;font-weight:600}}
    .sched-unset{{color:var(--t3);font-size:11px}}
    .spin-wrap{{display:flex;flex-direction:column;align-items:center;gap:0}}
    .spin-btn{{background:none;border:none;color:var(--t3);font-size:9px;line-height:1;
               cursor:pointer;padding:1px 5px}}
    .spin-btn:hover{{color:var(--t1)}}
    .spin-val{{background:var(--s1);border:1px solid var(--b1);color:var(--t1);
               border-radius:5px;padding:3px 0;font-size:13px;font-weight:600;
               text-align:center;width:34px;font-family:monospace;-moz-appearance:textfield}}
    .spin-val::-webkit-outer-spin-button,.spin-val::-webkit-inner-spin-button{{-webkit-appearance:none}}
    .spin-val:focus{{outline:none;border-color:var(--blue)}}
    .ampm-grp{{display:flex;border:1px solid var(--b1);border-radius:5px;overflow:hidden}}
    .ampm-opt{{background:var(--s1);color:var(--t3);border:none;padding:3px 7px;
               font-size:12px;font-weight:600;cursor:pointer;font-family:inherit}}
    .ampm-opt.sel{{background:var(--blue-dim);color:#fff}}
    .sep{{color:var(--t3);font-weight:700;padding:0 2px;font-size:13px}}

    /* TOGGLES */
    .toggle-row{{display:flex;align-items:center;justify-content:space-between;
                 padding:10px 0;border-bottom:1px solid var(--b1)}}
    .toggle-row:last-child{{border-bottom:none}}
    .toggle-label{{font-size:13px;font-weight:500}}
    .toggle-sub{{font-size:11px;color:var(--t3);margin-top:2px}}
    .toggle{{position:relative;width:36px;height:20px;flex-shrink:0}}
    .toggle input{{opacity:0;width:0;height:0}}
    .toggle-track{{position:absolute;inset:0;background:var(--b2);border:1px solid var(--b2);
                   border-radius:10px;cursor:pointer;transition:all .2s}}
    .toggle input:checked+.toggle-track{{background:var(--green-dim);border-color:var(--green-dim)}}
    .toggle-track::after{{content:'';position:absolute;top:2px;left:2px;width:14px;height:14px;
                          background:#fff;border-radius:50%;transition:.2s}}
    .toggle input:checked+.toggle-track::after{{transform:translateX(16px)}}

    /* HANGUP */
    .live-call-row{{display:flex;align-items:center;gap:8px;padding:8px 12px;
                    background:var(--s3);border:1px solid var(--b1);border-radius:var(--r);
                    margin-bottom:6px;font-size:13px}}
    .hup-actions{{display:flex;gap:5px;margin-left:auto}}

    /* DIALIN */
    .dialin-card{{background:linear-gradient(135deg,rgba(37,99,235,.06),rgba(37,99,235,.02));
                  border:1px solid rgba(56,139,253,.2);border-radius:var(--r2);padding:14px 16px}}
    .dialin-num{{font-size:20px;font-weight:700;letter-spacing:.08em;color:var(--t1);margin-top:4px}}

    /* RECORDING */
    .rec-file{{background:var(--s3);border:1px solid var(--b1);border-radius:var(--r);
               padding:10px 12px;font-size:12px;color:var(--t2);margin-top:10px;
               display:flex;align-items:center;justify-content:space-between}}

    /* ADMIN ZONE */
    .admin-panel{{background:#0a0e14;border:1px solid #1e2d3d;border-radius:var(--r3);
                  overflow:hidden}}
    .admin-panel-header{{background:linear-gradient(135deg,#0d1520,#111d2c);
                         border-bottom:1px solid #1e2d3d;padding:12px 16px;
                         display:flex;align-items:center;justify-content:space-between}}
    .admin-panel-title{{font-size:12px;font-weight:600;color:#4d7fa8;
                        text-transform:uppercase;letter-spacing:.08em;
                        display:flex;align-items:center;gap:6px}}
    .admin-chip{{background:rgba(227,179,65,.1);color:#b8912a;border:1px solid rgba(227,179,65,.2);
                 border-radius:4px;padding:1px 6px;font-size:10px;font-weight:600;
                 text-transform:uppercase}}
    .admin-body{{padding:14px 16px}}
    .admin-note{{font-size:12px;color:#2e4a62;background:rgba(56,139,253,.04);
                 border:1px solid rgba(56,139,253,.08);border-radius:var(--r);
                 padding:8px 12px;margin-bottom:12px;line-height:1.5}}
    .admin-field{{width:100%;background:#070b10;border:1px solid #1e2d3d;color:#8ab4cc;
                  border-radius:var(--r);padding:7px 10px;font-size:13px;
                  font-family:inherit;margin-bottom:6px}}
    .admin-field:focus{{outline:none;border-color:#2563eb}}
    .admin-actions{{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}}
    .admin-btn-add{{background:#0d1a28;color:#3a6a8a;border:1px solid #1e3248;
                    border-radius:var(--r);padding:6px 12px;font-size:12px;font-weight:500;
                    cursor:pointer;font-family:inherit;transition:all .15s}}
    .admin-btn-add:hover{{border-color:#3b82f6;color:#60a5fa}}
    .admin-btn-run{{flex:1;background:rgba(63,185,80,.08);color:#3fb950;
                    border:1px solid rgba(63,185,80,.2);border-radius:var(--r);
                    padding:7px 14px;font-size:13px;font-weight:600;cursor:pointer;
                    font-family:inherit;transition:all .15s}}
    .admin-btn-run:hover{{background:rgba(63,185,80,.15);border-color:rgba(63,185,80,.4)}}

    /* TOAST */
    .toast{{position:fixed;bottom:24px;right:24px;background:var(--t1);
            border:1px solid var(--t1);color:var(--bg);padding:10px 16px;
            border-radius:var(--r2);font-size:13px;opacity:0;transition:all .25s;
            pointer-events:none;z-index:999;box-shadow:0 8px 24px rgba(0,0,0,.4)}}
    .toast.show{{opacity:1}}

    /* SECTION DIVIDER */
    .section-title{{font-size:11px;font-weight:600;text-transform:uppercase;
                    letter-spacing:.06em;color:var(--t3);margin-bottom:10px}}
    .empty-state{{font-size:13px;color:var(--t3);padding:16px 0;text-align:center}}
  </style>
</head>
<body>

<nav>
  <div class='nav-left'>
    <div class='nav-logo'>📞</div>
    <span class='nav-title'>Conference Manager</span>
    <span class='nav-sub'>Shmiras HaLashon</span>
  </div>
  <div class='nav-right'>
    <a href='/history' class='nav-link'>Call History</a>
    <form method='POST' action='/logout' style='margin:0'>
      <button class='nav-btn'>Sign out</button>
    </form>
  </div>
</nav>

<div class='layout'>

  <!-- SIDEBAR -->
  <aside class='sidebar'>
    <div class='sidebar-section'>
      <div class='sidebar-label'>Overview</div>
      <button class='sidebar-item active' onclick='showSection("dashboard")'>
        <svg viewBox='0 0 16 16' fill='currentColor'><path d='M2 2h5v5H2V2zm0 7h5v5H2V9zm7-7h5v5H9V2zm0 7h5v5H9V9z'/></svg>
        Dashboard
        <span class='sidebar-badge live' id='sidebar-live' style='display:none'>Live</span>
      </button>
      <a href='/history' class='sidebar-item' style='text-decoration:none'>
        <svg viewBox='0 0 16 16' fill='currentColor'><path d='M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1zm1 7.5H7V4h2v3.5H11V9H9z'/></svg>
        Call History
      </a>
    </div>
    <div class='sidebar-section'>
      <div class='sidebar-label'>Settings</div>
      <button class='sidebar-item' onclick='showSection("members")'>
        <svg viewBox='0 0 16 16' fill='currentColor'><path d='M8 8a3 3 0 1 0 0-6 3 3 0 0 0 0 6zm-5 5s-1 0-1-1 1-4 6-4 6 3 6 4-1 1-1 1H3z'/></svg>
        Members
        <span class='sidebar-badge' id='sidebar-count'>0</span>
      </button>
      <button class='sidebar-item' onclick='showSection("schedule")'>
        <svg viewBox='0 0 16 16' fill='currentColor'><path d='M3.5 0a.5.5 0 0 1 .5.5V1h8V.5a.5.5 0 0 1 1 0V1h1a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V3a2 2 0 0 1 2-2h1V.5a.5.5 0 0 1 .5-.5zM1 4v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V4H1z'/></svg>
        Schedule
      </button>
      <button class='sidebar-item' onclick='showSection("settings")'>
        <svg viewBox='0 0 16 16' fill='currentColor'><path d='M8 4.754a3.246 3.246 0 1 0 0 6.492 3.246 3.246 0 0 0 0-6.492zM5.754 8a2.246 2.246 0 1 1 4.492 0 2.246 2.246 0 0 1-4.492 0z'/><path d='M9.796 1.343c-.527-1.79-3.065-1.79-3.592 0l-.094.319a.873.873 0 0 1-1.255.52l-.292-.16c-1.64-.892-3.433.902-2.54 2.541l.159.292a.873.873 0 0 1-.52 1.255l-.319.094c-1.79.527-1.79 3.065 0 3.592l.319.094a.873.873 0 0 1 .52 1.255l-.16.292c-.892 1.64.901 3.434 2.541 2.54l.292-.159a.873.873 0 0 1 1.255.52l.094.319c.527 1.79 3.065 1.79 3.592 0l.094-.319a.873.873 0 0 1 1.255-.52l.292.16c1.64.892 3.433-.902 2.54-2.541l-.159-.292a.873.873 0 0 1 .52-1.255l.319-.094c1.79-.527 1.79-3.065 0-3.592l-.319-.094a.873.873 0 0 1-.52-1.255l.16-.292c.892-1.64-.901-3.433-2.541-2.54l-.292.159a.873.873 0 0 1-1.255-.52l-.094-.319z'/></svg>
        Settings
      </button>
    </div>
    <div class='sidebar-section'>
      <div class='sidebar-label'>Admin</div>
      <button class='sidebar-item' onclick='showSection("test")'>
        <svg viewBox='0 0 16 16' fill='currentColor'><path d='M14 1H2a1 1 0 0 0-1 1v2a1 1 0 0 0 1 1h1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V5h1a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1zm-2 12H4V5h8v8zM2 3V2h12v1H2z'/></svg>
        Test Mode
        <span class='sidebar-badge' style='background:rgba(227,179,65,.1);color:#b8912a'>Admin</span>
      </button>
    </div>
  </aside>

  <!-- MAIN CONTENT -->
  <main class='main'>

    <!-- DASHBOARD SECTION -->
    <div id='sec-dashboard'>
      <div class='status-bar'>
        <div class='status-dot' id='status-dot'></div>
        <span class='status-text' id='status-text'>No conference running</span>
        <span class='status-meta' id='status-meta'></span>
      </div>

      <div style='display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px'>
        <!-- LEFT: Launch + Stats -->
        <div>
          <div class='panel'>
            <div class='panel-header'><span class='panel-title'>Start Conference</span></div>
            <div class='panel-body'>
              <button class='launch-btn' id='trigger-btn' onclick='triggerConference()'>
                <svg width='16' height='16' viewBox='0 0 16 16' fill='currentColor'><path d='M6 3.472v9.056L12.44 8 6 3.472z'/></svg>
                Start Conference Now
              </button>
              <button id='stop-btn' onclick='stopConference()'>⏹ Force Stop</button>
              <div class='metric-grid'>
                <div class='metric'><div class='metric-val green' id='stat-conn'>—</div><div class='metric-lbl'>Connected</div></div>
                <div class='metric'><div class='metric-val blue' id='stat-total'>—</div><div class='metric-lbl'>Total Dialed</div></div>
              </div>
            </div>
          </div>

          <div class='panel'>
            <div class='panel-header'><span class='panel-title'>Dial-In Number</span></div>
            <div class='panel-body'>
              <div class='dialin-card'>
                <div style='font-size:12px;color:var(--t3)'>Members call in directly</div>
                <div class='dialin-num'>{dial_in}</div>
              </div>
            </div>
          </div>
        </div>

        <!-- RIGHT: Active Calls + Hangup -->
        <div>
          <div class='panel'>
            <div class='panel-header'>
              <span class='panel-title'>Active Call Controls</span>
              <button class='panel-action' onclick='renderHangup()'>Refresh</button>
            </div>
            <div class='panel-body' id='hangup-controls'>
              <div class='empty-state'>No active calls</div>
            </div>
          </div>
        </div>
      </div>

      <!-- Last Conference Full Width -->
      <div class='panel'>
        <div class='panel-header'>
          <span class='panel-title'>Last Conference</span>
          <a href='/history' class='panel-action'>View all history →</a>
        </div>
        <div class='panel-body' id='last-run'>
          <div class='empty-state'>No conference run yet</div>
        </div>
      </div>
    </div>

    <!-- MEMBERS SECTION -->
    <div id='sec-members' style='display:none'>
      <div class='panel'>
        <div class='panel-header'>
          <span class='panel-title'>Phone Numbers <span id='num-count' style='background:var(--s3);color:var(--t2);font-size:11px;padding:1px 8px;border-radius:10px;margin-left:6px;font-weight:500'>0</span></span>
          <button class='panel-action' onclick='sheetsSync()'>↺ Sync from Sheet</button>
        </div>
        <div class='panel-body'>
          <div class='add-row' style='margin-bottom:16px'>
            <input class='field' type='tel'  id='new-num'  placeholder='Number e.g. 2025551234'/>
            <input class='field' type='text' id='new-name' placeholder='Name'/>
            <button class='btn btn-success' onclick='addNumber()'>Add</button>
          </div>
          <div id='members-list'><div class='empty-state'>Loading...</div></div>
          <div style='margin-top:14px;padding-top:14px;border-top:1px solid var(--b1);font-size:12px;color:var(--t3)'>
            Numbers synced from Google Sheet are marked with a Sheet tag. Column A = name, Column B = phone number.
          </div>
        </div>
      </div>
    </div>

    <!-- SCHEDULE SECTION -->
    <div id='sec-schedule' style='display:none'>
      <div class='panel'>
        <div class='panel-header'><span class='panel-title'>Weekly Schedule</span></div>
        <div class='panel-body'>
          <div style='font-size:12px;color:var(--t3);margin-bottom:14px'>All times are Eastern (ET). Set a time for each day you want the conference to run automatically.</div>
          <div id='day-grid'><div class='empty-state'>Loading...</div></div>
        </div>
      </div>
    </div>

    <!-- SETTINGS SECTION -->
    <div id='sec-settings' style='display:none'>
      <div style='display:grid;grid-template-columns:1fr 1fr;gap:16px'>
        <div class='panel'>
          <div class='panel-header'><span class='panel-title'>Recording</span></div>
          <div class='panel-body' id='rec-section'><div class='empty-state'>Loading...</div></div>
        </div>
        <div class='panel'>
          <div class='panel-header'><span class='panel-title'>Announcements</span></div>
          <div class='panel-body' id='ann-section'><div class='empty-state'>Loading...</div></div>
        </div>
      </div>
    </div>

    <!-- TEST SECTION -->
    <div id='sec-test' style='display:none'>
      <div class='admin-panel'>
        <div class='admin-panel-header'>
          <div class='admin-panel-title'>
            🧪 Test Mode
            <span class='admin-chip'>Admin Only</span>
          </div>
          <span style='font-size:11px;color:#1e3a4a'>Real members will not be called</span>
        </div>
        <div class='admin-body'>
          <div class='admin-note'>Enter one or more phone numbers to run a full conference test. The system will dial only these numbers — everything else (announcement, recording, history) works exactly as in a real call.</div>
          <div id='test-numbers'>
            <input class='admin-field' type='tel' id='test-num-0' placeholder='Enter phone number e.g. 2025551234'/>
          </div>
          <div class='admin-actions'>
            <button class='admin-btn-add' onclick='addTestNumber()'>+ Add another number</button>
            <button class='admin-btn-run' onclick='runTest()'>▶ Run Test Conference</button>
          </div>
        </div>
      </div>
    </div>

  </main>

  <!-- RIGHT SIDEBAR -->
  <aside class='rightbar'>
    <div style='margin-bottom:16px'>
      <div class='section-title'>Google Sheets</div>
      <div style='font-size:12px;color:var(--t3);margin-bottom:10px;line-height:1.5'>Numbers sync automatically on startup. Col A = name, Col B = number.</div>
      <div id='sheets-section'></div>
    </div>
    <div style='border-top:1px solid var(--b1);padding-top:16px;margin-bottom:16px'>
      <div class='section-title'>Quick Actions</div>
      <div style='display:flex;flex-direction:column;gap:6px'>
        <a href='#' onclick='event.preventDefault();showSection("members")' class='btn btn-ghost btn-full' style='justify-content:flex-start;gap:8px;text-decoration:none'>Manage Members</a>
        <a href='#' onclick='event.preventDefault();showSection("schedule")' class='btn btn-ghost btn-full' style='justify-content:flex-start;gap:8px;text-decoration:none'>Edit Schedule</a>
        <a href='#' onclick='event.preventDefault();showSection("test")' class='btn btn-ghost btn-full' style='justify-content:flex-start;gap:8px;text-decoration:none'>Run Test</a>
        <a href='/history' class='btn btn-ghost btn-full' style='justify-content:flex-start;gap:8px'>
          <svg width='14' height='14' viewBox='0 0 16 16' fill='currentColor'><path d='M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1zm1 7.5H7V4h2v3.5H11V9H9z'/></svg>
          Call History
        </a>
      </div>
    </div>
    <div style='border-top:1px solid var(--b1);padding-top:16px'>
      <div class='section-title'>System</div>
      <div id='system-info' style='font-size:12px;color:var(--t3);line-height:1.8'></div>
    </div>
  </aside>

</div><!-- /layout -->
<div class='toast' id='toast'></div>

<script>
const DAYS=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];

const STATUS_MAP={{
  connected:{{color:"var(--green)",label:"Connected",badge:"badge-connected"}},
  dialing:{{color:"var(--yellow)",label:"Ringing",badge:"badge-dialing"}},
  voicemail:{{color:"var(--orange)",label:"Voicemail",badge:"badge-voicemail"}},
  unanswered:{{color:"var(--t3)",label:"No answer",badge:""}},
  busy:{{color:"var(--red)",label:"Busy",badge:"badge-failed"}},
  failed:{{color:"var(--red)",label:"Failed",badge:"badge-failed"}},
  error:{{color:"var(--red)",label:"Error",badge:"badge-failed"}},
  "inbound-joined":{{color:"var(--blue)",label:"Called in",badge:"badge-inbound"}},
  "inbound-pending":{{color:"var(--yellow)",label:"Calling in",badge:"badge-dialing"}},
  "inbound-declined":{{color:"var(--t3)",label:"Declined",badge:""}},
  "heard-recording":{{color:"var(--purple)",label:"Heard recording",badge:"badge-recording"}},
}};

function toast(msg,dur=2400){{
  const t=document.getElementById("toast");
  t.textContent=msg;t.classList.add("show");
  setTimeout(()=>t.classList.remove("show"),dur);
}}

async function api(url,data){{
  return fetch(url,{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify(data||{{}})}})
    .then(r=>r.json()).catch(()=>({{ok:false}}));
}}

// ── Section navigation ────────────────────────────────────────────────────
function showSection(name){{
  var secs=["dashboard","members","schedule","settings","test"];
  for(var i=0;i<secs.length;i++){{
    var s=secs[i];
    var el=document.getElementById("sec-"+s);
    if(el)el.style.display=(s===name)?"block":"none";
    var nav=document.getElementById("nav-"+s);
    if(nav){{
      if(s===name)nav.classList.add("active");
      else nav.classList.remove("active");
    }}
  }}
}}


// ── Last Run ──────────────────────────────────────────────────────────────
function renderLastRun(s){{
  const el  = document.getElementById("last-run");
  const btn = document.getElementById("trigger-btn");
  const dot = document.getElementById("status-dot");
  const stxt= document.getElementById("status-text");
  const smeta=document.getElementById("status-meta");
  const liveBadge=document.getElementById("sidebar-live");
  const stopBtn=document.getElementById("stop-btn");
  const sc  = document.getElementById("stat-conn");
  const st  = document.getElementById("stat-total");

  const calls = s.calls||[];
  const connected = calls.filter(c=>c.status==="connected").length;

  if(s.running){{
    btn.disabled=true;
    btn.innerHTML='<svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor" style="animation:pulse 1s infinite"><circle cx="8" cy="8" r="7"/></svg> Conference in Progress';
    dot.classList.add("live");
    stxt.textContent="Conference in progress";
    smeta.textContent=`${{connected}} connected`;
    if(liveBadge)liveBadge.style.display="inline";
    if(stopBtn)stopBtn.style.display="block";
  }}else{{
    btn.disabled=false;
    btn.innerHTML='<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M6 3.472v9.056L12.44 8 6 3.472z"/></svg> Start Conference Now';
    dot.classList.remove("live");
    stxt.textContent = s.run_time ? `Last run: ${{s.run_time}}` : "No conference running";
    smeta.textContent = s.run_time ? `${{connected}}/${{calls.length}} connected` : "";
    if(liveBadge)liveBadge.style.display="none";
    if(stopBtn)stopBtn.style.display="none";
  }}

  if(sc)sc.textContent = s.run_time ? connected : "—";
  if(st)st.textContent = s.run_time ? calls.length : "—";

  if(!s.run_time){{el.innerHTML='<div class="empty-state">No conference run yet</div>';return;}}

  const rows = calls.map(c=>{{
    const sm = STATUS_MAP[c.status]||{{color:"var(--t3)",label:c.status,badge:""}};
    const badge = sm.badge ? `<span class="call-badge ${{sm.badge}}">${{sm.label}}</span>` : `<span style="font-size:11px;color:var(--t3)">${{sm.label}}</span>`;
    const name = c.name ? `<span class="call-name"> · ${{c.name}}</span>` : "";
    const err  = c.error ? `<span style="color:var(--red);font-size:11px"> (${{c.error}})</span>` : "";
    return `<div class="call-item">
      <div class="call-status-dot" style="background:${{sm.color}}"></div>
      <span class="call-num">${{c.number}}</span>${{name}}${{err}}
      ${{badge}}
    </div>`;
  }}).join("");
  el.innerHTML = rows || '<div class="empty-state">No calls logged</div>';
}}

// ── Members ────────────────────────────────────────────────────────────────
function renderMembers(members){{
  const countEl = document.getElementById("num-count");
  const sideCount = document.getElementById("sidebar-count");
  if(countEl) countEl.textContent = members.length;
  if(sideCount) sideCount.textContent = members.length;
  const el = document.getElementById("members-list");
  if(!el) return;
  if(!members.length){{el.innerHTML='<div class="empty-state">No members yet. Add one above or sync from Google Sheet.</div>';return;}}

  const active = members.filter(m=>!m.paused);
  const paused = members.filter(m=>m.paused);

  function row(m){{
    const initials = m.name ? m.name.split(" ").map(w=>w[0]).join("").slice(0,2).toUpperCase() : m.number.slice(-2);
    const srcTag = m.source==="sheet" ? `<span class="member-tag tag-sheet">Sheet</span>` : "";
    const pauseTag = m.paused ? `<span class="member-tag tag-paused">Paused</span>` : "";
    return `<div class="member-item${{m.paused?' paused':''}}">
      <div class="member-avatar">${{initials}}</div>
      <div class="member-info">
        <div class="member-name">${{m.name||'<span style="color:var(--t3)">No name</span>'}}</div>
        <div class="member-num">${{m.number}} ${{srcTag}}${{pauseTag}}</div>
      </div>
      <div class="member-actions">
        <input class="name-field" type="text" value="${{m.name}}" placeholder="Name" id="nm-${{m.number}}"/>
        <button class="icon-btn" onclick="saveName('${{m.number}}')">Save</button>
        <button class="icon-btn" onclick="togglePause('${{m.number}}',${{m.paused}})">${{m.paused?"▶":"⏸"}}</button>
        <button class="icon-btn danger" onclick="removeMember('${{m.number}}')">✕</button>
      </div>
    </div>`;
  }}

  let html = "";
  if(active.length){{
    html += `<div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--green);margin-bottom:6px">✓ Will be called (${{active.length}})</div>`;
    html += active.map(row).join("");
  }}
  if(paused.length){{
    html += `<div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--orange);margin-top:14px;margin-bottom:6px">⏸ Paused — skipped (${{paused.length}})</div>`;
    html += paused.map(row).join("");
  }}
  el.innerHTML = html;
}}

// ── Schedule Spinners ──────────────────────────────────────────────────────
const SS=Array.from({{length:7}},()=>({{h:12,m:0,ap:"AM"}}));
function to24(s){{let h=s.h%12;if(s.ap==="PM")h+=12;return h;}}
function loadSS(day,h24,m){{
  const s=SS[day];s.m=m;
  if(h24===0){{s.h=12;s.ap="AM";}}else if(h24<12){{s.h=h24;s.ap="AM";}}
  else if(h24===12){{s.h=12;s.ap="PM";}}else{{s.h=h24-12;s.ap="PM";}}
}}
function updSpin(day){{
  const s=SS[day];
  const h=document.getElementById(`sh-${{day}}`);
  const m=document.getElementById(`sm-${{day}}`);
  if(h)h.value=String(s.h).padStart(2,"0");
  if(m)m.value=String(s.m).padStart(2,"0");
  ["AM","PM"].forEach(v=>{{
    const el=document.getElementById(`ap-${{day}}-${{v}}`);
    if(el)el.className="ampm-opt"+(s.ap===v?" sel":"");
  }});
}}
function spinH(day,d){{SS[day].h=(SS[day].h-1+d+12)%12+1;updSpin(day);}}
function spinM(day,d){{SS[day].m=((SS[day].m+d)+60)%60;updSpin(day);}}
function setAP(day,v){{SS[day].ap=v;updSpin(day);}}
function setHDirect(day,val){{let h=parseInt(val);if(isNaN(h))return;SS[day].h=Math.max(1,Math.min(12,h));updSpin(day);}}
function setMDirect(day,val){{let m=parseInt(val);if(isNaN(m))return;SS[day].m=Math.max(0,Math.min(59,m));updSpin(day);}}

function renderSchedule(schedule){{
  const grid=document.getElementById("day-grid");
  if(!grid)return;
  const byDay={{}};schedule.forEach(e=>{{if(!(e.day in byDay))byDay[e.day]=e;}});
  DAYS.forEach((_,i)=>{{
    if(byDay[i])loadSS(i,byDay[i].hour,byDay[i].minute);
    else{{SS[i].h=12;SS[i].m=0;SS[i].ap="AM";}}
  }});
  grid.innerHTML=DAYS.map((name,i)=>{{
    const isSet=!!byDay[i];
    const s=SS[i];
    const h24=to24(s);
    const ampm=h24<12?"AM":"PM";
    const h12=(h24%12)||12;
    const timeStr=isSet?`${{String(h12).padStart(2,"0")}}:${{String(s.m).padStart(2,"0")}} ${{ampm}}`:"Not set";
    const clearBtn=isSet?`<button class="icon-btn danger" onclick="clearDay(${{i}})" style="font-size:11px">Remove</button>`:"";
    return `<div class="sched-row">
      <span class="sched-day">${{name}}</span>
      <div style="display:flex;align-items:center;gap:6px;flex:1">
        <div class="spin-wrap"><button class="spin-btn" onclick="spinH(${{i}},1)">▲</button>
          <input class="spin-val" type="number" id="sh-${{i}}" value="${{String(s.h).padStart(2,'00')}}" min="1" max="12" onchange="setHDirect(${{i}},this.value)" onclick="this.select()"/>
          <button class="spin-btn" onclick="spinH(${{i}},-1)">▼</button></div>
        <span class="sep">:</span>
        <div class="spin-wrap"><button class="spin-btn" onclick="spinM(${{i}},1)">▲</button>
          <input class="spin-val" type="number" id="sm-${{i}}" value="${{String(s.m).padStart(2,'00')}}" min="0" max="59" onchange="setMDirect(${{i}},this.value)" onclick="this.select()"/>
          <button class="spin-btn" onclick="spinM(${{i}},-1)">▼</button></div>
        <div class="ampm-grp">
          <button class="ampm-opt${{s.ap==="AM"?" sel":""}}" id="ap-${{i}}-AM" onclick="setAP(${{i}},'AM')">AM</button>
          <button class="ampm-opt${{s.ap==="PM"?" sel":""}}" id="ap-${{i}}-PM" onclick="setAP(${{i}},'PM')">PM</button>
        </div>
        <button class="btn btn-secondary" style="padding:4px 10px;font-size:12px" onclick="setDay(${{i}})">${{isSet?"Update":"Set"}}</button>
        ${{clearBtn}}
      </div>
      <span style="font-size:11px;color:${{isSet?'var(--green)':' var(--t3)'}}">${{isSet?"✓ "+timeStr:"—"}}</span>
    </div>`;
  }}).join("");
}}

// ── Recording ──────────────────────────────────────────────────────────────
function renderRec(s){{
  const el=document.getElementById("rec-section");
  if(!el)return;
  const recOn=s.record_enabled,repOn=s.replay_enabled;
  let recFile="";
  if(s.rec_exists&&s.rec_meta&&s.rec_meta.date){{
    const kb=(s.rec_meta.size_bytes||0)>>10;
    recFile=`<div class="rec-file"><span>Recording from ${{s.rec_meta.date}} · ${{kb}} KB</span><a href="/recordings/audio" download="conference.mp3" class="btn btn-secondary" style="padding:4px 10px;font-size:11px">Download</a></div>`;
  }}else{{
    recFile='<div class="rec-file">No recording saved yet</div>';
  }}
  el.innerHTML=`
    <div class="toggle-row">
      <div><div class="toggle-label">Record calls</div><div class="toggle-sub">Save a recording of each conference</div></div>
      <label class="toggle"><input type="checkbox" ${{recOn?"checked":""}} onchange="toggleSetting('record_enabled')"><span class="toggle-track"></span></label>
    </div>
    <div class="toggle-row">
      <div><div class="toggle-label">Replay for late callers</div><div class="toggle-sub">Play recording when conference is over</div></div>
      <label class="toggle"><input type="checkbox" ${{repOn?"checked":""}} onchange="toggleSetting('replay_enabled')"><span class="toggle-track"></span></label>
    </div>
    ${{recFile}}`;
}}

function renderAnn(s){{
  const el=document.getElementById("ann-section");
  if(!el)return;
  const on=s.announcements_enabled;
  el.innerHTML=`
    <div class="toggle-row">
      <div><div class="toggle-label">Join announcements</div><div class="toggle-sub">Announce who joined after all calls settle</div></div>
      <label class="toggle"><input type="checkbox" ${{on?"checked":""}} onchange="toggleSetting('announcements_enabled')"><span class="toggle-track"></span></label>
    </div>`;
}}

// ── Sheets ─────────────────────────────────────────────────────────────────
function renderSheets(msg,ok){{
  const el=document.getElementById("sheets-section");
  if(!el)return;
  const msgHtml=msg?`<div style="font-size:12px;color:${{ok?'var(--green)':' var(--red)'}};margin-top:6px">${{msg}}</div>`:"";
  el.innerHTML=`<button class="btn btn-ghost btn-full" onclick="sheetsSync()">↺ Re-sync from Sheet</button>${{msgHtml}}`;
}}

function renderSystemInfo(s){{
  const el=document.getElementById("system-info");
  if(!el)return;
  const members = s.members||[];
  const active = members.filter(m=>!m.paused).length;
  const sched = s.schedule||[];
  el.innerHTML=`
    <div>Members: ${{members.length}} (${{active}} active)</div>
    <div>Scheduled days: ${{sched.length}}</div>
    <div>Recording: ${{s.record_enabled?"On":"Off"}}</div>
    <div>Replay: ${{s.replay_enabled?"On":"Off"}}</div>`;
}}

// ── Hangup ─────────────────────────────────────────────────────────────────
async function renderHangup(){{
  const ctl=document.getElementById("hangup-controls");
  if(!ctl)return;
  const r=await fetch("/api/live-calls",{{credentials:"include"}}).then(x=>x.json()).catch(()=>({{calls:[]}}));
  const calls=r.calls||[];
  if(!calls.length){{ctl.innerHTML='<div class="empty-state">No active calls</div>';return;}}
  const conn=calls.filter(c=>c.status==="connected").length;
  const ring=calls.filter(c=>c.status==="dialing").length;
  let html=`<div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">
    <button class="btn btn-danger" style="flex:1" onclick="hupAll(false)">Hang Up All (${{calls.length}})</button>
    <button class="btn btn-danger" style="flex:1;background:rgba(240,136,62,.1);color:var(--orange);border-color:rgba(240,136,62,.2)" onclick="hupAll(true)">Hang Up + Block All</button>
  </div>
  <div style="font-size:11px;color:var(--t3);margin-bottom:8px">${{conn}} connected · ${{ring}} ringing</div>`;
  html+=calls.map(c=>{{
    const sm=STATUS_MAP[c.status]||{{color:"var(--t3)",label:c.status}};
    return `<div class="live-call-row">
      <div class="call-status-dot" style="background:${{sm.color}}"></div>
      <span style="font-weight:500">${{c.number}}</span>
      ${{c.name?`<span style="color:var(--t2);font-size:12px"> · ${{c.name}}</span>`:""}}
      <span style="color:${{sm.color}};font-size:11px">${{sm.label}}</span>
      ${{c.blocked?'<span style="color:var(--orange);font-size:11px">· Blocked</span>':""}}
      <div class="hup-actions">
        <button class="icon-btn" onclick="hupOne('${{c.uuid}}',false)">Hang up</button>
        <button class="icon-btn danger" onclick="hupOne('${{c.uuid}}',true)">+ Block</button>
      </div>
    </div>`;
  }}).join("");
  ctl.innerHTML=html;
}}

async function hupAll(block){{
  if(!confirm(block?"Hang up all and block from calling back?":"Hang up all active calls?"))return;
  const r=await api("/hangup/all",{{block}});
  if(r.ok){{toast(`Hung up ${{r.hung_up.length}} call(s)`);setTimeout(renderHangup,1500);}}
  else toast("Failed to hang up");
}}
async function hupOne(uuid,block){{
  if(!confirm(block?"Hang up and block from calling back this session?":"Hang up this person?"))return;
  const r=await api("/hangup/one",{{uuid,block}});
  if(r.ok){{toast(block?"Hung up and blocked":"Hung up");setTimeout(renderHangup,1500);}}
  else toast("Failed");
}}

// ── Actions ────────────────────────────────────────────────────────────────
async function triggerConference(){{
  const btn=document.getElementById("trigger-btn");
  btn.disabled=true;
  btn.innerHTML='<svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor" style="animation:pulse 1s infinite"><circle cx="8" cy="8" r="7"/></svg> Starting…';
  const r=await api("/trigger");
  if(!r.ok){{toast("Already running — use Force Stop if stuck");btn.disabled=false;btn.innerHTML='<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M6 3.472v9.056L12.44 8 6 3.472z"/></svg> Start Conference Now';}}
  else{{toast("Conference started!");setTimeout(refresh,2000);}}
}}
async function stopConference(){{
  if(!confirm("Force-stop the conference? This resets the display only."))return;
  await api("/trigger/stop");toast("Stopped");refresh();
}}
async function addNumber(){{
  const num=document.getElementById("new-num").value.trim();
  const name=document.getElementById("new-name").value.trim();
  if(!num)return;
  const r=await api("/numbers/add",{{number:num,name}});
  if(r.ok){{document.getElementById("new-num").value="";document.getElementById("new-name").value="";renderMembers(r.members);toast("Member added");}}
  else toast("Invalid number");
}}
async function removeMember(n){{if(!confirm(`Remove ${{n}}?`))return;const r=await api("/numbers/remove",{{number:n}});if(r.ok){{renderMembers(r.members);toast("Removed");}}}}
async function togglePause(n,p){{const r=await api(p?"/numbers/unpause":"/numbers/pause",{{number:n}});if(r.ok){{renderMembers(r.members);toast(p?"Resumed":"Paused");}}}}
async function saveName(n){{const name=document.getElementById(`nm-${{n}}`).value.trim();const r=await api("/numbers/setname",{{number:n,name}});if(r.ok){{renderMembers(r.members);toast("Saved");}}}}
async function setDay(day){{const s=SS[day];const h24=to24(s);const t=`${{String(h24).padStart(2,"00")}}:${{String(s.m).padStart(2,"00")}}`;const r=await api("/schedule/set-day",{{day,time:t}});if(r.ok){{renderSchedule(r.schedule);toast("Schedule saved!");}}}}
async function clearDay(day){{if(!confirm("Remove schedule for this day?"))return;const r=await api("/schedule/clear-day",{{day}});if(r.ok){{renderSchedule(r.schedule);toast("Removed");}}}}
async function toggleSetting(key){{await api("/settings/toggle",{{key}});refresh();}}
async function sheetsSync(){{
  toast("Syncing…",3000);
  const r=await api("/sheets/sync");
  if(r.members)renderMembers(r.members);
  renderSheets(r.msg||"",r.ok);
  toast(r.ok?"Synced successfully":"Sync failed");
}}

// ── Test Mode ──────────────────────────────────────────────────────────────
let testNumCount=1;
function addTestNumber(){{
  const container=document.getElementById("test-numbers");
  const inp=document.createElement("input");
  inp.className="admin-field";inp.type="tel";
  inp.placeholder="Enter phone number e.g. 2025551234";
  inp.id=`test-num-${{testNumCount++}}`;
  container.appendChild(inp);
}}
async function runTest(){{
  const numbers=[];
  document.querySelectorAll('[id^="test-num-"]').forEach(inp=>{{if(inp.value.trim())numbers.push(inp.value.trim());}});
  if(!numbers.length){{toast("Enter at least one number");return;}}
  if(!confirm(`Run test conference with:\n${{numbers.join("\n")}}\n\nReal members will NOT be called.`))return;
  const r=await api("/trigger/test",{{numbers}});
  if(r.ok){{toast("Test conference started!");setTimeout(refresh,2000);showSection("dashboard");}}
  else toast("Error: "+(r.error||"failed"));
}}

// ── Refresh ────────────────────────────────────────────────────────────────
async function refresh(){{
  try{{
    const s=await fetch("/api/state",{{credentials:"include"}}).then(r=>r.json());
    renderLastRun(s);
    renderMembers(s.members||[]);
    renderSchedule(s.schedule||[]);
    renderRec(s);renderAnn(s);
    renderSheets("",true);
    renderHangup();
    renderSystemInfo(s);
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

def _already_fired_today(day, hour, minute):
    """Check DB if this schedule entry already fired today (survives restarts)."""
    try:
        today = datetime.now(EASTERN).date()
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM scheduler_log WHERE day=%s AND hour=%s AND minute=%s AND fired_date=%s",
                    (day, hour, minute, today))
                return cur.fetchone() is not None
    except Exception as e:
        print(f"[scheduler] DB check error: {e}", flush=True)
        return False

def _mark_fired_today(day, hour, minute):
    """Record in DB that this schedule entry fired today."""
    try:
        today = datetime.now(EASTERN).date()
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scheduler_log(day,hour,minute,fired_date) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (day, hour, minute, today))
            conn.commit()
    except Exception as e:
        print(f"[scheduler] DB mark error: {e}", flush=True)

def run_scheduler():
    last_minute = None
    while True:
        now = datetime.now(EASTERN)
        key = (now.weekday(), now.hour, now.minute)
        if key != last_minute:
            last_minute = key
            with lock:
                already_running = last_run.get("running", False)
                still_active    = last_run.get("conference_active", False)
            if not already_running and not still_active:
                for e in load_schedule():
                    ekey = (e["day"], e["hour"], e["minute"])
                    if key == ekey and not _already_fired_today(*ekey):
                        _mark_fired_today(*ekey)
                        print(f"[scheduler] Firing conference for {ekey}", flush=True)
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
