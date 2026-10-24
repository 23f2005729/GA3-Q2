"""A2A 1.0 (HTTP+JSON) invoice action agent.

Run:  BASE_URL=https://<host>/a2a/ LLM_BASE_URL=... LLM_API_KEY=... LLM_MODEL=... uvicorn main:app --port 8000
BASE_URL must be EXACTLY the URL you submit on the page (trailing slash included).
Every request is appended to requests.log (Authorization is never logged).
"""
import os, re, json, uuid, time, hashlib, sqlite3, threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from urllib.parse import urlparse, parse_qs

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000/a2a/")
LLM_BASE = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1").rstrip("/")
LLM_KEY = os.getenv("LLM_API_KEY", "ollama")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:7b-instruct")
MOCK = os.getenv("MOCK_LLM") == "1"
DB_PATH = os.getenv("STATE_DB", "state.db")
LOG_PATH = os.getenv("REQUEST_LOG", "requests.log")
SEND_WAIT = float(os.getenv("SEND_WAIT", "38"))        # each request has 45 s
KEEP_BRACKETS = os.getenv("STRIP_BRACKETS", "0") != "1"  # evidenceRefs exactly as written in the documents

A2A = "application/a2a+json"
MT_BATCH = "application/vnd.ga5.invoice-claim-batch+json"
MT_PROP = "application/vnd.ga5.invoice-action-proposals+json"
MT_RES = "application/vnd.ga5.invoice-action-results+json"
MT_REC = "application/vnd.ga5.invoice-action-receipts+json"
ACTIONS = ["settle_invoice", "request_approval", "hold_invoice", "reject_duplicate", "open_exception"]
WORKING, INPUT, DONE, CANCELED = ("TASK_STATE_WORKING", "TASK_STATE_INPUT_REQUIRED",
                                  "TASK_STATE_COMPLETED", "TASK_STATE_CANCELED")
TERMINAL = {DONE, CANCELED, "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}
MAX_BYTES = 512 * 1024
BASE_PATH = urlparse(BASE_URL).path.rstrip("/")
STATS = {"llm_calls": 0}

app = FastAPI()
POOL = ThreadPoolExecutor(max_workers=8)
db = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
db.execute("PRAGMA journal_mode=WAL")
db.executescript("""
CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, principal TEXT, state TEXT, task_json TEXT, created REAL);
CREATE INDEX IF NOT EXISTS idx_tasks_p ON tasks(principal, created);
CREATE TABLE IF NOT EXISTS msgs(principal TEXT, message_id TEXT, hash TEXT, task_id TEXT, snapshot TEXT,
  PRIMARY KEY(principal, message_id));
CREATE TABLE IF NOT EXISTS decisions(key TEXT PRIMARY KEY, json TEXT);
""")
dbl = threading.Lock()
_locks, _lg = {}, threading.Lock()


def lock_for(k):
    with _lg:
        return _locks.setdefault(k, threading.RLock())

def q(sql, args=()):
    with dbl:
        return db.execute(sql, args).fetchall()

def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def canon(o):
    return json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha(s):
    return hashlib.sha256(s.encode()).hexdigest()

def log(entry):
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(entry, default=str)[:2_000_000] + "\n")


# ------------------------------------------------------------------ HTTP helpers
def out(obj, status=200, mt=A2A, headers=None):
    body = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()
    return Response(body, status_code=status, media_type=mt, headers=headers)

def err(status, code, title, headers=None):
    # generic on purpose: never echoes task ids or says whether a task exists
    return out({"code": code, "status": status, "title": title, "detail": title,
                "error": {"code": code, "status": status, "message": title}},
               status, "application/json", headers)

def mt_of(v):
    return (v or "").split(";")[0].strip().lower()

def gate(h, method, has_body):
    m = re.fullmatch(r"Bearer ([!-~]{1,4096})", h.get("authorization", ""))
    if not m:
        return None, err(401, "UNAUTHENTICATED", "Authentication required", {"WWW-Authenticate": "Bearer"})
    if h.get("a2a-version", "").strip() != "1.0":
        return None, err(400, "VERSION_NOT_SUPPORTED", "Unsupported A2A version")
    if method == "POST" and has_body and mt_of(h.get("content-type")) != A2A:
        return None, err(415, "UNSUPPORTED_MEDIA_TYPE", f"Content-Type must be {A2A}")
    acc = h.get("accept")
    if acc and not any(mt_of(x) in (A2A, "application/json", "application/*", "*/*") for x in acc.split(",")):
        return None, err(406, "NOT_ACCEPTABLE", f"Only {A2A} is produced")
    return sha(m.group(1))[:32], None


# ------------------------------------------------------------------ storage
def save_task(t, principal):
    q("INSERT INTO tasks VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state, task_json=excluded.task_json",
      (t["id"], principal, t["status"]["state"], json.dumps(t), time.time()))

def load_task(tid, principal):
    r = q("SELECT task_json FROM tasks WHERE id=? AND principal=?", (tid, principal))
    return json.loads(r[0][0]) if r else None

def find_part(msg, mt):
    for p in msg.get("parts") or []:
        if isinstance(p, dict) and p.get("mediaType") == mt and isinstance(p.get("data"), dict):
            return p["data"]
    return None


# ------------------------------------------------------------------ decision engine (LLM proposes, code validates)
SYSTEM = """You are an accounts-payable control agent. For EVERY invoice package choose exactly ONE action:
- settle_invoice: valid, reconciled, and within autonomous authority.
- request_approval: commercially valid, but outside delegated authority (amount above the limit that may be settled autonomously).
- hold_invoice: payment pauses until a stated verification completes.
- reject_duplicate: the SAME commercial invoice was already paid. A payment for a DIFFERENT invoice is NOT a duplicate.
- open_exception: material records conflict and need an exception workflow.
How to read a package:
- It mixes useful facts with old examples, archive examples, training decoys, negated sentences ("no hold is needed", "not a duplicate") and irrelevant action words. Those do NOT decide the action.
- Exactly ONE paragraph of the current record determines the action. Return the THREE bracketed references that appear in that deciding paragraph, copied EXACTLY as written in the documents.
- Never return the cover-sheet reference, archive-example references or training-decoy references.
- Follow the policy revision named below and any policy text inside the package.
- facts: vendorName and invoiceNumber exactly as written; amountMinor = integer amount in minor currency units (e.g. 1234.50 INR -> 123450); currency = ISO code.
Reply with JSON only: {"proposals":[{"packageId":"...","action":"...","facts":{"vendorName":"...","invoiceNumber":"...","amountMinor":123,"currency":"INR"},"evidenceRefs":["...","...","..."],"reason":"one or two sentences explaining why this action"}]}
Return one entry per package, using the packageId given."""

def leaves(o):
    if isinstance(o, str):
        yield o
    elif isinstance(o, dict):
        for v in o.values():
            yield from leaves(v)
    elif isinstance(o, list):
        for v in o:
            yield from leaves(v)

def pkg_key(p):
    return sha(canon({k: v for k, v in p.items() if k not in ("packageId", "batchId", "id")}))

def all_refs(text):
    seen = []
    for r in re.findall(r"\[[^\[\]\n]{1,80}\]", text):
        if r not in seen:
            seen.append(r)
    return seen

def mock_llm(user):
    STATS["llm_calls"] += 1
    pkgs = json.loads(user.split("PACKAGES:\n", 1)[1])
    res = []
    for p in pkgs:
        t = "\n".join(leaves(p))
        g = lambda k: (re.search(k + r"=([^;\n\]]+)", t) or [None, ""])[1].strip()
        res.append({"packageId": p["packageId"], "action": g("ACTION"),
                    "facts": {"vendorName": g("VENDOR"), "invoiceNumber": g("INV"),
                              "amountMinor": g("AMT"), "currency": g("CUR")},
                    "evidenceRefs": all_refs(t)[-3:], "reason": "mock reason for testing"})
    return json.dumps({"proposals": res})

def call_llm(user):
    if MOCK:
        return mock_llm(user)
    STATS["llm_calls"] += 1
    r = httpx.post(f"{LLM_BASE}/chat/completions", timeout=170,
                   headers={"Authorization": f"Bearer {LLM_KEY}"},
                   json={"model": LLM_MODEL, "temperature": 0, "response_format": {"type": "json_object"},
                         "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]})
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

def parse_json(s):
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{.*\}", s, re.S)
        return json.loads(m.group(0)) if m else {}

def clean(pkg, d):
    """Return (decision, problem). decision is None when it cannot be used."""
    text = "\n".join(leaves(pkg))
    if not isinstance(d, dict) or d.get("action") not in ACTIONS:
        return None, "action must be one of the five exact action names"
    f = d.get("facts") if isinstance(d.get("facts"), dict) else {}
    vendor, inv = str(f.get("vendorName") or "").strip(), str(f.get("invoiceNumber") or "").strip()
    cur = str(f.get("currency") or "").strip().upper()
    amt = f.get("amountMinor")
    if isinstance(amt, str):
        amt = re.sub(r"[^\d]", "", amt) or None
    if isinstance(amt, float) and amt.is_integer():
        amt = int(amt)
    try:
        amt = int(amt) if not isinstance(amt, bool) and amt is not None else None
    except (TypeError, ValueError):
        amt = None
    if not vendor or not inv or amt is None or not re.fullmatch(r"[A-Z]{3}", cur):
        return None, "facts need vendorName, invoiceNumber, integer amountMinor, 3-letter currency"
    refs = []
    for r in d.get("evidenceRefs") or []:
        r = str(r).strip()
        if r not in text and f"[{r}]" in text:
            r = f"[{r}]"
        if not KEEP_BRACKETS:
            r = r.strip("[]")
        if (r if KEEP_BRACKETS else f"[{r}]") in text and r not in refs:
            refs.append(r)
    if len(refs) != 3:
        return None, "evidenceRefs must be exactly three references copied exactly from the package"
    return {"action": d["action"], "facts": {"vendorName": vendor, "invoiceNumber": inv,
            "amountMinor": amt, "currency": cur}, "evidenceRefs": refs,
            "reason": str(d.get("reason") or "").strip()}, None

def decide(packages, policy_rev):
    """packages -> {packageId: decision}. One batched call for uncached packages, one repair call at most."""
    res, todo = {}, []
    for p in packages:
        r = q("SELECT json FROM decisions WHERE key=?", (pkg_key(p),))
        if r:
            res[p["packageId"]] = json.loads(r[0][0])
        else:
            todo.append(p)
    for attempt in range(2):
        if not todo:
            break
        note = "" if attempt == 0 else "\nYour previous answer was invalid for some packages. Fix: action must be one of the five exact names; evidenceRefs exactly three, copied exactly; facts complete."
        try:
            raw = parse_json(call_llm(f"POLICY_REVISION: {policy_rev}{note}\nPACKAGES:\n" +
                                      json.dumps(todo, ensure_ascii=False, separators=(",", ":"))))
            got = {x.get("packageId"): x for x in raw.get("proposals", []) if isinstance(x, dict)}
        except Exception:
            got = {}
        still = []
        for p in todo:
            dec, _ = clean(p, got.get(p["packageId"]))
            if dec:
                res[p["packageId"]] = dec
                q("INSERT OR REPLACE INTO decisions VALUES(?,?)", (pkg_key(p), json.dumps(dec)))
            else:
                still.append(p)
        todo = still
    for p in todo:     # never settle when unsure; not cached so a later Check retries
        text = "\n".join(leaves(p))
        refs = (all_refs(text) + ["[n/a]"] * 3)[:3]
        res[p["packageId"]] = {"action": "hold_invoice", "facts": {"vendorName": "unknown", "invoiceNumber": "unknown",
                               "amountMinor": 0, "currency": "INR"}, "evidenceRefs": refs,
                               "reason": "Automated analysis could not verify this package, so payment is paused for manual verification."}
    return res

def rationale(action, reason, refs):
    s = f"Chosen action: {action}. {reason} Decisive evidence: {refs[0]}, {refs[1]} and {refs[2]}."
    if len(s) > 1500:
        s = f"Chosen action: {action}. {reason[:1200]} Decisive evidence: {refs[0]}, {refs[1]} and {refs[2]}."
    return s.ljust(60, ".") if len(s) < 60 else s

def build_proposals(data):
    dec = decide(data["packages"], data.get("policyRevision"))
    out_ = []
    for p in data["packages"]:
        d = dec[p["packageId"]]
        out_.append({"packageId": p["packageId"],
                     "actionId": "act-" + sha(f"{data['batchId']}|{p['packageId']}|{pkg_key(p)}")[:24],
                     "action": d["action"], "facts": d["facts"], "evidenceRefs": d["evidenceRefs"],
                     "rationale": rationale(d["action"], d["reason"], d["evidenceRefs"])})
    return out_


# ------------------------------------------------------------------ lifecycle
def work(tid, principal, data):
    try:
        proposals = build_proposals(data)
    except Exception:
        return
    with lock_for(("t", tid)):
        t = load_task(tid, principal)
        if not t or t["status"]["state"] != WORKING:      # canceled meanwhile: discard
            return
        t["artifacts"] = [{"artifactId": "art-" + uuid.uuid4().hex[:12], "name": "invoice-action-proposals",
                           "parts": [{"mediaType": MT_PROP, "data": {"batchId": data["batchId"], "proposals": proposals}}]}]
        t["status"] = {"state": INPUT, "timestamp": now()}
        save_task(t, principal)

def start_task(principal, msg, h):
    data = find_part(msg, MT_BATCH)
    pk = data.get("packages") if data else None
    if (not data or not isinstance(data.get("batchId"), str) or not data["batchId"] or not isinstance(pk, list) or not pk
            or not all(isinstance(p, dict) and isinstance(p.get("packageId"), str) for p in pk)
            or len({p["packageId"] for p in pk}) != len(pk)):
        return err(400, "INVALID_ARGUMENT", "Invalid invoice batch")
    tid, ctx = str(uuid.uuid4()), msg.get("contextId") or str(uuid.uuid4())
    t = {"id": tid, "contextId": ctx, "status": {"state": WORKING, "timestamp": now()},
         "history": [dict(msg, taskId=tid, contextId=ctx)]}
    save_task(t, principal)
    q("INSERT INTO msgs VALUES(?,?,?,?,NULL)", (principal, msg["messageId"], h, tid))
    fut = POOL.submit(work, tid, principal, data)
    try:
        fut.result(timeout=SEND_WAIT)
    except FutTimeout:
        pass
    t = load_task(tid, principal)
    body = json.dumps({"task": t}, ensure_ascii=False, separators=(",", ":"))
    if t["status"]["state"] != WORKING:
        q("UPDATE msgs SET snapshot=? WHERE principal=? AND message_id=?", (body, principal, msg["messageId"]))
    return Response(body.encode(), media_type=A2A)

def continue_task(principal, msg, h):
    tid = msg["taskId"]
    with lock_for(("t", tid)):
        t = load_task(tid, principal) if isinstance(tid, str) else None
        if not t:
            return err(404, "TASK_NOT_FOUND", "Task not found")
        if t["status"]["state"] != INPUT:
            return err(409, "TASK_NOT_ACCEPTING_RESULTS", "Task cannot accept results in its current state")
        data = find_part(msg, MT_RES)
        props_part = t["artifacts"][0]["parts"][0]["data"]
        props = {p["packageId"]: p for p in props_part["proposals"]}
        bad = (msg.get("contextId") != t["contextId"] or not data or data.get("batchId") != props_part["batchId"]
               or not isinstance(data.get("results"), list))
        accepted, seen = {}, set()
        if not bad:
            for r in data["results"]:
                p = props.get(r.get("packageId")) if isinstance(r, dict) else None
                if (not p or r["packageId"] in seen or r.get("actionId") != p["actionId"] or r.get("action") != p["action"]
                        or r.get("outcome") not in ("ACCEPTED", "REJECTED")
                        or (r["outcome"] == "ACCEPTED" and not (isinstance(r.get("receiptNonce"), str) and r["receiptNonce"]))):
                    bad = True
                    break
                seen.add(r["packageId"])
                if r["outcome"] == "ACCEPTED":
                    accepted[r["packageId"]] = r["receiptNonce"]
        if bad:
            return err(400, "INVALID_ARGUMENT", "Results do not match this task")
        executions = [{"packageId": pid, "actionId": props[pid]["actionId"], "action": props[pid]["action"],
                       "receiptNonce": accepted[pid], "facts": props[pid]["facts"],
                       "evidenceRefs": props[pid]["evidenceRefs"]}
                      for pid in props if pid in accepted]
        t["history"].append(dict(msg, taskId=tid, contextId=t["contextId"]))
        t["artifacts"].append({"artifactId": "art-" + uuid.uuid4().hex[:12], "name": "invoice-action-receipts",
                               "parts": [{"mediaType": MT_REC, "data": {"batchId": props_part["batchId"], "executions": executions}}]})
        t["status"] = {"state": DONE, "timestamp": now()}
        save_task(t, principal)
        body = json.dumps({"task": t}, ensure_ascii=False, separators=(",", ":"))
        q("INSERT INTO msgs VALUES(?,?,?,?,?)", (principal, msg["messageId"], h, tid, body))
    return Response(body.encode(), media_type=A2A)

def do_send(principal, raw):
    try:
        req = json.loads(raw)
        msg = req["message"]
        mid = msg["messageId"]
        ok = (isinstance(mid, str) and 0 < len(mid) <= 256 and msg.get("role") == "ROLE_USER"
              and isinstance(msg.get("parts"), list) and msg["parts"])
    except Exception:
        ok = False
    if not ok:
        return err(400, "INVALID_ARGUMENT", "Invalid message")
    h = sha(canon(msg))                                   # message only; configuration ignored
    with lock_for(("m", principal, mid)):                 # concurrent equivalents serialize here
        row = q("SELECT hash, task_id, snapshot FROM msgs WHERE principal=? AND message_id=?", (principal, mid))
        if row:
            if row[0][0] != h:
                return err(409, "IDEMPOTENCY_CONFLICT", "Message ID was already used with different content")
            if row[0][2]:
                return Response(row[0][2].encode(), media_type=A2A)
            return out({"task": load_task(row[0][1], principal)})
        return continue_task(principal, msg, h) if msg.get("taskId") else start_task(principal, msg, h)

def do_cancel(principal, tid):
    with lock_for(("t", tid)):
        t = load_task(tid, principal)
        if not t:
            return err(404, "TASK_NOT_FOUND", "Task not found")
        if t["status"]["state"] in TERMINAL:
            return err(409, "TASK_NOT_CANCELABLE", "Task is already in a terminal state")
        t["status"] = {"state": CANCELED, "timestamp": now()}
        save_task(t, principal)
    return out(t)


# ------------------------------------------------------------------ routing
def card():
    return {"name": "Invoice Action Agent",
            "description": "Reads invoice batches, proposes one typed action per package with exact evidence, "
                           "and records receipts only for proposals the grader accepted.",
            "version": "1.0.0",
            "supportedInterfaces": [{"url": BASE_URL, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}],
            "capabilities": {"streaming": False, "pushNotifications": False},
            "securitySchemes": {"bearerAuth": {"httpAuthSecurityScheme": {"scheme": "Bearer"}}},
            "securityRequirements": [{"schemes": {"bearerAuth": {"list": []}}}],
            "defaultInputModes": [MT_BATCH, MT_RES],
            "defaultOutputModes": [MT_PROP, MT_REC],
            "skills": [{"id": "invoice_action_agent", "name": "Invoice action agent",
                        "description": "Choose settle, approve, hold, reject-duplicate or exception for each invoice package.",
                        "tags": ["invoice", "payments", "approval"]}]}

def dispatch(method, path, query, h, raw):
    if path in ("/.well-known/agent-card.json", BASE_PATH + "/.well-known/agent-card.json"):
        return out(card(), mt="application/json") if method == "GET" else err(405, "METHOD_NOT_ALLOWED", "Method not allowed")
    if BASE_PATH and not path.startswith(BASE_PATH + "/"):
        return err(404, "NOT_FOUND", "Not found")
    rel = path[len(BASE_PATH):]
    m_task = re.fullmatch(r"/tasks/([^/:]+)", rel)
    m_cancel = re.fullmatch(r"/tasks/([^/:]+):cancel", rel)
    allowed = {"/message:send": "POST", "/tasks": "GET", "/tasks/": "GET"}.get(rel) or ("GET" if m_task else "POST" if m_cancel else None)
    if not allowed:
        return err(404, "NOT_FOUND", "Not found")
    principal, bad = gate(h, method, bool(raw))
    if bad:
        return bad
    if method != allowed:
        return err(405, "METHOD_NOT_ALLOWED", "Method not allowed")
    if rel == "/message:send":
        return do_send(principal, raw)
    if m_cancel:
        return do_cancel(principal, m_cancel.group(1))
    if m_task:
        t = load_task(m_task.group(1), principal)
        if not t:
            return err(404, "TASK_NOT_FOUND", "Task not found")
        n = (parse_qs(query).get("historyLength") or [None])[0]
        if n is not None and n.isdigit():
            t = dict(t, history=t.get("history", [])[-int(n):] if int(n) else [])
        return out(t)
    rows = [json.loads(r[0]) for r in q("SELECT task_json FROM tasks WHERE principal=? ORDER BY created DESC", (principal,))]
    while rows and len(json.dumps({"tasks": rows})) > MAX_BYTES:
        rows.pop()
    return out({"tasks": rows})


@app.get("/_test/stats")
def _stats():
    return STATS if MOCK else Response(status_code=404)

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def any_route(path: str, request: Request):
    raw = await request.body()
    h = {k.lower(): v for k, v in request.headers.items()}
    log({"ts": now(), "method": request.method, "path": "/" + path, "query": request.url.query,
         "headers": {k: v for k, v in h.items() if k != "authorization"},
         "auth": "bearer:" + sha(h["authorization"])[:8] if "authorization" in h else None,
         "body": raw.decode("utf-8", "replace")})
    return await run_in_threadpool(dispatch, request.method, "/" + path, request.url.query, h, raw)
