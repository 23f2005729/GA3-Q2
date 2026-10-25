"""Local contract test.  Start the server first, e.g.:
  BASE_URL=http://127.0.0.1:8000/a2a/ MOCK_LLM=1 uvicorn main:app --port 8000
then:  python test_local.py
"""
import json, uuid
from concurrent.futures import ThreadPoolExecutor
import httpx

ROOT = "http://127.0.0.1:8000"
BASE = ROOT + "/a2a"
c = httpx.Client(trust_env=False, timeout=60)
fails = []

def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  {extra}" if (extra and not cond) else ""))
    if not cond:
        fails.append(name)

def H(tok="alice", **kw):
    h = {"Authorization": f"Bearer {tok}", "A2A-Version": "1.0", "Content-Type": "application/a2a+json",
         "Accept": "application/a2a+json"}
    h.update(kw)
    return h

def stats():
    return c.get(ROOT + "/_test/stats").json()["llm_calls"]

ACTS = ["settle_invoice", "request_approval", "hold_invoice", "reject_duplicate", "open_exception"]

def pkg(pid, i, action, tag="x"):
    return {"packageId": pid, "documents": [{"text":
        f"Cover sheet [COVER-{tag}{i}]. The archive example [OLD-{tag}{i}] mentions hold but is obsolete. "
        f"Current record: ACTION={action}; VENDOR=Acme {tag}{i} Ltd; INV=INV-{tag}{i}; AMT={i + 1}00000; CUR=INR; "
        f"[EV-{tag}{i}-A] [EV-{tag}{i}-B] [EV-{tag}{i}-C]"}]}

def batch_msg(packages, batch="b-" + uuid.uuid4().hex[:6], mid=None):
    return {"messageId": mid or "m-" + uuid.uuid4().hex[:8], "role": "ROLE_USER",
            "parts": [{"mediaType": "application/vnd.ga5.invoice-claim-batch+json",
                       "data": {"batchId": batch, "policyRevision": "rev-7", "packages": packages}}]}

def send(msg, tok="alice", ri=False):
    return c.post(BASE + "/message:send", headers=H(tok), content=json.dumps(
        {"message": msg, "configuration": {"returnImmediately": ri, "historyLength": 20,
         "acceptedOutputModes": ["application/vnd.ga5.invoice-action-proposals+json"]}}))

def new_task(tag, tok="alice", n=4):
    pk = [pkg(f"p{i}", i, ACTS[i % 5], tag) for i in range(n)]
    r = send(batch_msg(pk), tok)
    return r.json()["task"], pk

def results_msg(task, outcomes=None, **over):
    props = task["artifacts"][0]["parts"][0]["data"]["proposals"]
    res = [{"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
            "outcome": (outcomes or {}).get(i, "ACCEPTED" if i % 2 == 0 else "REJECTED"),
            "receiptNonce": "nonce-" + uuid.uuid4().hex[:10]} for i, p in enumerate(props)]
    m = {"messageId": "r-" + uuid.uuid4().hex[:8], "taskId": task["id"], "contextId": task["contextId"],
         "role": "ROLE_USER", "parts": [{"mediaType": "application/vnd.ga5.invoice-action-results+json",
         "data": {"batchId": task["artifacts"][0]["parts"][0]["data"]["batchId"], "results": res}}]}
    m.update(over)
    return m, res

# ---- card and gates
r = c.get(ROOT + "/.well-known/agent-card.json")
card = r.json()
check("card public 200", r.status_code == 200)
check("card exact base url", card["supportedInterfaces"][0] == {"url": BASE + "/", "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"})
check("card fields", card["name"] and card["description"] and card["version"] and isinstance(card["capabilities"], dict)
      and card["skills"][0]["id"] == "invoice_action_agent" and card["skills"][0]["tags"])
check("card modes", "application/vnd.ga5.invoice-claim-batch+json" in card["defaultInputModes"]
      and len(card["defaultOutputModes"]) == 2)
check("no auth -> 401", c.get(BASE + "/tasks").status_code == 401)
check("bad scheme -> 401", c.get(BASE + "/tasks", headers={"Authorization": "Basic abc", "A2A-Version": "1.0"}).status_code == 401)
check("wrong version -> 400", c.get(BASE + "/tasks", headers=H(**{"A2A-Version": "0.3"})).status_code == 400)
check("missing version -> 400", c.get(BASE + "/tasks", headers={"Authorization": "Bearer alice"}).status_code == 400)
check("json content type -> 415", c.post(BASE + "/message:send", headers=H(**{"Content-Type": "application/json"}), content="{}").status_code == 415)
check("accept xml -> 406", c.get(BASE + "/tasks", headers=H(Accept="application/xml")).status_code == 406)
check("no redirect on /tasks/", c.get(BASE + "/tasks/", headers=H()).status_code == 200)

# ---- initial message
pk = [pkg(f"p{i}", i, ACTS[i % 5]) for i in range(5)]
msg = batch_msg(pk, batch="batch-A")
before = stats()
r = send(msg)
t = r.json()["task"]
check("send 200 a2a+json", r.status_code == 200 and r.headers["content-type"].startswith("application/a2a+json"))
check("INPUT_REQUIRED", t["status"]["state"] == "TASK_STATE_INPUT_REQUIRED")
check("one artifact, one part", len(t["artifacts"]) == 1 and len(t["artifacts"][0]["parts"]) == 1
      and t["artifacts"][0]["parts"][0]["mediaType"] == "application/vnd.ga5.invoice-action-proposals+json")
props = t["artifacts"][0]["parts"][0]["data"]["proposals"]
check("one llm call for 5 packages", stats() - before == 1)
check("actions follow documents", [p["action"] for p in props] == [ACTS[i % 5] for i in range(5)])
check("proposal shape", all(len(p["actionId"]) >= 12 and 60 <= len(p["rationale"]) <= 1500 and p["action"] in p["rationale"]
      and sum(ref in p["rationale"] for ref in p["evidenceRefs"]) >= 2 and len(p["evidenceRefs"]) == 3
      and p["facts"]["currency"] == "INR" and isinstance(p["facts"]["amountMinor"], int) for p in props))
check("cover/archive refs excluded", all("COVER" not in x and "OLD" not in x for p in props for x in p["evidenceRefs"]))
check("unique action ids", len({p["actionId"] for p in props}) == 5)
check("history keeps initial message", t["history"][0]["messageId"] == msg["messageId"] and t["history"][0]["taskId"] == t["id"])
check("no receipts yet", "receipts" not in json.dumps(t["artifacts"]))

# ---- idempotency
body_a = send(msg).text
check("replay identical", body_a == r.text)
reordered = dict(reversed(list(msg.items())))
r2 = c.post(BASE + "/message:send", headers=H(), content=json.dumps({"configuration": {"returnImmediately": True}, "message": reordered}))
check("reordered keys + returnImmediately -> same task", r2.text == r.text)
changed = json.loads(json.dumps(msg)); changed["parts"][0]["data"]["policyRevision"] = "rev-8"
r3 = send(changed)
check("changed content -> 409 IDEMPOTENCY_CONFLICT", r3.status_code == 409 and "IDEMPOTENCY_CONFLICT" in r3.text)
check("original unchanged after conflict", c.get(f"{BASE}/tasks/{t['id']}", headers=H()).json()["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"
      and c.get(f"{BASE}/tasks/{t['id']}", headers=H()).json()["history"][0]["parts"][0]["data"]["policyRevision"] == "rev-7")

# ---- concurrency
cm = batch_msg([pkg(f"p{i}", i, ACTS[i % 5], "conc") for i in range(4)])
before = stats()
with ThreadPoolExecutor(8) as ex:
    rs = list(ex.map(lambda ri: send(cm, ri=ri % 2 == 0), range(8)))
ids = {x.json()["task"]["id"] for x in rs}
check("8 concurrent equivalents -> one task", len(ids) == 1 and len({x.text for x in rs}) == 1)
check("... and one model call", stats() - before == 1)
check("concurrent result complete", rs[0].json()["task"]["status"]["state"] == "TASK_STATE_INPUT_REQUIRED")

# ---- cache by package content
before = stats()
r = send(batch_msg([pkg(f"other{i}", i, ACTS[i % 5]) for i in range(5)], batch="batch-B"))
t2 = r.json()["task"]
check("cached content -> no model call", stats() - before == 0)
check("cached decisions identical, ids differ", [p["action"] for p in t2["artifacts"][0]["parts"][0]["data"]["proposals"]] == [p["action"] for p in props]
      and t2["artifacts"][0]["parts"][0]["data"]["proposals"][0]["actionId"] != props[0]["actionId"])

# ---- continuation validation
bad_cases = {
    "wrong actionId": lambda m, res: res[0].update(actionId="act-zzzzzzzzzzzz"),
    "wrong action": lambda m, res: res[0].update(action="settle_invoice" if res[0]["action"] != "settle_invoice" else "hold_invoice"),
    "wrong package": lambda m, res: res[0].update(packageId="nope"),
    "wrong batch": lambda m, res: m["parts"][0]["data"].update(batchId="other"),
    "wrong context": lambda m, res: m.update(contextId="ctx-other"),
}
for name, mut in bad_cases.items():
    m, res = results_msg(t)
    mut(m, res)
    rr = send(m)
    check(f"continuation rejected: {name}", rr.status_code in (400, 404, 409, 422))
check("task untouched after bad continuations", c.get(f"{BASE}/tasks/{t['id']}", headers=H()).json()["status"]["state"] == "TASK_STATE_INPUT_REQUIRED")
m, _ = results_msg(t)
check("outsider continuation -> 404/403", send(m, "bob").status_code in (403, 404))

m, res = results_msg(t)
r = send(m)
done = r.json()["task"]
check("continuation -> COMPLETED", r.status_code == 200 and done["status"]["state"] == "TASK_STATE_COMPLETED")
rec = [p for a in done["artifacts"] for p in a["parts"] if p["mediaType"].endswith("receipts+json")]
prop_parts = [p for a in done["artifacts"] for p in a["parts"] if p["mediaType"].endswith("proposals+json")]
check("proposal kept, exactly one receipts part", len(rec) == 1 and len(prop_parts) == 1)
ex = rec[0]["data"]["executions"]
acc = {x["packageId"]: x["receiptNonce"] for x in res if x["outcome"] == "ACCEPTED"}
check("executions = accepted only, nonces exact", {e["packageId"]: e["receiptNonce"] for e in ex} == acc)
check("executions match stored proposals", all(e["facts"] == next(p for p in props if p["packageId"] == e["packageId"])["facts"]
      and e["actionId"] == next(p for p in props if p["packageId"] == e["packageId"])["actionId"] for e in ex))
check("history has both messages", [h["messageId"] for h in done["history"]] == [msg["messageId"], m["messageId"]])
check("replay of continuation identical", send(m).text == r.text)
m2, _ = results_msg(t)
check("second continuation after terminal -> 409", send(m2).status_code == 409)
check("cancel after terminal -> 409", c.post(f"{BASE}/tasks/{t['id']}:cancel", headers=H()).status_code == 409)
check("terminal task immutable", c.get(f"{BASE}/tasks/{t['id']}", headers=H()).json() == done)

# ---- cancel
tc, _ = new_task("cancel")
rc = c.post(f"{BASE}/tasks/{tc['id']}:cancel", headers=H())
check("cancel INPUT task -> CANCELED, no receipts", rc.status_code == 200 and rc.json()["status"]["state"] == "TASK_STATE_CANCELED"
      and "receipts" not in json.dumps(rc.json()["artifacts"]))
mm, _ = results_msg(tc)
check("results after cancel -> 409", send(mm).status_code == 409)
check("cancel twice -> 409", c.post(f"{BASE}/tasks/{tc['id']}:cancel", headers=H()).status_code == 409)

# ---- receipt vs cancel race
bad_race = 0
for i in range(10):
    tr, _ = new_task(f"race{i}")
    mm, _ = results_msg(tr)
    with ThreadPoolExecutor(2) as ex2:
        f1 = ex2.submit(send, mm)
        f2 = ex2.submit(lambda: c.post(f"{BASE}/tasks/{tr['id']}:cancel", headers=H()))
        a, b = f1.result(), f2.result()
    final = c.get(f"{BASE}/tasks/{tr['id']}", headers=H()).json()
    has_rec = "receipts+json" in json.dumps(final["artifacts"])
    ok = (sorted([a.status_code, b.status_code]) == [200, 409]
          and ((final["status"]["state"] == "TASK_STATE_COMPLETED" and has_rec) or (final["status"]["state"] == "TASK_STATE_CANCELED" and not has_rec)))
    bad_race += not ok
check("10 receipt/cancel races: exactly one 200, consistent final", bad_race == 0)

# ---- isolation
check("outsider GET task -> 404/403", c.get(f"{BASE}/tasks/{t['id']}", headers=H("bob")).status_code in (403, 404))
rb = c.get(f"{BASE}/tasks/{t['id']}", headers=H("bob"))
check("outsider error body generic", t["id"] not in rb.text)
check("outsider cancel -> 404/403", c.post(f"{BASE}/tasks/{t['id']}:cancel", headers=H("bob")).status_code in (403, 404))
check("outsider list empty", c.get(BASE + "/tasks", headers=H("bob")).json() == {"tasks": []})
al = c.get(BASE + "/tasks", headers=H()).json()["tasks"]
check("owner list has own tasks only", len(al) >= 5 and t["id"] in {x["id"] for x in al})
rb = send(msg, "bob")
check("same messageId from another user is independent", rb.status_code == 200 and rb.json()["task"]["id"] != t["id"])
check("alice still unaffected", c.get(f"{BASE}/tasks/{t['id']}", headers=H()).json() == done)
check("outsider can't see bob's via alice list", rb.json()["task"]["id"] not in {x["id"] for x in c.get(BASE + "/tasks", headers=H()).json()["tasks"]})
check("unknown task -> 404", c.get(f"{BASE}/tasks/does-not-exist", headers=H()).status_code == 404)

print("\n" + ("ALL PASSED" if not fails else f"{len(fails)} FAILED: {fails}"))
