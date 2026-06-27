"""
Bully algorithm for leader election.

The node with the highest ID that is reachable wins the election.
"""

import os
import threading
import time
from datetime import datetime, timezone

import requests

NODE_ID = int(os.environ.get("NODE_ID", "0") or 0)
SELF_URL = os.environ.get("SELF_URL", "http://localhost:8080").rstrip("/")
PEER_NODES_RAW = os.environ.get("PEER_NODES", "")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "5"))
TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "2"))

elected_leader: dict | None = None
last_heartbeat: datetime | None = None
_in_election = False
_monitor_started = False
_lock = threading.RLock()


def _parse_peers(raw: str) -> list[dict]:
    result = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" in entry:
            id_part, url_part = entry.split("=", 1)
            try:
                nid = int(id_part.strip())
            except ValueError:
                nid = None
            url = url_part.strip().rstrip("/")
        else:
            nid = None
            url = entry.rstrip("/")
        if not url.startswith("http"):
            url = f"http://{url}"
        if url:
            result.append({"id": nid, "url": url})
    return result


PEERS = [p for p in _parse_peers(PEER_NODES_RAW) if p["url"] != SELF_URL]


def _peers_with_higher_id() -> list[dict]:
    higher = [p for p in PEERS if p.get("id") is not None and p["id"] > NODE_ID]
    return sorted(higher, key=lambda p: p["id"]) if higher else [p for p in PEERS if p.get("id") is None]


def start_election() -> dict:
    global elected_leader, _in_election
    with _lock:
        if _in_election:
            return {"status": "already_in_progress"}
        _in_election = True
        elected_leader = None

    targets = _peers_with_higher_id()
    if not targets:
        return declare_victory()

    any_ok = False
    for peer in targets:
        try:
            r = requests.post(f"{peer['url']}/election", json={"sender_id": NODE_ID}, timeout=TIMEOUT)
            if r.ok:
                any_ok = True
        except requests.RequestException:
            pass

    if any_ok:
        with _lock:
            _in_election = False
        return {"status": "waiting_for_higher_node"}

    return declare_victory()


def handle_election_message(sender_id: int) -> dict:
    if NODE_ID > sender_id:
        with _lock:
            if not _in_election:
                threading.Thread(target=start_election, daemon=True).start()
        return {"accepted": True}
    return {"accepted": False}


def declare_victory() -> dict:
    global elected_leader, last_heartbeat, _in_election
    with _lock:
        elected_leader = {"id": NODE_ID, "url": SELF_URL}
        last_heartbeat = datetime.now(timezone.utc)
        _in_election = False

    for peer in PEERS:
        try:
            requests.post(
                f"{peer['url']}/coordinator",
                json={"leader_id": NODE_ID, "leader_url": SELF_URL},
                timeout=TIMEOUT,
            )
        except requests.RequestException:
            pass

    return {"status": "declared_victory", "leader_id": NODE_ID, "leader_url": SELF_URL}


def handle_coordinator_message(leader_id: int, leader_url: str) -> dict:
    global elected_leader, last_heartbeat, _in_election
    leader_url = leader_url.rstrip("/")
    if not leader_url.startswith("http"):
        leader_url = f"http://{leader_url}"

    with _lock:
        if leader_id < NODE_ID and NODE_ID != 0:
            threading.Thread(target=start_election, daemon=True).start()
            return {"status": "ignored_lower_leader"}
        elected_leader = {"id": leader_id, "url": leader_url}
        last_heartbeat = datetime.now(timezone.utc)
        _in_election = False

    return {"status": "coordinator_accepted", "leader_id": leader_id}


def heartbeat_check() -> dict:
    global elected_leader, last_heartbeat, _in_election
    with _lock:
        if _in_election:
            return {"status": "election_in_progress"}
        leader = elected_leader

    if leader is None:
        return start_election()

    try:
        r = requests.get(f"{leader['url']}/heartbeat", timeout=TIMEOUT)
        if r.ok:
            with _lock:
                last_heartbeat = datetime.now(timezone.utc)
            return {"status": "leader_alive", "leader_id": leader["id"]}
    except requests.RequestException:
        pass

    with _lock:
        elected_leader = None
        _in_election = False
    return start_election()


def initialize_election_monitor() -> None:
    global _monitor_started
    with _lock:
        if _monitor_started:
            return
        _monitor_started = True

    def loop():
        while True:
            heartbeat_check()
            time.sleep(HEARTBEAT_INTERVAL)

    threading.Thread(target=loop, daemon=True).start()
