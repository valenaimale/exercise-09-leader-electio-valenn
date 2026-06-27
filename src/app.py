from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src import election
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate

Base.metadata.create_all(bind=engine)
app = FastAPI()


@app.on_event("startup")
def on_startup() -> None:
    election.initialize_election_monitor()


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)


@app.post("/election")
def receive_election(payload: dict):
    sender_id = payload.get("sender_id")
    if sender_id is None:
        raise HTTPException(status_code=400, detail="Missing sender_id")
    return election.handle_election_message(int(sender_id))


@app.post("/coordinator")
def receive_coordinator(payload: dict):
    leader_id = payload.get("leader_id")
    leader_url = payload.get("leader_url", "")
    if leader_id is None or not leader_url:
        raise HTTPException(status_code=400, detail="Missing leader_id or leader_url")
    return election.handle_coordinator_message(int(leader_id), str(leader_url))


@app.get("/heartbeat")
def heartbeat():
    return {"alive": True}


@app.get("/leader")
def get_leader():
    hb = election.last_heartbeat
    return {
        "node_id": election.NODE_ID,
        "self_url": election.SELF_URL,
        "leader": election.elected_leader,
        "election_in_progress": election._in_election,
        "last_heartbeat": hb.isoformat() if hb else None,
        "peers": election.PEERS,
    }


@app.post("/trigger-election")
def trigger_election():
    return election.start_election()
    return Response(status_code=204)
