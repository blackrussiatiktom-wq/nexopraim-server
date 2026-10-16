from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict, List
import json, sqlite3, datetime, os

app = FastAPI()

DB = "/tmp/chat.db"

def init_db():
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user TEXT, text TEXT, ts TEXT)""")
    con.commit(); con.close()
init_db()

class Room:
    def __init__(self):
        self.clients: List[WebSocket] = []
    async def join(self, ws): self.clients.append(ws)
    def leave(self, ws):
        if ws in self.clients: self.clients.remove(ws)
    async def broadcast(self, msg: dict):
        dead = []
        for c in self.clients:
            try: await c.send_json(msg)
            except: dead.append(c)
        for d in dead: self.leave(d)

rooms: Dict[str, Room] = {}

@app.get("/")
def root(): return {"status": "ok", "app": "Nexopraim"}

@app.websocket("/ws/{room}/{user}")
async def ws_endpoint(ws: WebSocket, room: str, user: str):
    await ws.accept()
    r = rooms.setdefault(room, Room())
    await r.join(ws)
    con = sqlite3.connect(DB)
    rows = con.execute("SELECT user,text,ts FROM messages ORDER BY id DESC LIMIT 50").fetchall()
    con.close()
    for u, t, ts in reversed(rows):
        await ws.send_json({"user": u, "text": t, "ts": ts})
    try:
        while True:
            data = await ws.receive_json()
            data["user"] = user
            data["ts"] = datetime.datetime.now().isoformat(timespec="seconds")
            con = sqlite3.connect(DB)
            con.execute("INSERT INTO messages(user,text,ts) VALUES(?,?,?)",
                        (user, data["text"], data["ts"]))
            con.commit(); con.close()
            await r.broadcast(data)
    except WebSocketDisconnect:
        r.leave(ws)
