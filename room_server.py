import asyncio
import http
import json
import os
import random
import sys
import websockets

PORT = int(os.environ.get("PORT", 8765))
HOST = "0.0.0.0"

# Room storage: room_code -> Room object
rooms = {}
# Connection to room mapping: websocket -> (room, player_idx)
connections = {}

def get_weighted_dice_roll() -> int:
    """Weighted dice roll: 1-3 have ~85% chance, 4-6 have ~15%."""
    r = random.random() * 100.0
    if r < 30.0:
        return 1
    elif r < 60.0:
        return 2
    elif r < 85.0:
        return 3
    elif r < 93.0:
        return 4
    elif r < 98.0:
        return 5
    else:
        return 6

class GameRoom:
    def __init__(self, code: str, p0_ws, turns: int, zone_ids: list, rand_seed: int):
        self.code = code
        self.p0 = p0_ws
        self.p1 = None
        self.turns = turns
        self.zone_ids = zone_ids
        self.rand_seed = rand_seed
        self.committed_plays = [None, None]
        self.current_turn = 1

    async def send_to_player(self, player_idx: int, data: dict):
        ws = self.p0 if player_idx == 0 else self.p1
        if ws:
            try:
                await ws.send(json.dumps(data))
            except Exception:
                pass

    async def broadcast(self, data: dict):
        msg = json.dumps(data)
        for ws in [self.p0, self.p1]:
            if ws:
                try:
                    await ws.send(msg)
                except Exception:
                    pass

    async def handle_submit_plays(self, p_idx: int, plays: list):
        self.committed_plays[p_idx] = plays
        opp_idx = 1 if p_idx == 0 else 0
        
        # Notify opponent that P submitted N hidden cards
        await self.send_to_player(opp_idx, {
            "type": "OPPONENT_COMMITTED",
            "count": len(plays)
        })

        # If both submitted -> reveal!
        if self.committed_plays[0] is not None and self.committed_plays[1] is not None:
            dice_val = get_weighted_dice_roll()
            reveal_msg = {
                "type": "REVEAL_ALL",
                "p0_plays": self.committed_plays[0],
                "p1_plays": self.committed_plays[1],
                "dice_val": dice_val
            }
            await self.broadcast(reveal_msg)
            self.committed_plays = [None, None]

async def process_http_request(connection, request):
    # Only respond to plain HTTP requests (e.g. Render/Railway health checks), NOT websocket upgrades!
    upgrade = request.headers.get("Upgrade", "").lower()
    if upgrade != "websocket":
        if request.path in ["/", "/health", "/status"]:
            body = json.dumps({
                "status": "ok",
                "game": "Little Wars",
                "service": "Cloud Room Relay Server",
                "active_rooms": len(rooms),
                "total_players": len(connections)
            }) + "\n"
            return connection.respond(http.HTTPStatus.OK, body)
    return None

async def ws_handler(websocket):
    print(f"[WS] New client connected: {websocket.remote_address}")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except Exception:
                continue

            msg_type = data.get("type", "")

            # Ping keepalive
            if msg_type == "PING":
                await websocket.send(json.dumps({"type": "PONG"}))
                continue

            # CREATE ROOM
            if msg_type == "CREATE_ROOM":
                # Generate unique 4-digit code
                for _ in range(50):
                    code = str(random.randint(1000, 9999))
                    if code not in rooms:
                        break
                
                turns = int(data.get("turns", 6))
                zone_ids = data.get("zone_ids", [])
                rand_seed = int(data.get("rand_seed", random.randint(1, 9999999)))

                room = GameRoom(code, websocket, turns, zone_ids, rand_seed)
                rooms[code] = room
                connections[websocket] = (room, 0)

                print(f"[ROOM] Created room {code} by Player 0")
                await websocket.send(json.dumps({
                    "type": "ROOM_CREATED",
                    "room_code": code,
                    "player_idx": 0
                }))
                continue

            # JOIN ROOM
            elif msg_type == "JOIN_ROOM":
                code = str(data.get("room_code", "")).strip()
                if code not in rooms:
                    await websocket.send(json.dumps({
                        "type": "ERROR",
                        "message": f"Комната {code} не найдена. Проверьте код!"
                    }))
                    continue

                room = rooms[code]
                if room.p1 is not None:
                    await websocket.send(json.dumps({
                        "type": "ERROR",
                        "message": f"Комната {code} уже заполнена (2/2)!"
                    }))
                    continue

                room.p1 = websocket
                connections[websocket] = (room, 1)
                print(f"[ROOM] Player 1 joined room {code}! Starting match.")

                # Notify Player 1 of join success
                await websocket.send(json.dumps({
                    "type": "ROOM_JOINED",
                    "room_code": code,
                    "player_idx": 1
                }))

                # Broadcast match start to both
                await room.send_to_player(0, {
                    "type": "MATCH_START",
                    "player_idx": 0,
                    "room_code": code,
                    "turns": room.turns,
                    "zone_ids": room.zone_ids,
                    "rand_seed": room.rand_seed
                })
                await room.send_to_player(1, {
                    "type": "MATCH_START",
                    "player_idx": 1,
                    "room_code": code,
                    "turns": room.turns,
                    "zone_ids": room.zone_ids,
                    "rand_seed": room.rand_seed
                })
                continue

            # In-room messages
            if websocket in connections:
                room, p_idx = connections[websocket]
                opp_idx = 1 if p_idx == 0 else 0

                if msg_type == "CARD_STAGED":
                    await room.send_to_player(opp_idx, {
                        "type": "CARD_STAGED",
                        "zone_idx": data.get("zone_idx", 0)
                    })
                elif msg_type == "CARD_UNSTAGED":
                    await room.send_to_player(opp_idx, {
                        "type": "CARD_UNSTAGED",
                        "zone_idx": data.get("zone_idx", 0)
                    })
                elif msg_type == "PLAYER_READY":
                    await room.send_to_player(opp_idx, {
                        "type": "PLAYER_READY"
                    })
                elif msg_type == "SUBMIT_PLAYS":
                    plays = data.get("plays", [])
                    await room.handle_submit_plays(p_idx, plays)
                elif msg_type == "ADVANCE_TURN":
                    next_turn = data.get("turn", room.current_turn + 1)
                    room.current_turn = next_turn
                    await room.broadcast({
                        "type": "NEXT_TURN",
                        "turn": next_turn
                    })

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        print(f"[WS] Client disconnected: {websocket.remote_address}")
        if websocket in connections:
            room, p_idx = connections[websocket]
            opp_idx = 1 if p_idx == 0 else 0
            await room.send_to_player(opp_idx, {
                "type": "OPPONENT_LEFT",
                "message": "Соперник отключился от игры."
            })
            del connections[websocket]
            if room.p0 == websocket or room.p1 == websocket:
                if room.code in rooms:
                    del rooms[room.code]
                    print(f"[ROOM] Room {room.code} destroyed.")

async def main():
    print("=" * 50)
    print("  LITTLE WARS - Cloud Room Relay Server")
    print(f"  Listening on http://{HOST}:{PORT} & ws://{HOST}:{PORT}")
    print("=" * 50)
    async with websockets.serve(ws_handler, HOST, PORT, process_request=process_http_request):
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SERVER] Stopped by user.")
