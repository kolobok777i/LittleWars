import asyncio
import http
import json
import os
import random
import sys
import websockets

PORT = int(os.environ.get("PORT", 8765))
HOST = "0.0.0.0"
MIN_REQUIRED_VERSION = 15.1

def parse_version(raw) -> float:
    try:
        return float(str(raw).strip())
    except Exception:
        return 0.0

# Room storage: room_code -> GameRoom object
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
    def __init__(self, code: str, p0_ws, turns: int, zone_ids: list, rand_seed: int, p0_deck: list = None, p0_name: str = "Игрок 1", mode: str = "1v1"):
        self.code = code
        self.mode = mode # "1v1" or "2v2"
        self.turns = turns
        self.zone_ids = zone_ids
        self.rand_seed = rand_seed
        self.current_turn = 1
        
        # 1v1 attributes for backward compatibility
        self.p0 = p0_ws
        self.p1 = None
        self.p0_deck = p0_deck or []
        self.p1_deck = []
        self.p0_name = p0_name or "Игрок 1"
        self.p1_name = "Игрок 2"
        
        # 2v2 attributes: 4 player slots (0 & 1 = Team 0; 2 & 3 = Team 1)
        self.max_players = 4 if mode == "2v2" else 2
        self.players = [p0_ws] + [None] * (self.max_players - 1)
        self.player_names = [p0_name] + [""] * (self.max_players - 1)
        self.player_decks = [p0_deck or []] + [[]] * (self.max_players - 1)
        self.player_is_bot = [False] * self.max_players
        self.committed_plays = [None] * self.max_players
        self.advance_ready = [False] * self.max_players
        self.player_ready_planning = [False] * self.max_players
        self.match_started = False

    async def send_to_player(self, player_idx: int, data: dict):
        if player_idx < 0 or player_idx >= len(self.players):
            return
        ws = self.players[player_idx]
        if ws:
            try:
                await ws.send(json.dumps(data))
            except Exception:
                pass

    async def broadcast(self, data: dict):
        msg = json.dumps(data)
        for ws in self.players:
            if ws:
                try:
                    await ws.send(msg)
                except Exception:
                    pass

    async def broadcast_team(self, team_idx: int, data: dict):
        """Send message only to players of a specific team (0 or 1)."""
        msg = json.dumps(data)
        team_slots = [0, 1] if team_idx == 0 else [2, 3]
        for slot in team_slots:
            if slot < len(self.players):
                ws = self.players[slot]
                if ws:
                    try:
                        await ws.send(msg)
                    except Exception:
                        pass

    def get_lobby_players_info(self) -> list:
        info = []
        for i in range(self.max_players):
            ws = self.players[i]
            is_occupied = (ws is not None) or self.player_is_bot[i]
            team = 0 if i < 2 else 1
            info.append({
                "slot": i,
                "team": team,
                "name": self.player_names[i] if is_occupied else "Ожидание...",
                "occupied": is_occupied,
                "is_bot": self.player_is_bot[i]
            })
        return info

    async def broadcast_lobby_update(self):
        msg = {
            "type": "LOBBY_UPDATE",
            "room_code": self.code,
            "mode": self.mode,
            "max_players": self.max_players,
            "players": self.get_lobby_players_info()
        }
        await self.broadcast(msg)

    async def handle_submit_plays_1v1(self, p_idx: int, plays: list):
        self.committed_plays[p_idx] = plays
        opp_idx = 1 if p_idx == 0 else 0
        
        await self.send_to_player(opp_idx, {
            "type": "OPPONENT_COMMITTED",
            "count": len(plays)
        })

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

    async def handle_submit_plays_2v2(self, p_idx: int, plays: list):
        self.committed_plays[p_idx] = plays
        self.player_ready_planning[p_idx] = True

        # Notify teammate & opponents of commitment
        await self.broadcast({
            "type": "PLAYER_SUBMITTED_2V2",
            "player_idx": p_idx,
            "count": len(plays)
        })

        # Simulate bot plays for any bot players in room
        for i in range(self.max_players):
            if self.player_is_bot[i] and self.committed_plays[i] is None:
                # Bot commits basic simulated play
                bot_deck = self.player_decks[i] or ["farmer", "archer", "scout"]
                chosen_card = random.choice(bot_deck)
                chosen_z = random.randint(0, 2)
                self.committed_plays[i] = [{"card_id": chosen_card, "zone_idx": chosen_z, "player_idx": i}]
                self.player_ready_planning[i] = True

        # Check if all 4 players committed
        all_committed = all(p is not None for p in self.committed_plays[:self.max_players])
        if all_committed:
            dice_val = get_weighted_dice_roll()
            reveal_msg = {
                "type": "REVEAL_ALL_2V2",
                "p0_plays": self.committed_plays[0] or [],
                "p1_plays": self.committed_plays[1] or [],
                "p2_plays": self.committed_plays[2] or [],
                "p3_plays": self.committed_plays[3] or [],
                "dice_val": dice_val
            }
            await self.broadcast(reveal_msg)
            self.committed_plays = [None] * self.max_players
            self.player_ready_planning = [False] * self.max_players


async def process_http_request(connection, request):
    upgrade = request.headers.get("Upgrade", "").lower()
    if upgrade != "websocket":
        if request.path in ["/", "/health", "/status"]:
            body = json.dumps({
                "status": "ok",
                "game": "Little Wars",
                "service": "Cloud Room Relay Server (1v1 & 2v2)",
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

            # CREATE ROOM (Supports mode: "1v1" or "2v2")
            if msg_type == "CREATE_ROOM":
                client_ver = parse_version(data.get("game_version", 0))
                if client_ver < MIN_REQUIRED_VERSION:
                    await websocket.send(json.dumps({
                        "type": "ERROR",
                        "code": "OUTDATED_VERSION",
                        "message": "Ваша версия игры устарела! Требуется актуальная версия игры для сетевой игры."
                    }))
                    continue
                
                # Generate unique 4-digit code
                code = ""
                for _ in range(50):
                    cand = str(random.randint(1000, 9999))
                    if cand not in rooms:
                        code = cand
                        break
                
                turns = int(data.get("turns", 6))
                zone_ids = data.get("zone_ids", [])
                rand_seed = int(data.get("rand_seed", random.randint(1, 9999999)))
                p0_deck = data.get("deck", [])
                p0_name = str(data.get("player_name", data.get("name", "Игрок 1"))).strip() or "Игрок 1"
                mode = str(data.get("mode", "1v1")).lower()
                if mode != "2v2":
                    mode = "1v1"

                room = GameRoom(code, websocket, turns, zone_ids, rand_seed, p0_deck, p0_name, mode=mode)
                rooms[code] = room
                connections[websocket] = (room, 0)

                print(f"[ROOM] Created {mode.upper()} room {code} by {p0_name} (Player 0)")
                await websocket.send(json.dumps({
                    "type": "ROOM_CREATED",
                    "room_code": code,
                    "player_idx": 0,
                    "mode": mode
                }))
                if mode == "2v2":
                    await room.broadcast_lobby_update()
                continue

            # JOIN ROOM
            elif msg_type == "JOIN_ROOM":
                client_ver = parse_version(data.get("game_version", 0))
                if client_ver < MIN_REQUIRED_VERSION:
                    await websocket.send(json.dumps({
                        "type": "ERROR",
                        "code": "OUTDATED_VERSION",
                        "message": "Ваша версия игры устарела! Требуется актуальная версия игры для сетевой игры."
                    }))
                    continue
                code = str(data.get("room_code", "")).strip()
                if code not in rooms:
                    await websocket.send(json.dumps({
                        "type": "ERROR",
                        "message": f"Комната {code} не найдена. Проверьте код!"
                    }))
                    continue

                room = rooms[code]
                
                # 1v1 Room Join
                if room.mode == "1v1":
                    if room.p1 is not None:
                        await websocket.send(json.dumps({
                            "type": "ERROR",
                            "message": f"Комната {code} уже заполнена (2/2)!"
                        }))
                        continue

                    p1_deck = data.get("deck", [])
                    p1_name = str(data.get("player_name", data.get("name", "Игрок 2"))).strip() or "Игрок 2"
                    room.p1 = websocket
                    room.players[1] = websocket
                    room.p1_deck = p1_deck
                    room.player_decks[1] = p1_deck
                    room.p1_name = p1_name
                    room.player_names[1] = p1_name
                    connections[websocket] = (room, 1)
                    print(f"[ROOM] {p1_name} (Player 1) joined 1v1 room {code}! Starting match.")

                    await websocket.send(json.dumps({
                        "type": "ROOM_JOINED",
                        "room_code": code,
                        "player_idx": 1,
                        "mode": "1v1"
                    }))

                    # Broadcast match start to both
                    for p_i in [0, 1]:
                        await room.send_to_player(p_i, {
                            "type": "MATCH_START",
                            "player_idx": p_i,
                            "room_code": code,
                            "turns": room.turns,
                            "zone_ids": room.zone_ids,
                            "rand_seed": room.rand_seed,
                            "p0_deck": room.p0_deck,
                            "p1_deck": room.p1_deck,
                            "p0_name": room.p0_name,
                            "p1_name": room.p1_name
                        })
                    continue

                # 2v2 Room Join
                else:
                    free_slot = -1
                    for i in range(1, room.max_players):
                        if room.players[i] is None and not room.player_is_bot[i]:
                            free_slot = i
                            break
                    
                    if free_slot == -1:
                        await websocket.send(json.dumps({
                            "type": "ERROR",
                            "message": f"Комната 2v2 {code} уже полностью заполнена (4/4)!"
                        }))
                        continue

                    joiner_name = str(data.get("player_name", data.get("name", f"Игрок {free_slot + 1}"))).strip() or f"Игрок {free_slot + 1}"
                    joiner_deck = data.get("deck", [])
                    room.players[free_slot] = websocket
                    room.player_names[free_slot] = joiner_name
                    room.player_decks[free_slot] = joiner_deck
                    connections[websocket] = (room, free_slot)

                    print(f"[ROOM 2v2] {joiner_name} joined room {code} at Slot {free_slot} (Team {0 if free_slot < 2 else 1})")
                    await websocket.send(json.dumps({
                        "type": "ROOM_JOINED",
                        "room_code": code,
                        "player_idx": free_slot,
                        "mode": "2v2",
                        "team_idx": 0 if free_slot < 2 else 1
                    }))
                    await room.broadcast_lobby_update()
                    continue

            # In-room messages
            if websocket in connections:
                room, p_idx = connections[websocket]

                # --- 2v2 LOBBY ACTIONS ---
                if room.mode == "2v2" and not room.match_started:
                    if msg_type == "FILL_BOTS" and p_idx == 0:
                        # Host requested to fill empty slots with bots
                        bot_names = ["Бот-Союзник", "Бот-Соперник 1", "Бот-Соперник 2"]
                        for i in range(1, 4):
                            if room.players[i] is None and not room.player_is_bot[i]:
                                room.player_is_bot[i] = True
                                room.player_names[i] = bot_names[i - 1]
                                room.player_decks[i] = ["farmer", "archer", "scout", "mountain_guardian", "slime", "rage_potion"]
                                print(f"[ROOM 2v2] Filled slot {i} with {room.player_names[i]}")
                        await room.broadcast_lobby_update()
                        continue

                    elif msg_type == "START_2V2_MATCH" and p_idx == 0:
                        # Verify all 4 slots are occupied
                        all_occupied = all((room.players[i] is not None or room.player_is_bot[i]) for i in range(4))
                        if not all_occupied:
                            await websocket.send(json.dumps({
                                "type": "ERROR",
                                "message": "Не все 4 слота заполнены! Заполните ботами или дождитесь игроков."
                            }))
                            continue

                        room.match_started = True
                        print(f"[ROOM 2v2] Starting match in room {code}!")
                        
                        players_payload = []
                        for i in range(4):
                            players_payload.append({
                                "player_idx": i,
                                "team": 0 if i < 2 else 1,
                                "name": room.player_names[i],
                                "deck": room.player_decks[i],
                                "is_bot": room.player_is_bot[i]
                            })

                        for i in range(4):
                            if room.players[i] is not None:
                                await room.send_to_player(i, {
                                    "type": "MATCH_START_2V2",
                                    "player_idx": i,
                                    "team_idx": 0 if i < 2 else 1,
                                    "room_code": room.code,
                                    "turns": room.turns,
                                    "zone_ids": room.zone_ids,
                                    "rand_seed": room.rand_seed,
                                    "players": players_payload
                                })
                        continue

                # --- 1v1 MATCH LOGIC ---
                if room.mode == "1v1":
                    opp_idx = 1 if p_idx == 0 else 0

                    if msg_type == "PLAYER_INFO":
                        info_name = str(data.get("name", "")).strip() or ("Игрок 1" if p_idx == 0 else "Игрок 2")
                        if p_idx == 0:
                            room.p0_name = info_name
                        else:
                            room.p1_name = info_name
                        await room.send_to_player(opp_idx, {
                            "type": "PLAYER_INFO",
                            "name": info_name
                        })

                    elif msg_type == "CARD_STAGED":
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
                        await room.handle_submit_plays_1v1(p_idx, plays)
                    elif msg_type == "ADVANCE_TURN":
                        room.advance_ready[p_idx] = True
                        await room.send_to_player(opp_idx, {
                            "type": "OPPONENT_READY_NEXT"
                        })
                        if room.advance_ready[0] and room.advance_ready[1]:
                            next_turn = data.get("turn", room.current_turn + 1)
                            room.current_turn = next_turn
                            room.advance_ready = [False, False]
                            await room.broadcast({
                                "type": "NEXT_TURN",
                                "turn": next_turn
                            })

                # --- 2v2 MATCH LOGIC ---
                else:
                    team_idx = 0 if p_idx < 2 else 1
                    opp_team_idx = 1 if team_idx == 0 else 0
                    teammate_idx = (p_idx + 1) if p_idx % 2 == 0 else (p_idx - 1)
                    opp_indices = [2, 3] if team_idx == 0 else [0, 1]

                    if msg_type == "CARD_STAGED":
                        card_id = data.get("card_id", "")
                        zone_idx = data.get("zone_idx", 0)
                        # Teammate sees full card details (green glow)
                        await room.send_to_player(teammate_idx, {
                            "type": "TEAMMATE_CARD_STAGED",
                            "player_idx": p_idx,
                            "card_id": card_id,
                            "zone_idx": zone_idx
                        })
                        # Opponents see only that a card was placed (facedown)
                        for opp_i in opp_indices:
                            await room.send_to_player(opp_i, {
                                "type": "OPPONENT_CARD_STAGED",
                                "player_idx": p_idx,
                                "zone_idx": zone_idx
                            })

                    elif msg_type == "CARD_UNSTAGED":
                        zone_idx = data.get("zone_idx", 0)
                        await room.send_to_player(teammate_idx, {
                            "type": "TEAMMATE_CARD_UNSTAGED",
                            "player_idx": p_idx,
                            "zone_idx": zone_idx
                        })
                        for opp_i in opp_indices:
                            await room.send_to_player(opp_i, {
                                "type": "OPPONENT_CARD_UNSTAGED",
                                "player_idx": p_idx,
                                "zone_idx": zone_idx
                            })

                    elif msg_type == "PLAYER_READY":
                        room.player_ready_planning[p_idx] = True
                        await room.broadcast({
                            "type": "PLAYER_READY_STATUS_2V2",
                            "player_idx": p_idx,
                            "ready": True
                        })

                    elif msg_type == "SUBMIT_PLAYS":
                        plays = data.get("plays", [])
                        await room.handle_submit_plays_2v2(p_idx, plays)

                    elif msg_type == "ADVANCE_TURN":
                        room.advance_ready[p_idx] = True
                        # Bots are automatically advance ready
                        for i in range(4):
                            if room.player_is_bot[i]:
                                room.advance_ready[i] = True
                        
                        all_adv = all(room.advance_ready[i] for i in range(4))
                        if all_adv:
                            next_turn = data.get("turn", room.current_turn + 1)
                            room.current_turn = next_turn
                            room.advance_ready = [False] * 4
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
            del connections[websocket]
            # Notify remaining players
            await room.broadcast({
                "type": "PLAYER_LEFT",
                "player_idx": p_idx,
                "message": f"{room.player_names[p_idx]} вышел из игры."
            })
            if p_idx == 0:
                # If host leaves lobby/game, close room
                if room.code in rooms:
                    del rooms[room.code]
                    print(f"[ROOM] Room {room.code} destroyed by host disconnect.")

async def main():
    print("=" * 50)
    print("  LITTLE WARS - Cloud Room Relay Server (1v1 & 2v2)")
    print(f"  Listening on http://{HOST}:{PORT} & ws://{HOST}:{PORT}")
    print("=" * 50)
    async with websockets.serve(ws_handler, HOST, PORT, process_request=process_http_request):
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SERVER] Stopped by user.")
