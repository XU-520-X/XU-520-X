"""
ws_server.py — 交友平台后端服务（FastAPI + WebSocket + 数据库）
职责划分（与前端 交友.html 的数据模型一一对齐）：
  - REST : 注册 / 登录 / 用户列表 / 历史消息 / 会话列表
  - WS   : 消息实时推送 + 服务端持久化存储
数据库连接配置全部来自环境变量，代码里不硬编码：
  DB_TYPE = sqlite | mysql                 默认 sqlite
  DB_PATH = SQLite 数据文件路径             默认 ./data.db
  DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME   MySQL 参数
启动：python ws_server.py → http://127.0.0.1:8000
"""
import os
import re
import json
import time
import hmac
import base64
import secrets
import hashlib
import sqlite3
import threading
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Header, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ==================== 数据库接入（读环境变量） ====================
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).parent / "data.db"))
PORT = int(os.getenv("PORT", "8000"))
TOKEN_TTL = int(os.getenv("TOKEN_TTL", str(7 * 24 * 3600)))
MAX_MSG_LEN = 2000

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(Path(DB_PATH).with_name("avatars"))))
MAX_AVATAR_BYTES = 2 * 1024 * 1024
ALLOWED_IMAGE_TYPES = {"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp"}

MEDIA_DIR = Path(os.getenv("MEDIA_DIR", str(Path(DB_PATH).with_name("media"))))
MAX_IMAGE_BYTES = 5 * 1024 * 1024
MAX_VIDEO_BYTES = 50 * 1024 * 1024
ALLOWED_VIDEO_TYPES = {"video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov"}
ALLOWED_INTERESTS_MAX = 20


def load_secret_key() -> str:
    env = os.getenv("SECRET_KEY")
    if env:
        return env
    path = Path(DB_PATH).with_name("secret.key")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    key = secrets.token_hex(32)
    path.write_text(key, encoding="utf-8")
    return key


SECRET_KEY = load_secret_key()

DDL_SQLITE = [
    """CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL,
        nickname TEXT NOT NULL,
        gender TEXT,
        bio TEXT,
        avatar TEXT,
        age INTEGER,
        interests TEXT,
        cover TEXT)""",
    """CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_key TEXT NOT NULL,
        sender TEXT NOT NULL,
        recipient TEXT NOT NULL,
        content TEXT NOT NULL,
        ts INTEGER NOT NULL,
        read_at INTEGER,
        msg_type TEXT DEFAULT 'text')""",
    "CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_key, ts)",
    """CREATE TABLE IF NOT EXISTS friendships(
        username TEXT NOT NULL,
        friend TEXT NOT NULL,
        ts INTEGER NOT NULL,
        PRIMARY KEY(username, friend))""",
    """CREATE TABLE IF NOT EXISTS friend_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_user TEXT NOT NULL,
        to_user TEXT NOT NULL,
        status TEXT NOT NULL,
        ts INTEGER NOT NULL)""",
]

DDL_MYSQL = [
    """CREATE TABLE IF NOT EXISTS users(
        username VARCHAR(64) PRIMARY KEY,
        password VARCHAR(128) NOT NULL,
        nickname VARCHAR(64) NOT NULL,
        gender VARCHAR(8),
        bio VARCHAR(255),
        avatar TEXT,
        age INT,
        interests TEXT,
        cover VARCHAR(255))""",
    """CREATE TABLE IF NOT EXISTS messages(
        id INT AUTO_INCREMENT PRIMARY KEY,
        chat_key VARCHAR(160) NOT NULL,
        sender VARCHAR(64) NOT NULL,
        recipient VARCHAR(64) NOT NULL,
        content TEXT NOT NULL,
        ts BIGINT NOT NULL,
        read_at BIGINT,
        msg_type VARCHAR(8) DEFAULT 'text',
        INDEX idx_msg_chat(chat_key, ts))""",
    """CREATE TABLE IF NOT EXISTS friendships(
        username VARCHAR(64) NOT NULL,
        friend VARCHAR(64) NOT NULL,
        ts BIGINT NOT NULL,
        PRIMARY KEY(username, friend))""",
    """CREATE TABLE IF NOT EXISTS friend_requests(
        id INT AUTO_INCREMENT PRIMARY KEY,
        from_user VARCHAR(64) NOT NULL,
        to_user VARCHAR(64) NOT NULL,
        status VARCHAR(16) NOT NULL,
        ts BIGINT NOT NULL)""",
]


class DB:
    """极简数据库适配层：屏蔽 sqlite / mysql 的占位符差异"""

    def __init__(self):
        self.lock = threading.Lock()
        if DB_TYPE == "mysql":
            import pymysql
            self.conn = pymysql.connect(
                host=os.getenv("DB_HOST", "127.0.0.1"),
                port=int(os.getenv("DB_PORT", "3306")),
                user=os.getenv("DB_USER", "root"),
                password=os.getenv("DB_PASS", ""),
                database=os.getenv("DB_NAME", "dating"),
                charset="utf8mb4",
                autocommit=False,
                cursorclass=pymysql.cursors.DictCursor,
            )
        else:
            Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    def execute(self, sql: str, args: tuple = ()):
        if DB_TYPE != "sqlite":
            sql = sql.replace("?", "%s")
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(sql, args)
            self.conn.commit()
            return cur

    def query(self, sql: str, args: tuple = ()) -> list:
        cur = self.execute(sql, args)
        rows = cur.fetchall()
        if rows and isinstance(rows[0], dict):
            return [dict(r) for r in rows]
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in rows]


# 导入阶段不创建数据库连接，避免构建时触发 MySQL 连接报错
db = None


def init_db():
    global db
    for ddl in (DDL_MYSQL if DB_TYPE == "mysql" else DDL_SQLITE):
        db.execute(ddl)
    ensure_schema()
    if not db.query("SELECT username FROM users LIMIT 1"):
        seed = [
            ("xiaoyu", "小鱼", "女", "喜欢旅行和摄影", "🐟", 24, ["旅行", "摄影", "咖啡"]),
            ("chenchen", "晨晨", "男", "程序员，爱健身", "😎", 28, ["健身", "编程", "篮球"]),
            ("linlin", "林林", "女", "咖啡重度爱好者", "☕", 26, ["咖啡", "阅读", "电影"]),
            ("ahao", "阿豪", "男", "音乐发烧友", "🎧", 30, ["音乐", "吉他", "旅行"]),
        ]
        for username, nickname, gender, bio, avatar, age, interests in seed:
            db.execute(
                "INSERT INTO users(username, password, nickname, gender, bio, avatar, age, interests) VALUES(?,?,?,?,?,?,?,?)",
                (username, hash_pwd("123456"), nickname, gender, bio, avatar, age, dump_interests(interests)),
            )
        print(f"[init] 已写入 4 个演示用户（密码均为 123456），DB_TYPE={DB_TYPE}")
    backfill_friendships()
    seed_demo_friendships()


DEMO_USERNAMES = ["xiaoyu", "chenchen", "linlin", "ahao"]


def _has_column(table: str, name: str) -> bool:
    if DB_TYPE == "sqlite":
        return name in [r["name"] for r in db.query("PRAGMA table_info({})".format(table))]
    rows = db.query(
        "SELECT COUNT(*) AS c FROM information_schema.columns "
        "WHERE table_schema=DATABASE() AND table_name=? AND column_name=?", (table, name))
    return bool(rows and rows[0]["c"])


def _ensure_column(table: str, name: str, sqlite_type: str, mysql_type: str):
    if _has_column(table, name):
        return
    t = sqlite_type if DB_TYPE == "sqlite" else mysql_type
    try:
        db.execute("ALTER TABLE {} ADD COLUMN {} {}".format(table, name, t))
    except Exception:
        pass


def ensure_schema():
    _ensure_column("messages", "read_at", "INTEGER", "BIGINT")
    _ensure_column("messages", "msg_type", "TEXT DEFAULT 'text'", "VARCHAR(8) DEFAULT 'text'")
    _ensure_column("users", "age", "INTEGER", "INT")
    _ensure_column("users", "interests", "TEXT", "TEXT")
    _ensure_column("users", "cover", "TEXT", "VARCHAR(255)")


def backfill_friendships():
    for r in db.query("SELECT DISTINCT sender, recipient FROM messages"):
        add_friendship(r["sender"], r["recipient"])


def seed_demo_friendships():
    for i, a in enumerate(DEMO_USERNAMES):
        for b in DEMO_USERNAMES[i + 1:]:
            if user_exists(a) and user_exists(b):
                add_friendship(a, b)


PBKDF2_ITERATIONS = 120_000
USERNAME_RE = re.compile(r"^[A-Za-z0-9_\u4e00-\u9fa5]{1,32}$")


def hash_pwd(pwd: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return "pbkdf2${}${}${}".format(PBKDF2_ITERATIONS, salt.hex(), dk.hex())


def verify_pwd(pwd: str, stored: str) -> bool:
    if not stored:
        return False
    if stored.startswith("pbkdf2$"):
        try:
            _, iters, salt_hex, hash_hex = stored.split("$")
            dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), bytes.fromhex(salt_hex), int(iters))
        except ValueError:
            return False
        return hmac.compare_digest(dk.hex(), hash_hex)
    return hmac.compare_digest(hashlib.sha256(pwd.encode("utf-8")).hexdigest(), stored)


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def issue_token(username: str) -> dict:
    exp = int(time.time()) + TOKEN_TTL
    payload = _b64e(json.dumps({"u": username, "exp": exp}, separators=(",", ":")).encode("utf-8"))
    sig = _b64e(hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest())
    return {"token": "{}.{}".format(payload, sig), "expiresAt": exp * 1000}


def verify_token(token: Optional[str]) -> Optional[str]:
    if not token or token.count(".") != 1:
        return None
    payload, sig = token.split(".")
    expect = _b64e(hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest())
    if not hmac.compare_digest(expect, sig):
        return None
    try:
        data = json.loads(_b64d(payload).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if int(data.get("exp", 0)) < int(time.time()):
        return None
    return data.get("u")


def user_exists(username: str) -> bool:
    return bool(db.query("SELECT username FROM users WHERE username=?", (username,)))


def require_auth(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "未登录或登录凭证缺失")
    username = verify_token(authorization[7:].strip())
    if not username:
        raise HTTPException(401, "登录状态已失效，请重新登录")
    if not user_exists(username):
        raise HTTPException(401, "账号不存在或已被删除")
    return username


def optional_auth(authorization: Optional[str] = Header(None)) -> Optional[str]:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    username = verify_token(authorization[7:].strip())
    if username and user_exists(username):
        return username
    return None


def get_chat_key(u1: str, u2: str) -> str:
    return "_".join(sorted([u1, u2]))


def are_friends(a: str, b: str) -> bool:
    return bool(db.query("SELECT 1 AS x FROM friendships WHERE username=? AND friend=?", (a, b)))


def add_friendship(a: str, b: str):
    if not a or not b or a == b:
        return
    ts = int(time.time() * 1000)
    for u, f in ((a, b), (b, a)):
        if not are_friends(u, f):
            db.execute(
                "INSERT INTO friendships(username, friend, ts) VALUES(?,?,?)",
                (u, f, ts),
            )


def pending_request(from_user: str, to_user: str) -> bool:
    return bool(db.query(
        "SELECT 1 AS x FROM friend_requests WHERE from_user=? AND to_user=? AND status='pending'",
        (from_user, to_user),
    ))


def mark_messages_read(me: str, peer: str) -> int:
    ts = int(time.time() * 1000)
    cur = db.execute(
        "UPDATE messages SET read_at=? WHERE chat_key=? AND recipient=? AND sender=? AND read_at IS NULL",
        (ts, get_chat_key(me, peer), me, peer),
    )
    return cur.rowcount or 0


# ==================== 应用与模型 ====================
app = FastAPI(title="交友平台 WebSocket 后端")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# 服务启动后再连接数据库、建表；构建阶段不会执行
@app.on_event("startup")
def startup_event():
    global db
    db = DB()
    init_db()


@app.on_event("shutdown")
def shutdown_event():
    global db
    if db is not None and hasattr(db, "conn"):
        try:
            db.conn.close()
        except Exception:
            pass
    db = None


# 静态资源目录（启动后目录已就绪）
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/uploads/avatars", StaticFiles(directory=str(UPLOAD_DIR)), name="avatars")
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/uploads/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")


class RegisterIn(BaseModel):
    username: str = Field(min_length=1, max_length=32)
    password: str = Field(min_length=6, max_length=64)
    nickname: str = Field(min_length=1, max_length=32)
    gender: str = ""
    bio: str = ""
    avatar: str = ""
    age: int = 0
    interests: list = Field(default_factory=list)


class LoginIn(BaseModel):
    username: str
    password: str


class SendIn(BaseModel):
    sender: str = ""
    recipient: str
    content: str = Field(min_length=1, max_length=MAX_MSG_LEN)
    kind: str = "text"


class FriendReqIn(BaseModel):
    to: str


class FriendRespondIn(BaseModel):
    target: str
    accept: bool


class AvatarUrlIn(BaseModel):
    avatar: str = Field(max_length=256)


class ProfileIn(BaseModel):
    nickname: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[int] = None
    bio: Optional[str] = None
    interests: Optional[list] = None
    avatar: Optional[str] = None
    cover: Optional[str] = None


USER_PUBLIC_COLS = "username, nickname, gender, bio, avatar, age, interests, cover"


def parse_interests(raw) -> list:
    if not raw:
        return []
    try:
        v = json.loads(raw)
        if isinstance(v, list):
            return [str(x) for x in v if str(x).strip()]
    except (ValueError, TypeError):
        pass
    return [s.strip() for s in str(raw).split(",") if s.strip()]


def dump_interests(items) -> str:
    cleaned = []
    for x in (items or []):
        s = str(x).strip()[:24]
        if s and s not in cleaned:
            cleaned.append(s)
    return json.dumps(cleaned[:ALLOWED_INTERESTS_MAX], ensure_ascii=False)


def public_user(row: dict) -> dict:
    out = {k: row[k] for k in ("username", "nickname", "gender", "bio", "avatar") if k in row}
    age = row.get("age")
    out["age"] = int(age) if age else None
    out["interests"] = parse_interests(row.get("interests"))
    out["cover"] = row.get("cover") or ""
    return out


def row_to_msg(row: dict) -> dict:
    return {
        "from": row["sender"],
        "to": row["recipient"],
        "content": row["content"],
        "time": int(row["ts"]),
        "read": bool(row.get("read_at")),
        "kind": row.get("msg_type") or "text",
    }


def msg_preview(content: str, kind: str) -> str:
    if kind == "image":
        return "[图片]"
    if kind == "video":
        return "[视频]"
    return content


# ==================== REST 接口 ====================
@app.get("/")
def index():
    page = Path(__file__).parent / "交友.html"
    if page.exists():
        return FileResponse(page, media_type="text/html; charset=utf-8")
    return {"service": "dating-ws-backend", "docs": "/docs"}


@app.get("/api/health")
def health():
    return {"ok": True, "db": DB_TYPE, "online": len(connected)}


@app.get("/api/me")
def me_info(me: str = Depends(require_auth)):
    rows = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (me,))
    if not rows:
        raise HTTPException(404, "账号不存在")
    return public_user(rows[0])


@app.get("/api/users")
def list_users(me: str = Depends(require_auth)):
    rows = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users")
    return [public_user(r) for r in rows if r["username"] != me]


@app.post("/api/register")
def register(body: RegisterIn):
    username = body.username.strip()
    if not USERNAME_RE.match(username):
        raise HTTPException(400, "账号只能用字母、数字、下划线或中文，长度 1-32")
    if db.query("SELECT username FROM users WHERE username=?", (username,)):
        raise HTTPException(409, "该账号已被注册")
    avatar = body.avatar.strip()
    if not _is_safe_avatar(avatar):
        raise HTTPException(400, "头像只能是 emoji 或上传图片")
    if body.age < 0 or body.age > 120:
        raise HTTPException(400, "年龄需在 0-120 之间")
    db.execute(
        "INSERT INTO users(username, password, nickname, gender, bio, avatar, age, interests) VALUES(?,?,?,?,?,?,?,?)",
        (username, hash_pwd(body.password), body.nickname.strip(), body.gender, body.bio, avatar,
         body.age, dump_interests(body.interests)),
    )
    return {"ok": True}


@app.post("/api/login")
def login(body: LoginIn):
    rows = db.query("SELECT * FROM users WHERE username=?", (body.username,))
    if not rows or not verify_pwd(body.password, rows[0]["password"]):
        raise HTTPException(401, "账号或密码错误")
    user = rows[0]
    if not user["password"].startswith("pbkdf2$"):
        db.execute("UPDATE users SET password=? WHERE username=?", (hash_pwd(body.password), user["username"]))
    result = public_user(user)
    result.update(issue_token(user["username"]))
    return result


@app.get("/api/messages")
def get_messages(target: str, me: str = Depends(require_auth)):
    rows = db.query("SELECT sender, recipient, content, ts, read_at, msg_type FROM messages WHERE chat_key=? ORDER BY ts ASC",
                    (get_chat_key(me, target),))
    return [row_to_msg(r) for r in rows]


@app.post("/api/messages")
def send_message(body: SendIn, me: str = Depends(require_auth)):
    if body.recipient == me:
        raise HTTPException(400, "不能给自己发消息")
    if not db.query("SELECT username FROM users WHERE username=?", (body.recipient,)):
        raise HTTPException(404, "接收人不存在")
    if not are_friends(me, body.recipient):
        raise HTTPException(403, "你们还不是好友，请先添加好友")
    kind = body.kind if body.kind in ("text", "image", "video") else "text"
    if kind != "text" and not body.content.startswith("/uploads/media/"):
        raise HTTPException(400, "非法的媒体地址")
    record = save_message(me, body.recipient, body.content, kind)
    return {"ok": True, "message": record}


@app.get("/api/chats")
def list_chats(me: str = Depends(require_auth)):
    user = me
    mine = db.query("SELECT chat_key FROM messages WHERE sender=? OR recipient=? GROUP BY chat_key",
                    (user, user))
    sessions = []
    for item in mine:
        rows = db.query(
            "SELECT sender, recipient, content, ts, msg_type FROM messages WHERE chat_key=? ORDER BY ts DESC LIMIT 1",
            (item["chat_key"],))
        if not rows:
            continue
        last = rows[0]
        other_name = last["recipient"] if last["sender"] == user else last["sender"]
        others = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?",
                          (other_name,))
        if others:
            sessions.append({"user": public_user(others[0]),
                             "lastMsg": msg_preview(last["content"], last.get("msg_type") or "text"),
                             "time": int(last["ts"])})
    sessions.sort(key=lambda s: s["time"], reverse=True)
    return sessions


# ==================== 好友申请 / 好友关系 ====================
@app.get("/api/friends")
async def list_friends(me: str = Depends(require_auth)):
    out = []
    for r in db.query("SELECT friend FROM friendships WHERE username=?", (me,)):
        u = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (r["friend"],))
        if u:
            out.append(public_user(u[0]))
    return out


@app.get("/api/friends/requests")
async def friend_requests(me: str = Depends(require_auth)):
    incoming = []
    for r in db.query(
            "SELECT from_user AS other FROM friend_requests WHERE to_user=? AND status='pending'", (me,)):
        u = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (r["other"],))
        if u:
            incoming.append(public_user(u[0]))
    outgoing = [r["other"] for r in db.query(
        "SELECT to_user AS other FROM friend_requests WHERE from_user=? AND status='pending'", (me,))]
    return {"incoming": incoming, "outgoing": outgoing}


@app.post("/api/friends/request")
async def send_friend_request(body: FriendReqIn, me: str = Depends(require_auth)):
    to = body.to.strip()
    if to == me:
        raise HTTPException(400, "不能加自己为好友")
    target = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (to,))
    if not target:
        raise HTTPException(404, "用户不存在")
    if are_friends(me, to):
        raise HTTPException(409, "你们已经是好友了")
    if not pending_request(me, to):
        db.execute(
            "INSERT INTO friend_requests(from_user, to_user, status, ts) VALUES(?,?,?,?)",
            (me, to, "pending", int(time.time() * 1000)),
        )
    await push(to, {"type": "friend_request", "from": me, "user": public_user(target[0])})
    return {"ok": True}


@app.post("/api/friends/respond")
async def respond_friend_request(body: FriendRespondIn, me: str = Depends(require_auth)):
    requester = body.target.strip()
    row = db.query(
        "SELECT id FROM friend_requests WHERE from_user=? AND to_user=? AND status='pending'",
        (requester, me))
    if not row:
        raise HTTPException(404, "没有待处理的好友申请")
    db.execute("UPDATE friend_requests SET status=? WHERE id=?",
               ("accepted" if body.accept else "rejected", row[0]["id"]))
    if not body.accept:
        return {"ok": True, "accepted": False}
    add_friendship(requester, me)
    me_rows = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (me,))
    await push(requester, {"type": "friend_accepted", "from": me,
                           "user": public_user(me_rows[0]) if me_rows else {"username": me}})
    other = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (requester,))
    return {"ok": True, "accepted": True, "friend": public_user(other[0]) if other else {"username": requester}}


# ==================== 头像：图片文件存储 ====================
def _is_safe_avatar(val: str) -> bool:
    if val == "":
        return True
    if val.startswith("/uploads/avatars/") and "<" not in val:
        return True
    if len(val) <= 8 and "<" not in val:
        return True
    return False


def _delete_old_avatar(avatar):
    if avatar and avatar.startswith("/uploads/avatars/"):
        p = UPLOAD_DIR / Path(avatar).name
        try:
            if p.exists():
                p.unlink()
        except OSError:
            pass


@app.post("/api/avatar")
async def upload_avatar(file: UploadFile = File(...), me: Optional[str] = Depends(optional_auth)):
    ext = ALLOWED_IMAGE_TYPES.get((file.content_type or "").lower())
    if not ext:
        raise HTTPException(400, "仅支持 jpg/png/gif/webp 图片")
    raw = await file.read()
    if len(raw) > MAX_AVATAR_BYTES:
        raise HTTPException(413, "图片过大，请选择 2MB 以内的图片")
    name = "{}_{}{}".format(me or "tmp", secrets.token_hex(8), ext)
    (UPLOAD_DIR / name).write_bytes(raw)
    url = "/uploads/avatars/" + name
    if me:
        old = db.query("SELECT avatar FROM users WHERE username=?", (me,))
        _delete_old_avatar(old[0]["avatar"] if old else None)
        db.execute("UPDATE users SET avatar=? WHERE username=?", (url, me))
    return {"url": url, "ok": bool(me)}


@app.post("/api/me/avatar")
async def set_my_avatar(body: AvatarUrlIn, me: str = Depends(require_auth)):
    val = body.avatar.strip()
    if not _is_safe_avatar(val):
        raise HTTPException(400, "头像只能是 emoji 或站内上传图片 URL")
    old = db.query("SELECT avatar FROM users WHERE username=?", (me,))
    _delete_old_avatar(old[0]["avatar"] if old else None)
    db.execute("UPDATE users SET avatar=? WHERE username=?", (val, me))
    return {"ok": True, "avatar": val}


# ==================== 个人资料编辑 / 媒体上传 ====================
def _is_safe_image(val) -> bool:
    if val is None or val == "":
        return True
    return isinstance(val, str) and val.startswith("/uploads/") and "<" not in val


def _delete_managed_file(url):
    if not url:
        return
    if url.startswith("/uploads/avatars/"):
        p = UPLOAD_DIR / Path(url).name
    elif url.startswith("/uploads/media/"):
        p = MEDIA_DIR / Path(url).name
    else:
        return
    try:
        if p.exists():
            p.unlink()
    except OSError:
        pass


@app.put("/api/me")
async def update_me(body: ProfileIn, me: str = Depends(require_auth)):
    row = db.query("SELECT {} FROM users WHERE username=?".format(USER_PUBLIC_COLS), (me,))
    if not row:
        raise HTTPException(404, "账号不存在")
    cur = row[0]
    sets, args = [], []
    if body.nickname is not None:
        nick = body.nickname.strip()
        if not (1 <= len(nick) <= 32):
            raise HTTPException(400, "昵称长度需 1-32")
        sets.append("nickname=?"); args.append(nick)
    if body.gender is not None:
        sets.append("gender=?"); args.append(body.gender.strip()[:8])
    if body.age is not None:
        if body.age < 0 or body.age > 120:
            raise HTTPException(400, "年龄需在 0-120 之间")
        sets.append("age=?"); args.append(int(body.age))
    if body.bio is not None:
        sets.append("bio=?"); args.append(body.bio.strip()[:255])
    if body.interests is not None:
        sets.append("interests=?"); args.append(dump_interests(body.interests))
    if body.avatar is not None:
        av = body.avatar.strip()
        if not _is_safe_avatar(av):
            raise HTTPException(400, "头像只能是 emoji 或站内上传图片 URL")
        if av != (cur.get("avatar") or ""):
            _delete_managed_file(cur.get("avatar"))
        sets.append("avatar=?"); args.append(av)
    if body.cover is not None:
        cover = body.cover.strip()
        if not _is_safe_image(cover):
            raise HTTPException(400, "封面只能使用站内上传图片")
        if cover != (cur.get("cover") or ""):
            _delete_managed_file(cur.get("cover"))
        sets.append("cover=?"); args.append(cover)
    if sets:
        args.append(me)
        db.execute("UPDATE users SET {} WHERE username=?".format(",".join(sets)), tuple(args))
    rows = db.query("SELECT {} FROM users WHERE username=?".format(USER_PUBLIC_COLS), (me,))
    return public_user(rows[0])


@app.post("/api/media")
async def upload_media(file: UploadFile = File(...), me: str = Depends(require_auth)):
    ctype = (file.content_type or "").lower()
    ext = ALLOWED_IMAGE_TYPES.get(ctype)
    kind, limit = "image", MAX_IMAGE_BYTES
    if not ext:
        ext = ALLOWED_VIDEO_TYPES.get(ctype)
        kind, limit = "video", MAX_VIDEO_BYTES
    if not ext:
        raise HTTPException(400, "仅支持 jpg/png/gif/webp 图片或 mp4/webm/mov 视频")
    raw = await file.read()
    if len(raw) > limit:
        raise HTTPException(413, "文件过大：图片上限 5MB，视频上限 50MB")
    name = "{}_{}{}".format(me, secrets.token_hex(8), ext)
    (MEDIA_DIR / name).write_bytes(raw)
    return {"url": "/uploads/media/" + name, "kind": kind}


@app.get("/api/profile")
def get_profile(username: str, me: str = Depends(require_auth)):
    rows = db.query("SELECT {} FROM users WHERE username=?".format(USER_PUBLIC_COLS), (username,))
    if not rows:
        raise HTTPException(404, "用户不存在")
    return public_user(rows[0])


# ==================== WebSocket：在线表 + 实时推送 ====================
connected = {}


def save_message(sender: str, recipient: str, content: str, kind: str = "text") -> dict:
    kind = kind if kind in ("text", "image", "video") else "text"
    ts = int(time.time() * 1000)
    db.execute(
        "INSERT INTO messages(chat_key, sender, recipient, content, ts, msg_type) VALUES(?,?,?,?,?,?)",
        (get_chat_key(sender, recipient), sender, recipient, content, ts, kind),
    )
    return {"from": sender, "to": recipient, "content": content, "time": ts, "read": False, "kind": kind}


async def push(username: str, payload: dict) -> bool:
    ws = connected.get(username)
    if not ws:
        return False
    await ws.send_text(json.dumps(payload, ensure_ascii=False))
    return True


@app.websocket("/ws/{username}")
async def chat_ws(websocket: WebSocket, username: str):
    if verify_token(websocket.query_params.get("token")) != username:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    connected[username] = websocket
    peers = [u for u in connected if u != username]
    await websocket.send_text(json.dumps({"type": "presence_sync", "users": peers}, ensure_ascii=False))
    await broadcast_presence(username, True)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"type": "error", "msg": "非法 JSON"}))
                continue

            mtype = data.get("type")
            if mtype == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "time": int(time.time() * 1000)}))
            elif mtype == "typing":
                to = data.get("to")
                if to:
                    await push(to, {"type": "typing", "from": username})
            elif mtype == "read":
                to = data.get("to")
                if to and to != username and user_exists(to) and mark_messages_read(username, to):
                    await push(to, {"type": "read", "by": username})
            elif mtype == "message":
                content = (data.get("content") or "").strip()
                to = data.get("to")
                kind = data.get("kind") or "text"
                if kind not in ("text", "image", "video"):
                    kind = "text"
                if kind == "text":
                    if not content or len(content) > MAX_MSG_LEN:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "内容为空或超过 {} 字".format(MAX_MSG_LEN)}, ensure_ascii=False))
                        continue
                elif not content.startswith("/uploads/media/"):
                    await websocket.send_text(json.dumps(
                        {"type": "error", "msg": "非法的媒体地址"}, ensure_ascii=False))
                    continue
                if not to or to == username or not user_exists(to):
                    await websocket.send_text(json.dumps(
                        {"type": "error", "msg": "接收人无效"}, ensure_ascii=False))
                    continue
                if not are_friends(username, to):
                    await websocket.send_text(json.dumps(
                        {"type": "error", "msg": "你们还不是好友，请先添加好友"}, ensure_ascii=False))
                    continue
                record = save_message(username, to, content, kind)
                await websocket.send_text(json.dumps({"type": "ack", "data": record}, ensure_ascii=False))
                await push(to, {"type": "message", "data": record})
    except WebSocketDisconnect:
        pass
    finally:
        if connected.get(username) is websocket:
            connected.pop(username, None)
            await broadcast_presence(username, False)


async def broadcast_presence(username: str, online: bool):
    payload = json.dumps({"type": "presence", "username": username, "online": online}, ensure_ascii=False)
    for name, ws in list(connected.items()):
        if name != username:
            try:
                await ws.send_text(payload)
            except Exception:
                connected.pop(name, None)


# ==================== 入口 ====================
# 建库/建表由 startup 事件触发，构建阶段不执行
if __name__ == "__main__":
    print(f"WebSocket 后端启动： http://127.0.0.1:{PORT}   （数据库：{DB_TYPE}）")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
