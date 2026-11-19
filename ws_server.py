"""
ws_server.py — 交友平台后端服务（FastAPI + WebSocket + 数据库）

职责划分（与前端 交友.html 的数据模型一一对齐）：
  - REST : 注册 / 登录 / 用户列表 / 历史消息 / 会话列表
  - WS   : 消息实时推送 + 服务端持久化存储

数据库连接配置全部来自环境变量（由 devfile.yaml 注入），代码里不硬编码：
  DB_TYPE = sqlite | mysql                 默认 sqlite
  DB_PATH = SQLite 数据文件路径             默认 ./data.db（必须落在持久卷上，否则容器重建数据丢失）
  DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME   MySQL 参数
          （devfile 里组件名即 hostname，所以 DB_HOST 直接写 mysql）

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
from typing import Optional  # 兼容 Python 3.8：不能用 str | None / list[dict] 这类 3.9+ 写法

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
TOKEN_TTL = int(os.getenv("TOKEN_TTL", str(7 * 24 * 3600)))  # 登录态有效期（秒）
MAX_MSG_LEN = 2000

# 头像图片改走文件存储：图片落盘、数据库只存 URL。
# 目录默认跟着 DB_PATH 走（devfile 里只有 DB 目录挂了持久卷，容器重建图片才不丢）。
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(Path(DB_PATH).with_name("avatars"))))
MAX_AVATAR_BYTES = 2 * 1024 * 1024
ALLOWED_IMAGE_TYPES = {"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp"}

# 聊天图片/短视频、封面图统一走文件存储（与头像分开目录，方便清理与鉴权）
MEDIA_DIR = Path(os.getenv("MEDIA_DIR", str(Path(DB_PATH).with_name("media"))))
MAX_IMAGE_BYTES = 5 * 1024 * 1024
MAX_VIDEO_BYTES = 50 * 1024 * 1024
ALLOWED_VIDEO_TYPES = {"video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov"}
ALLOWED_INTERESTS_MAX = 20


def load_secret_key() -> str:
    """签名密钥：优先环境变量（多副本部署必须显式指定），否则落盘复用。

    文件放在 DB 同目录而不是代码目录：devfile 里只有 DB 目录挂了持久卷，
    密钥若随容器重建而变，所有用户的登录态会被集体踢下线。
    """
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
    """极简数据库适配层：屏蔽 sqlite / mysql 的占位符差异，后续换库不动业务代码"""

    def __init__(self):
        self.lock = threading.Lock()
        if DB_TYPE == "mysql":
            import pymysql  # 仅 DB_TYPE=mysql 时需要
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


db = DB()


def init_db():
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
    # 引入好友门槛后：历史会话回填成好友 + 演示用户预置互加，避免老对话发不出消息
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
    """幂等补列（列已存在则跳过），专用于旧库升级"""
    if _has_column(table, name):
        return
    t = sqlite_type if DB_TYPE == "sqlite" else mysql_type
    try:
        db.execute("ALTER TABLE {} ADD COLUMN {} {}".format(table, name, t))
    except Exception:
        pass  # 并发/重复列等异常一律忽略，列已在下一轮存在


def ensure_schema():
    """旧库升级：幂等补齐 messages.read_at/msg_type 与 users.age/interests/cover"""
    _ensure_column("messages", "read_at", "INTEGER", "BIGINT")
    _ensure_column("messages", "msg_type", "TEXT DEFAULT 'text'", "VARCHAR(8) DEFAULT 'text'")
    _ensure_column("users", "age", "INTEGER", "INT")
    _ensure_column("users", "interests", "TEXT", "TEXT")
    _ensure_column("users", "cover", "TEXT", "VARCHAR(255)")


def backfill_friendships():
    """把已有过消息往来的双方补成好友（幂等）"""
    for r in db.query("SELECT DISTINCT sender, recipient FROM messages"):
        add_friendship(r["sender"], r["recipient"])


def seed_demo_friendships():
    """演示用户两两互加好友，保证开箱即用"""
    for i, a in enumerate(DEMO_USERNAMES):
        for b in DEMO_USERNAMES[i + 1:]:
            if user_exists(a) and user_exists(b):
                add_friendship(a, b)


PBKDF2_ITERATIONS = 120_000
USERNAME_RE = re.compile(r"^[A-Za-z0-9_\u4e00-\u9fa5]{1,32}$")


def hash_pwd(pwd: str) -> str:
    """pbkdf2_hmac + 每用户随机盐（纯标准库，不引入额外依赖）"""
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
    # 兼容旧数据：之前 seed 进去的是无盐 sha256，登录成功后会被透明升级成 pbkdf2
    return hmac.compare_digest(hashlib.sha256(pwd.encode("utf-8")).hexdigest(), stored)


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def issue_token(username: str) -> dict:
    """HMAC 签名的白拿 token：payload.signature，无需引入 PyJWT"""
    exp = int(time.time()) + TOKEN_TTL
    payload = _b64e(json.dumps({"u": username, "exp": exp}, separators=(",", ":")).encode("utf-8"))
    sig = _b64e(hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest())
    return {"token": "{}.{}".format(payload, sig), "expiresAt": exp * 1000}


def verify_token(token: Optional[str]) -> Optional[str]:
    """校签 + 校过期，通过则返回 username，否则 None"""
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
    """REST 统一鉴权依赖：从 Authorization: Bearer <token> 取出当前登录用户"""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "未登录或登录凭证缺失")
    username = verify_token(authorization[7:].strip())
    if not username:
        raise HTTPException(401, "登录状态已失效，请重新登录")
    if not user_exists(username):
        raise HTTPException(401, "账号不存在或已被删除")
    return username


def optional_auth(authorization: Optional[str] = Header(None)) -> Optional[str]:
    """与 require_auth 同样的取 token 逻辑，但不报错：无/无效 token 返回 None。
    用于注册阶段（尚未登录）的图片上传——只落盘拿 URL，不动数据库。"""
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    username = verify_token(authorization[7:].strip())
    if username and user_exists(username):
        return username
    return None


def get_chat_key(u1: str, u2: str) -> str:
    return "_".join(sorted([u1, u2]))


def are_friends(a: str, b: str) -> bool:
    """friendships 双向存两行，查一行即可判定“是否好友”"""
    return bool(db.query("SELECT 1 AS x FROM friendships WHERE username=? AND friend=?", (a, b)))


def add_friendship(a: str, b: str):
    """幂等写入双向关系（a==b 或已存在则跳过）"""
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
    """把 peer 发给我、我尚未读的消息标记为已读，返回本次新标记条数"""
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
    # 前端可能以 file:// 或其他端口打开，开发环境放开；生产请收紧为具体域名
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 头像图片静态服务：上传落盘到 UPLOAD_DIR（持久卷），对外路径 /uploads/avatars/<文件>
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/uploads/avatars", StaticFiles(directory=str(UPLOAD_DIR)), name="avatars")
# 聊天图片/短视频、封面图静态服务：落盘到 MEDIA_DIR，对外路径 /uploads/media/<文件>
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
    # sender 字段已废弃：发送者身份只从 token 推导，客户端传什么都不信
    sender: str = ""
    recipient: str
    content: str = Field(min_length=1, max_length=MAX_MSG_LEN)
    kind: str = "text"  # text | image | video


class FriendReqIn(BaseModel):
    to: str


class FriendRespondIn(BaseModel):
    target: str   # 待处理的好友申请发起方用户名
    accept: bool


class AvatarUrlIn(BaseModel):
    # 登录后可直接提交的头像：emoji 或 /uploads/avatars/xxx 这类 URL（不再收 base64）
    avatar: str = Field(max_length=256)


class ProfileIn(BaseModel):
    # 个人中心“全部可编辑”：传哪个字段改哪个，None 表示不动
    nickname: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[int] = None
    bio: Optional[str] = None
    interests: Optional[list] = None
    avatar: Optional[str] = None
    cover: Optional[str] = None


# 对外公开的用户列（所有查 user 再交给 public_user 的地方统一用）
USER_PUBLIC_COLS = "username, nickname, gender, bio, avatar, age, interests, cover"


def parse_interests(raw) -> list:
    """interests 列存 JSON 字符串；兼容旧数据按逗号分隔，都解成字符串列表"""
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
    """列表去重、限长后序列化为 JSON 字符串入库"""
    cleaned = []
    for x in (items or []):
        s = str(x).strip()[:24]
        if s and s not in cleaned:
            cleaned.append(s)
    return json.dumps(cleaned[:ALLOWED_INTERESTS_MAX], ensure_ascii=False)


def public_user(row: dict) -> dict:
    """去掉 password，避免密码散列外泄；补充 age/interests/cover 并做格式归一"""
    out = {k: row[k] for k in ("username", "nickname", "gender", "bio", "avatar") if k in row}
    age = row.get("age")
    out["age"] = int(age) if age else None
    out["interests"] = parse_interests(row.get("interests"))
    out["cover"] = row.get("cover") or ""
    return out


def row_to_msg(row: dict) -> dict:
    """转成前端使用的字段名：from / to / content / time(毫秒) / read / kind"""
    return {
        "from": row["sender"],
        "to": row["recipient"],
        "content": row["content"],
        "time": int(row["ts"]),
        "read": bool(row.get("read_at")),
        "kind": row.get("msg_type") or "text",
    }


def msg_preview(content: str, kind: str) -> str:
    """会话列表预览：媒体消息不显示 URL，显占位文案"""
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
    """当前登录者资料：刷新页面后恢复会话用（不能用 /api/users 查自己，那个接口排除了自己）"""
    rows = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (me,))
    if not rows:
        raise HTTPException(404, "账号不存在")
    return public_user(rows[0])


@app.get("/api/users")
def list_users(me: str = Depends(require_auth)):
    """广场列表：排除自己由服务端根据 token 决定，不再依赖客户端传 exclude"""
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
        raise HTTPException(400, "头像只能是 emoji 或上传图片（不支持 base64）")
    if body.age < 0 or body.age > 120:
        raise HTTPException(400, "年龄需在 0-120 之间（0 表示未填写）")
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
        # 统一返回同一个错，不区分“账号不存在/密码错”，避免账号枚举
        raise HTTPException(401, "账号或密码错误")
    user = rows[0]
    if not user["password"].startswith("pbkdf2$"):
        # 旧的无盐 sha256 散列，登录成功这一次顺手升级
        db.execute("UPDATE users SET password=? WHERE username=?", (hash_pwd(body.password), user["username"]))
    result = public_user(user)
    result.update(issue_token(user["username"]))
    return result


@app.get("/api/messages")
def get_messages(target: str, me: str = Depends(require_auth)):
    """历史消息：只允许查自己参与的会话（chat_key 包含 me，天然隔离别人的聊天记录）"""
    rows = db.query("SELECT sender, recipient, content, ts, read_at, msg_type FROM messages WHERE chat_key=? ORDER BY ts ASC",
                    (get_chat_key(me, target),))
    return [row_to_msg(r) for r in rows]


@app.post("/api/messages")
def send_message(body: SendIn, me: str = Depends(require_auth)):
    """HTTP 兜底发送（WS 断线时用），发送者强制为当前登录用户"""
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
    """会话列表：取该用户参与的每个会话的最后一条消息"""
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
    """我的好友列表（返回公开资料）"""
    out = []
    for r in db.query("SELECT friend FROM friendships WHERE username=?", (me,)):
        u = db.query("SELECT username, nickname, gender, bio, avatar, age, interests, cover FROM users WHERE username=?", (r["friend"],))
        if u:
            out.append(public_user(u[0]))
    return out


@app.get("/api/friends/requests")
async def friend_requests(me: str = Depends(require_auth)):
    """待处理好友申请：incoming=别人加我，outgoing=我加别人（还在等待）"""
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
    # 实时通知对方：我发起了好友申请
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
    """头像只允许：emoji（短、无标签）或站内上传 URL；拒绝 base64/data: 入库"""
    if val == "":
        return True
    if val.startswith("/uploads/avatars/") and "<" not in val:
        return True
    if len(val) <= 8 and "<" not in val:
        return True
    return False


def _delete_old_avatar(avatar):
    """只删我们自己落盘的图片（emoji / 外链不动），删失败无所谓"""
    if avatar and avatar.startswith("/uploads/avatars/"):
        p = UPLOAD_DIR / Path(avatar).name
        try:
            if p.exists():
                p.unlink()
        except OSError:
            pass


@app.post("/api/avatar")
async def upload_avatar(file: UploadFile = File(...), me: Optional[str] = Depends(optional_auth)):
    """图片上传：落盘到持久卷、返回 URL。已登录直接写库并换头像；未登录（注册阶段）仅暂存。"""
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
    """登录后直接换头像（emoji 或已上传 URL，不收 base64）"""
    val = body.avatar.strip()
    if not _is_safe_avatar(val):
        raise HTTPException(400, "头像只能是 emoji 或站内上传图片 URL")
    old = db.query("SELECT avatar FROM users WHERE username=?", (me,))
    _delete_old_avatar(old[0]["avatar"] if old else None)
    db.execute("UPDATE users SET avatar=? WHERE username=?", (val, me))
    return {"ok": True, "avatar": val}


# ==================== 个人资料编辑 / 媒体上传 ====================
def _is_safe_image(val) -> bool:
    """封面/媒体地址：允许空或站内 /uploads/ URL，拒绝 base64/外链"""
    if val is None or val == "":
        return True
    return isinstance(val, str) and val.startswith("/uploads/") and "<" not in val


def _delete_managed_file(url):
    """删除我们自己落盘的上传文件（头像/媒体），emoji、外链、非本站路径不动"""
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
    """个人中心保存：按传入字段局部更新，None 不动"""
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
    """聊天图片/短视频上传：按类型校验大小、落盘到 MEDIA_DIR，返回 {url, kind}。
    封面图也复用此接口上传，再通过 PUT /api/me 写库。"""
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
    """查看某个用户的主页资料（弹窗用）：排除密码，含 age/interests/cover"""
    rows = db.query("SELECT {} FROM users WHERE username=?".format(USER_PUBLIC_COLS), (username,))
    if not rows:
        raise HTTPException(404, "用户不存在")
    return public_user(rows[0])


# ==================== WebSocket：在线表 + 实时推送 ====================
# 在线连接表：username -> WebSocket
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
    """对方在线就直接推；离线则只落库，等其拉取历史消息"""
    ws = connected.get(username)
    if not ws:
        return False
    await ws.send_text(json.dumps(payload, ensure_ascii=False))
    return True


@app.websocket("/ws/{username}")
async def chat_ws(websocket: WebSocket, username: str):
    # 浏览器的 WebSocket 无法自定义请求头，token 只能走 query 参数
    if verify_token(websocket.query_params.get("token")) != username:
        # 未 accept 就 close：在握手阶段直接拒绝（HTTP 403），不给它建通道
        await websocket.close(code=1008)
        return
    await websocket.accept()
    connected[username] = websocket
    # 先把“已在线的人”全量同步给新连接：presence 是增量事件，
    # 不做这一步会导致在线名单永远缺少先登录的那些人
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
                # 纯瞬时信号，不落库，对方在线就转一下
                to = data.get("to")
                if to:
                    await push(to, {"type": "typing", "from": username})
            elif mtype == "read":
                # 我打开了与 to 的会话：把我收到、还没读的消息标为已读，有变更再告知对方
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
                # 回执给发送方（气泡上屏）+ 推给接收方（实时到达）
                await websocket.send_text(json.dumps({"type": "ack", "data": record}, ensure_ascii=False))
                await push(to, {"type": "message", "data": record})
    except WebSocketDisconnect:
        pass
    finally:
        if connected.get(username) is websocket:
            connected.pop(username, None)
            await broadcast_presence(username, False)


async def broadcast_presence(username: str, online: bool):
    """给所有在线用户广播上下线状态（好友列表灰点/绿点）"""
    payload = json.dumps({"type": "presence", "username": username, "online": online}, ensure_ascii=False)
    for name, ws in list(connected.items()):
        if name != username:
            try:
                await ws.send_text(payload)
            except Exception:
                connected.pop(name, None)


# ==================== 入口 ====================
init_db()

if __name__ == "__main__":
    print(f"WebSocket 后端启动： http://127.0.0.1:{PORT}   （数据库：{DB_TYPE}）")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
