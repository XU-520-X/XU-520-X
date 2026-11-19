"""后端鉴权 + WebSocket 冒烟测试。

用法：
    python ws_smoke_test.py [http://127.0.0.1:8000] [sqlite文件路径]
第二个参数可选：传入正在被服务端使用的 .db 路径，会额外校验“旧无盐 sha256 登录成功后升级为 pbkdf2”。

覆盖：登录签发 token / 无 token 拒绝 / 伪造 sender 无效 / token 与身份不符时 WS 握手被拒 /
      实时消息 / presence 全量同步与增量 / typing / read / 非法收件人 / 注册校验 /
      好友门槛 / 资料编辑与主页查询 / 图片短视频上传与媒体消息
"""
import asyncio
import base64
import json
import os
import sqlite3
import secrets
import sys
import urllib.error
import urllib.request

import websockets

HTTP = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
WS = HTTP.replace("http", "ws", 1)
DB_FILE = sys.argv[2] if len(sys.argv) > 2 else ""

# 1x1 像素 PNG，够小且是真实图片头
TINY_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFAAH/q842iQAAAABJRU5ErkJggg==")


def http_json(path, data=None, token=None, method=None):
    """返回 (status, payload)，不抛异常，方便断言 401/403 这类负向用例"""
    req = urllib.request.Request(HTTP + path)
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        req.add_header("Content-Type", "application/json")
    if method:
        req.get_method = lambda: method
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, data=body) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            return e.code, json.loads(raw or "{}")
        except json.JSONDecodeError:
            return e.code, {}


def http_upload(path, filename, content_type, raw, token=None):
    """multipart/form-data 上传（不引入 requests，手写 boundary）"""
    boundary = "----qoder" + secrets.token_hex(8)
    head = ("--{}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n"
            "Content-Type: {}\r\n\r\n").format(boundary, filename, content_type).encode("utf-8")
    tail = "\r\n--{}--\r\n".format(boundary).encode("utf-8")
    req = urllib.request.Request(HTTP + path, data=head + raw + tail, method="POST")
    req.add_header("Content-Type", "multipart/form-data; boundary=" + boundary)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raw_body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(raw_body or "{}")
        except json.JSONDecodeError:
            return e.code, {}


async def wait_type(ws, want, timeout=5):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        left = deadline - loop.time()
        if left <= 0:
            raise AssertionError("未收到 " + want)
        msg = json.loads(await asyncio.wait_for(ws.recv(), left))
        if msg.get("type") == want:
            return msg


async def ws_rejected(username, token):
    """True = 握手被拒（符合预期）"""
    url = "{}/ws/{}".format(WS, username) + ("?token=" + token if token else "")
    try:
        async with websockets.connect(url):
            return False
    except Exception:
        return True


async def main():
    # ---------- 1. REST 鉴权 ----------
    code, res = http_json("/api/users")
    assert code == 401, (code, res)
    print("no-token  -> GET /api/users 被拒 401")

    code, bad = http_json("/api/login", {"username": "chenchen", "password": "wrong-pwd"})
    assert code == 401, (code, bad)

    code, chen = http_json("/api/login", {"username": "chenchen", "password": "123456"})
    assert code == 200 and chen.get("token"), (code, chen)
    assert "password" not in chen, "响应不能带出密码散列"
    t_chen = chen["token"]
    print("login     -> 签发 token，且响应体不含密码散列")

    code, users = http_json("/api/users", token=t_chen)
    assert code == 200 and "chenchen" not in [u["username"] for u in users], users
    print("self-list -> 服务端按 token 排除自己")

    # 伪造 sender：传 xiaoyu 也必须记成 chenchen
    code, sent = http_json("/api/messages",
                           {"sender": "xiaoyu", "recipient": "linlin", "content": "spoof-check"},
                           token=t_chen)
    assert code == 200 and sent["message"]["from"] == "chenchen", sent
    print("spoof     -> 伪造 sender 无效，落库身份来自 token")

    code, _ = http_json("/api/messages", {"recipient": "ghost", "content": "x"}, token=t_chen)
    assert code == 404, code
    print("bad-to    -> 不存在的收件人返回 404")

    # ---------- 2. 注册校验 ----------
    # 注意：pydantic 的字段校验（422）先于端点内的业务校验（400）执行，
    # 所以“非法字符”用例必须搭配一个合法长度的密码，否则会被 422 先拦掉
    code, _ = http_json("/api/register", {"username": "bad name!", "nickname": "n", "password": "123456"})
    assert code == 400, (code, "非法账号名应被拒")
    code, _ = http_json("/api/register", {"username": "okuser1", "nickname": "n", "password": "123"})
    assert code == 422, (code, "过短密码应被拒")
    new_user = "t" + secrets.token_hex(4)   # 每次跑用不同账号，保证脚本可重复执行
    code, _ = http_json("/api/register", {"username": new_user, "nickname": "测试号", "password": "123456"})
    assert code == 200, (code, "合法注册应成功")
    code, t = http_json("/api/login", {"username": new_user, "password": "123456"})
    assert code == 200 and t.get("token"), (code, t)
    t_new = t["token"]
    code, again = http_json("/api/register", {"username": new_user, "nickname": "n", "password": "123456"})
    assert code == 409, (code, "重复账号应被拒")
    print("register  -> 字符集/密码长度/重名 校验生效，合法注册可登录")

    # ---------- 3. WS 握手鉴权 ----------
    code2, xiao = http_json("/api/login", {"username": "xiaoyu", "password": "123456"})
    assert code2 == 200, (code2, xiao)
    t_xiao = xiao["token"]

    assert await ws_rejected("xiaoyu", None), "无 token 不应连上"
    assert await ws_rejected("xiaoyu", t_chen), "拿 chenchen 的 token 冒充 xiaoyu 不应连上"
    assert await ws_rejected("xiaoyu", "garbage.sig"), "伪造 token 不应连上"
    print("ws-guard  -> 无 token / 身份不符 / 伪造签名 均握手失败")

    # ---------- 4. 正常业务链路 ----------
    async with websockets.connect(WS + "/ws/chenchen?token=" + t_chen) as a, \
                websockets.connect(WS + "/ws/xiaoyu?token=" + t_xiao) as b:
        sync = await wait_type(b, "presence_sync")
        assert "chenchen" in sync["users"], sync
        inc = await wait_type(a, "presence")
        assert inc["username"] == "xiaoyu" and inc["online"] is True, inc
        print("presence  -> 全量同步 + 增量广播正常")

        await a.send(json.dumps({"type": "message", "to": "xiaoyu", "content": "ws-auth-msg"}))
        ack = await wait_type(a, "ack")
        got = await wait_type(b, "message")
        assert ack["data"]["content"] == got["data"]["content"] == "ws-auth-msg"
        print("message   -> ack 与实时推送一致")

        await a.send(json.dumps({"type": "typing", "to": "xiaoyu"}))
        tp = await wait_type(b, "typing")
        assert tp["from"] == "chenchen", tp
        print("typing    -> 已转发")

        # 已读回执：接收方 xiaoyu 打开会话上报已读，发送方 chenchen 应收到 read 事件
        await b.send(json.dumps({"type": "read", "to": "chenchen"}))
        rd = await wait_type(a, "read")
        assert rd["by"] == "xiaoyu", rd
        print("read      -> 已读回执推送正常")

        await a.send(json.dumps({"type": "message", "to": "ghost", "content": "hi"}))
        err = await wait_type(a, "error")
        assert "接收人" in err["msg"], err
        print("bad-peer  -> 非法收件人被拒且未落库")

        await a.send(json.dumps({"type": "ping"}))
        await wait_type(a, "pong")
        print("ping/pong -> 心跳正常")

    # ---------- 4b. 好友门槛与申请流程 ----------
    code, _ = http_json("/api/messages", {"recipient": "ahao", "content": "hi"}, token=t_new)
    assert code == 403, (code, "非好友直发应被拒 403")
    print("friend-gate -> 非好友发消息被 403 拦截")

    code, _ = http_json("/api/friends/request", {"to": "ahao"}, token=t_new)
    assert code == 200, (code, "发起好友申请应成功")
    code, ahao = http_json("/api/login", {"username": "ahao", "password": "123456"})
    assert code == 200 and ahao.get("token"), (code, ahao)
    t_ahao = ahao["token"]
    code, reqs = http_json("/api/friends/requests", token=t_ahao)
    assert code == 200 and new_user in [u["username"] for u in reqs["incoming"]], reqs
    print("req-list    -> 接收端可看到待处理申请")

    code, _ = http_json("/api/friends/respond", {"target": new_user, "accept": True}, token=t_ahao)
    assert code == 200, (code, "同意好友申请应成功")
    code, sent = http_json("/api/messages", {"recipient": "ahao", "content": "now friends"}, token=t_new)
    assert code == 200 and sent["message"]["from"] == new_user, sent
    code, fl = http_json("/api/friends", token=t_new)
    assert code == 200 and "ahao" in [u["username"] for u in fl], fl
    print("friend-flow -> 申请/同意后即可私聊，好友关系双向可见")

    # ---------- 4c. 资料编辑与用户主页 ----------
    code, upd = http_json("/api/me", {"nickname": "改名了", "age": 27,
                                      "interests": ["旅行", "手冲"], "bio": "冒烟签名"},
                          token=t_new, method="PUT")
    assert code == 200 and upd["nickname"] == "改名了" and upd["age"] == 27, upd
    assert upd["interests"] == ["旅行", "手冲"], upd
    assert "password" not in upd, "更新响应不能带出密码散列"
    code, prof = http_json("/api/profile?username=" + new_user, token=t_chen)
    assert code == 200 and prof["age"] == 27 and prof["interests"] == ["旅行", "手冲"], prof
    assert "password" not in prof, "主页响应不能带出密码散列"
    print("profile   -> PUT /api/me 局部更新生效，GET /api/profile 可见年龄/兴趣且不含密码")

    code, _ = http_json("/api/me", {"cover": "data:image/png;base64,AAAA"}, token=t_new, method="PUT")
    assert code == 400, (code, "base64 封面应被拒")
    code, _ = http_json("/api/me", {"age": 999}, token=t_new, method="PUT")
    assert code == 400, (code, "非法年龄应被拒")
    print("field-guard -> 非法封面/年龄被拒 400")

    # ---------- 4d. 聊天图片与短视频 ----------
    code, up = http_upload("/api/media", "p.png", "image/png", TINY_PNG, t_new)
    assert code == 200 and up["kind"] == "image" and up["url"].startswith("/uploads/media/"), up
    code, _ = http_upload("/api/media", "x.txt", "text/plain", b"hello", t_new)
    assert code == 400, (code, "非图片/视频类型应被拒")
    # 上传完的文件必须能静态访问
    with urllib.request.urlopen(HTTP + up["url"]) as r:
        assert r.status == 200 and r.read() == TINY_PNG, "上传后的图片应可访问"
    print("upload    -> 图片落盘、类型校验生效、静态 URL 可访问")

    code, sent = http_json("/api/messages", {"recipient": "ahao", "content": up["url"], "kind": "image"}, token=t_new)
    assert code == 200 and sent["message"]["kind"] == "image", sent
    code, _ = http_json("/api/messages", {"recipient": "ahao", "content": "/etc/passwd", "kind": "image"}, token=t_new)
    assert code == 400, (code, "伪造媒体地址应被拒")
    code, msgs = http_json("/api/messages?target=ahao", token=t_new)
    assert msgs and msgs[-1]["kind"] == "image", msgs[-1] if msgs else msgs
    code, chats = http_json("/api/chats", token=t_new)
    ahao_chat = [c for c in chats if c["user"]["username"] == "ahao"]
    assert ahao_chat and ahao_chat[0]["lastMsg"] == "[图片]", ahao_chat
    print("media-msg -> 图片消息按 kind 落库，会话列表显 [图片]")

    code, vid = http_upload("/api/media", "c.mp4", "video/mp4", b"\x00\x00\x00\x18ftypmp42", t_new)
    assert code == 200 and vid["kind"] == "video", vid
    print("video     -> 短视频按 video 类型受理")

    # ---------- 4e. WS 上的媒体消息 ----------
    async with websockets.connect(WS + "/ws/chenchen?token=" + t_chen) as a, \
                websockets.connect(WS + "/ws/xiaoyu?token=" + t_xiao) as b:
        await a.send(json.dumps({"type": "message", "to": "xiaoyu", "content": up["url"], "kind": "image"}))
        ack = await wait_type(a, "ack")
        got = await wait_type(b, "message")
        assert ack["data"]["kind"] == got["data"]["kind"] == "image", (ack, got)
        await a.send(json.dumps({"type": "message", "to": "xiaoyu",
                                 "content": "/uploads/avatars/../../secret.key", "kind": "image"}))
        err = await wait_type(a, "error")
        assert "媒体" in err["msg"], err
        print("ws-media  -> 图片消息 ack/推送带 kind，非法媒体地址被拒")

    # ---------- 5. 旧密码散列平滑升级 ----------
    if DB_FILE and os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute("SELECT password FROM users WHERE username='xiaoyu'").fetchone()
        conn.close()
        assert row and row[0].startswith("pbkdf2$"), row
        print("upgrade   -> 旧无盐 sha256 已升级为 pbkdf2 加盐散列")

    print("\n全部通过：鉴权、防冒充、实时收发、在线状态、好友门槛、资料/主页、媒体上传 均正常")


asyncio.run(main())
