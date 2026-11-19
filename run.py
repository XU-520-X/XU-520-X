"""run.py — 本地启动入口（独立于业务代码 ws_server.py）

作用：
  1. 给本地开发补默认环境变量（用 setdefault：devfile / 显式设置的值优先，不覆盖）
  2. 检查并自动安装缺失的 Python 依赖（fastapi / uvicorn / python-multipart）
  3. 用 uvicorn 拉起 FastAPI + WebSocket 后端

用法：
    python run.py               # 前台启动
    双击 start.bat              # Windows 下等价
可选环境变量：
    RUN_RELOAD=1                # 开启代码热重载（默认关闭，更稳）
"""
import os
import sys
import subprocess
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# ---------- 1. 本地默认环境变量（setdefault：外部已给定则不覆盖） ----------
os.environ.setdefault("DB_TYPE", "sqlite")
os.environ.setdefault("DB_PATH", str(ROOT / "data.db"))
os.environ.setdefault("PORT", "8000")

# ---------- 2. 依赖清单：(导入名, pip 安装规格) ----------
DEPENDENCIES = [
    ("fastapi", "fastapi>=0.110"),
    ("uvicorn", "uvicorn[standard]>=0.29"),
    ("multipart", "python-multipart>=0.0.9"),  # 头像图片上传接口所需
]


def ensure_deps():
    """缺哪个装哪个；都齐了就直接返回，不联网"""
    missing = [spec for mod, spec in DEPENDENCIES if importlib.util.find_spec(mod) is None]
    if not missing:
        return
    print("[启动] 检测到缺失依赖，正在自动安装：{}".format(", ".join(missing)))
    cmd = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"] + missing
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print("[启动] 依赖安装失败：{}。\n       可手动执行：pip install -r requirements.txt".format(e))
        sys.exit(1)
    importlib.invalidate_caches()


def main():
    ensure_deps()

    # 保证无论从哪个目录启动，都能 import 到 ws_server
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    port = int(os.environ.get("PORT", "8000"))
    db_type = os.environ.get("DB_TYPE", "sqlite")
    reload = os.environ.get("RUN_RELOAD", "").lower() in ("1", "true", "yes")

    print("=" * 56)
    print("  交友平台后端启动中……")
    print("   前端页面 : http://127.0.0.1:{}/".format(port))
    print("   接口文档 : http://127.0.0.1:{}/docs".format(port))
    print("   WebSocket: ws://127.0.0.1:{}/ws/<username>".format(port))
    print("   数据库   : {}  ({})".format(db_type, os.environ.get("DB_PATH")))
    print("   冒烟测试 : python ws_smoke_test.py")
    print("=" * 56)

    # 用 import 字符串形式，reload=True 时才生效（否则 uvicorn 需要 app 对象）
    import uvicorn
    uvicorn.run("ws_server:app", host="0.0.0.0", port=port, reload=reload)


if __name__ == "__main__":
    main()
