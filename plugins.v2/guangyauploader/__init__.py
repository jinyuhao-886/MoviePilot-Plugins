import json
import os
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType
from app.core.meta.metavideo import MetaVideo
from app.core.meta.metabase import MetaBase
from app.db.systemconfig_oper import SystemConfigOper

# 光鸭云盘 API
from app.plugins.guangyadisk.guangya_api import GuangYaApi
from app.plugins.guangyadisk.guangya_client import GuangYaClient

# 插件 API 注册
from app.api.endpoints.plugin import register_plugin_api

# watchdog
try:
    from watchdog.observers import Observer
    from watchdog.events import PatternMatchingEventHandler, FileSystemEvent
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False
    Observer = None
    PatternMatchingEventHandler = None
    FileSystemEvent = None


# ============================================================
# 数据结构
# ============================================================

@dataclass
class UploadTask:
    file_path: str
    event_time: float


@dataclass
class ShareRecord:
    file_name: str
    file_id: str
    file_size: int
    share_url: str
    share_time: str
    hits: int = 0


# ============================================================
# 插件主类
# ============================================================

class GuangyaUploader(_PluginBase):
    """
    光鸭云盘自动上传整理插件
    功能：目录监控上传 + 文件列表 + 一键分享
    """

    plugin_name = "光鸭云盘自动上传"
    plugin_desc = "监控本地目录上传到光鸭云盘，展示已整理文件列表，一键生成分享链接。"
    plugin_icon = "https://raw.githubusercontent.com/KoWming/MoviePilot-Plugins/main/icons/GuangyaDisk.png"
    plugin_version = "1.3.0"
    plugin_author = "jinyuhao-886"
    plugin_priority = 10
    _event = None
    _only_once = False

    # 配置
    _enabled: bool = False
    _watch_dirs: List[str] = []
    _upload_dir: str = "/待整理"
    _organized_dir: str = "/已整理"
    _delete_after_upload: bool = False
    _auto_organize: bool = True
    _upload_extensions: str = ".mkv,.mp4,.ts,.iso,.bdmv,.avi,.wmv,.mov,.flv,.m2ts"
    _only_new_files: bool = True
    _run_immediately: bool = False

    # 运行时
    _running: bool = False
    _observer: Optional[Observer] = None
    _guangya_api: Optional[GuangYaApi] = None
    _api_lock: threading.Lock = threading.Lock()
    _upload_queue: Queue = Queue()
    _worker_thread: Optional[threading.Thread] = None
    _processed_set: set = set()
    _share_records: List[ShareRecord] = []

    STABLE_INTERVAL = 2.0
    STABLE_CHECKS = 3

    # ========================================================
    # 生命周期
    # ========================================================

    def init_plugin(self, config: dict = None):
        self.stop_service()
        if not config:
            config = {}

        self._enabled = config.get("enabled", False)
        raw_dirs = config.get("watch_dirs", "")
        self._watch_dirs = [l.strip() for l in raw_dirs.strip().split("\n") if l.strip() and not l.strip().startswith("#")]
        self._upload_dir = (config.get("upload_dir", "") or "/待整理").strip()
        self._organized_dir = (config.get("organized_dir", "") or "/已整理").strip()
        self._delete_after_upload = config.get("delete_after_upload", False)
        self._auto_organize = config.get("auto_organize", True)
        self._upload_extensions = config.get("upload_extensions", ".mkv,.mp4,.ts,.iso,.bdmv,.avi,.wmv,.mov,.flv,.m2ts")
        self._only_new_files = config.get("only_new_files", True)
        self._run_immediately = config.get("run_immediately", False)
        self._organize_rename_format = (config.get("organize_rename_format", "") or "").strip()
        self._auto_share = config.get("auto_share", False)

        # 绕过系统配置缓存，直接从 DB 读取最新配置
        try:
            import sqlite3, json as _json
            _conn = sqlite3.connect('/config/user.db')
            _cur = _conn.execute(
                "SELECT VALUE FROM SYSTEMCONFIG WHERE KEY='plugin.GuangyaUploader'"
            )
            _row = _cur.fetchone()
            if _row:
                live_config = _json.loads(_row[0])
                if live_config.get("run_immediately") and not self._run_immediately:
                    self._run_immediately = True
            _conn.close()
        except Exception:
            pass

        if not self._enabled or not self._watch_dirs:
            logger.info("【光鸭上传】未启用或未配监控目录")
            # 仍然注册 API 路由，方便调试
            try:
                register_plugin_api(plugin_id=self.__class__.__name__)
            except Exception:
                pass
            return

        if not HAS_WATCHDOG:
            logger.error("【光鸭上传】watchdog 未安装")
            return

        # 确保目录存在
        for d in self._watch_dirs:
            Path(d).mkdir(parents=True, exist_ok=True)

        if not self._init_api():
            return

        self._running = True

        # worker 线程
        self._worker_thread = threading.Thread(target=self._upload_worker, name="GuangyaUploadWorker", daemon=True)
        self._worker_thread.start()

        # watchdog
        self._start_watchdog()

        if not self._only_new_files:
            self._scan_existing()

        self._load_share_records()
        logger.info(f"【光鸭上传】启动，监控 {len(self._watch_dirs)} 目录")

        # 注册插件 API 路由（热重载后需要重新注册）
        try:
            register_plugin_api(plugin_id=self.__class__.__name__)
            logger.info("【光鸭上传】API 路由已注册")
        except Exception as e:
            logger.error(f"【光鸭上传】API 路由注册失败: {e}")

        # 保存后立即执行整理
        if self._run_immediately:
            logger.info("【光鸭上传】检测到「保存后立即执行整理」开关已开启，执行整理...")
            try:
                result = self._execute_reorganize()
                if result.get("code") == 0:
                    data = result.get("data", {})
                    logger.info(f"【光鸭上传】立即执行整理完成: {data.get('msg', '')}")
                else:
                    logger.warning(f"【光鸭上传】立即执行整理结果: {result.get('msg', '')}")
            except Exception as e:
                logger.error(f"【光鸭上传】立即执行整理异常: {e}")
            finally:
                # 重置标志位，避免下次 init 再次触发
                self._run_immediately = False
                try:
                    raw = SystemConfigOper().get("plugin.GuangyaUploader")
                    cfg = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    if isinstance(cfg, dict):
                        cfg["run_immediately"] = False
                        SystemConfigOper().set("plugin.GuangyaUploader", cfg)
                        logger.info("【光鸭上传】已重置立即执行标志位")
                except Exception as e:
                    logger.error(f"【光鸭上传】重置标志位失败: {e}")

    def stop_service(self):
        self._running = False
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join(5)
            except Exception:
                pass
            self._observer = None
        self._upload_queue = Queue()
        self._worker_thread = None
        logger.info("【光鸭上传】已停止")

    def get_state(self) -> bool:
        return self._enabled and self._running

    # ========================================================
    # API 初始化
    # ========================================================

    def _init_api(self) -> bool:
        try:
            raw = SystemConfigOper().get("plugin.GuangyaDisk")
            cfg = json.loads(raw) if isinstance(raw, str) else (raw or {})
            tok = (cfg.get("access_token") or "").strip()
            ref = (cfg.get("refresh_token") or "").strip()
            if not tok or not ref:
                logger.error("【光鸭上传】光鸭云盘未登录")
                return False

            client = GuangYaClient(
                access_token=tok, refresh_token=ref,
                client_id=cfg.get("client_id", "") or GuangYaClient.DEFAULT_CLIENT_ID,
                device_id=cfg.get("device_id", ""),
                on_token_refresh=self._on_token_refresh,
            )
            with self._api_lock:
                self._guangya_api = GuangYaApi(client=client, disk_name="光鸭云盘",
                                                page_size=200, order_by=3, sort_type=1)
            return True
        except Exception as e:
            logger.error(f"【光鸭上传】API 初始化失败: {e}")
            return False

    def _on_token_refresh(self, access_token: str, refresh_token: str):
        try:
            raw = SystemConfigOper().get("plugin.GuangyaDisk")
            cfg = json.loads(raw) if isinstance(raw, str) else (raw or {})
            cfg["access_token"] = access_token
            cfg["refresh_token"] = refresh_token
            SystemConfigOper().set("plugin.GuangyaDisk", cfg)
        except Exception as e:
            logger.error(f"【光鸭上传】保存token失败: {e}")

    # ========================================================
    # 光鸭云盘操作
    # ========================================================

    def _get_api(self) -> Optional[GuangYaApi]:
        with self._api_lock:
            return self._guangya_api

    def _list_dir(self, path_str: str) -> List[Any]:
        api = self._get_api()
        if not api:
            return []
        try:
            folder = api.get_item(Path(path_str)) or api.get_folder(Path(path_str))
            return api.list(folder) if folder else []
        except Exception as e:
            logger.error(f"【光鸭上传】列目录失败 {path_str}: {e}")
            return []

    def _upload_file(self, local_path: Path, remote_dir: Any) -> Optional[Any]:
        api = self._get_api()
        if not api:
            return None
        try:
            return api.upload(remote_dir, local_path)
        except Exception as e:
            logger.error(f"【光鸭上传】上传失败 {local_path}: {e}")
            return None

    def _move_file(self, item: Any, target_path: str) -> bool:
        api = self._get_api()
        if not api:
            return False
        try:
            return api.move(item, Path(target_path))
        except Exception as e:
            logger.error(f"【光鸭上传】移动失败: {e}")
            return False

    # ========================================================
    # Watchdog 监控
    # ========================================================

    def _start_watchdog(self):
        exts = [e.strip().lower() for e in self._upload_extensions.split(",") if e.strip()]
        patterns = [f"*{ext}" for ext in exts]

        class Handler(PatternMatchingEventHandler):
            def __init__(self, plugin):
                super().__init__(patterns=patterns, ignore_directories=True, case_sensitive=False)
                self.plugin = plugin
            def on_created(self, event):
                self.plugin._enqueue(Path(event.src_path))
            def on_moved(self, event):
                if not event.is_directory:
                    self.plugin._enqueue(Path(event.dest_path))

        self._observer = Observer()
        for d in self._watch_dirs:
            if Path(d).exists():
                self._observer.schedule(Handler(self), d, recursive=True)
        self._observer.start()

    def _enqueue(self, fp: Path):
        key = str(fp)
        if key in self._processed_set or not fp.exists():
            return
        self._upload_queue.put(UploadTask(file_path=key, event_time=time.time()))

    def _upload_worker(self):
        while self._running:
            try:
                task = self._upload_queue.get(timeout=3)
            except Exception:
                continue
            try:
                self._process(task)
            except Exception as e:
                logger.error(f"【光鸭上传】处理异常: {e}")

    def _process(self, task: UploadTask):
        fp = Path(task.file_path)
        key = str(fp)
        if key in self._processed_set or not fp.exists():
            return

        # 稳定性检测
        ok = False
        for _ in range(self.STABLE_CHECKS):
            try:
                s1 = fp.stat()
                time.sleep(self.STABLE_INTERVAL)
                s2 = fp.stat()
                if s1.st_size > 0 and s1.st_size == s2.st_size and s1.st_mtime == s2.st_mtime:
                    ok = True
                    break
            except OSError:
                return
        if not ok:
            logger.warning(f"【光鸭上传】文件不稳定跳过: {fp}")
            return

        # 扩展名过滤
        exts = [e.strip().lower() for e in self._upload_extensions.split(",") if e.strip()]
        if exts and fp.suffix.lower() not in exts:
            return

        self._processed_set.add(key)

        upload_dir = self._get_api().get_folder(Path(self._upload_dir)) if self._get_api() else None
        if not upload_dir:
            logger.error(f"【光鸭上传】上传目录不存在: {self._upload_dir}")
            return

        cloud = self._upload_file(fp, upload_dir)
        if not cloud:
            logger.error(f"【光鸭上传】上传失败: {fp}")
            return
        logger.info(f"【光鸭上传】上传成功: {fp.name}")

        if self._auto_organize:
            self._move_file(cloud, self._organized_dir)
            logger.info(f"【光鸭上传】已整理: {cloud.name}")

        if self._delete_after_upload:
            try:
                fp.unlink(missing_ok=True)
            except Exception as e:
                logger.error(f"【光鸭上传】删除本地失败: {e}")

    def _scan_existing(self):
        exts = [e.strip().lower() for e in self._upload_extensions.split(",") if e.strip()]
        for d in self._watch_dirs:
            root = Path(d)
            if not root.exists():
                continue
            for f in root.rglob("*"):
                if f.is_file() and (not exts or f.suffix.lower() in exts):
                    self._enqueue(f)
        logger.info(f"【光鸭上传】初始扫描入队 {self._upload_queue.qsize()} 文件")

    # ========================================================
    # 分享功能
    # ========================================================

    def _share_file(self, file_id: str, file_name: str) -> Optional[str]:
        """
        为文件生成分享链接。
        先尝试光鸭云盘分享API，失败则返回下载链接。
        """
        api = self._get_api()
        if not api:
            return None

        # 方法1: 用 guangyapan.com 分享 API
        try:
            client = api.client
            headers = client._get_auth_headers()
            import requests
            resp = requests.post(
                "https://api.guangyapan.com/nd.bizuserres.s/v1/share/create",
                json={"fileIds": [file_id], "expireDays": 0},
                headers=headers, timeout=15,
            )
            data = resp.json()
            if data.get("code") in (0, "0") or data.get("success"):
                url = data.get("data", {}).get("url") or data.get("data", {}).get("shareUrl")
                if url:
                    return url
        except Exception:
            pass

        # 方法2: 用 get_download_url
        try:
            client = api.client
            resp = client.get_download_url(file_id)
            if resp:
                url = resp.get("data", {}).get("signedURL") or resp.get("signedURL")
                if url:
                    return url
        except Exception:
            pass

        return None

    def _load_share_records(self):
        try:
            import sqlite3
            conn = sqlite3.connect('/config/user.db')
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS guangya_shares ("
                      "file_id TEXT PRIMARY KEY, file_name TEXT, file_size INTEGER, "
                      "share_url TEXT, share_time TEXT, hits INTEGER DEFAULT 0)")
            for row in c.execute("SELECT * FROM guangya_shares"):
                self._share_records.append(ShareRecord(*row))
            conn.close()
        except Exception as e:
            logger.error(f"【光鸭上传】加载分享记录失败: {e}")

    def _save_share_record(self, rec: ShareRecord):
        self._share_records = [r for r in self._share_records if r.file_id != rec.file_id]
        self._share_records.append(rec)
        try:
            import sqlite3
            conn = sqlite3.connect('/config/user.db')
            conn.execute("INSERT OR REPLACE INTO guangya_shares VALUES (?,?,?,?,?,?)",
                         (rec.file_id, rec.file_name, rec.file_size, rec.share_url, rec.share_time, rec.hits))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"【光鸭上传】保存分享失败: {e}")

    # ========================================================
    # API 接口
    # ========================================================

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/share_file", "endpoint": self._api_share_file, "methods": ["GET"], "summary": "分享文件"},
            {"path": "/copylink", "endpoint": self._api_copylink, "methods": ["GET"], "summary": "获取下载链接"},
            {"path": "/share_records", "endpoint": self._api_share_records, "methods": ["GET"], "summary": "分享记录"},
            {"path": "/status", "endpoint": self._api_status, "methods": ["GET"], "summary": "上传状态"},
            {"path": "/reorganize", "endpoint": self._api_reorganize, "methods": ["GET"], "summary": "手动整理云盘"},
        ]

    def _api_share_file(self, **kwargs) -> Dict:
        file_id = kwargs.get("file_id", "")
        if not file_id:
            return {"code": 1, "msg": "缺少 file_id"}
        file_name = kwargs.get("file_name", "未知文件")
        url = self._share_file(file_id, file_name)
        if url:
            self._save_share_record(ShareRecord(file_name=file_name, file_id=file_id,
                                                 file_size=0, share_url=url,
                                                 share_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            return {"code": 0, "data": {"url": url, "file_name": file_name}}
        return {"code": 1, "msg": "生成分享链接失败"}

    def _api_copylink(self, **kwargs) -> Dict:
        file_id = kwargs.get("file_id", "")
        if not file_id:
            return {"code": 1, "msg": "缺少 file_id"}
        api = self._get_api()
        if not api:
            return {"code": 1, "msg": "API 未初始化"}
        try:
            resp = api.client.get_download_url(file_id)
            url = resp.get("data", {}).get("signedURL") or resp.get("signedURL") if resp else None
            if url:
                return {"code": 0, "data": {"url": url}}
        except Exception as e:
            return {"code": 1, "msg": str(e)}
        return {"code": 1, "msg": "获取链接失败"}

    def _api_share_records(self, **kwargs) -> Dict:
        return {"code": 0, "data": {
            "records": [{"file_name": r.file_name, "file_id": r.file_id,
                         "share_url": r.share_url, "share_time": r.share_time,
                         "hits": r.hits} for r in self._share_records],
            "total": len(self._share_records)
        }}

    def _api_status(self, **kwargs) -> Dict:
        return {"code": 0, "data": {
            "running": self._running, "enabled": self._enabled,
            "queue": self._upload_queue.qsize(), "processed": len(self._processed_set),
            "shares": len(self._share_records),
            "watch_dirs": self._watch_dirs,
            "organized_dir": self._organized_dir,
        }}

    def _execute_reorganize(self) -> Dict:
        """
        执行云盘整理（MP 原生 TransferChain，走光鸭云盘储存后端）
        扫描 /待整理 → FileItem → TransferChain.do_transfer() → TMDB识别→重命名→移到/已整理

        支持自定义重命名模板（_organize_rename_format）和整理后自动分享（_auto_share）
        """
        api = self._get_api()
        if not api:
            return {"code": 1, "msg": "API 未初始化"}
        try:
            upload_dir = api.get_folder(Path(self._upload_dir))
            if not upload_dir:
                return {"code": 1, "msg": f"上传目录不存在: {self._upload_dir}"}

            items = api.list(upload_dir) or []

            from app.chain.transfer import TransferChain
            from app.schemas.file import FileItem
            from app.schemas.system import TransferDirectoryConf
            from app.db.systemconfig_oper import SystemConfigOper
            from app.schemas.types import SystemConfigKey

            # 读取「光鸭整理」目录别名配置
            dir_confs: list = SystemConfigOper().get(SystemConfigKey.Directories) or []
            target_conf = None
            for dc in dir_confs:
                if dc.get("name") == "光鸭整理":
                    target_conf = TransferDirectoryConf(**dc)
                    break

            if not target_conf:
                return {"code": 1, "msg": "未找到「光鸭整理」目录别名配置，请在MP后台设置"}

            transferchain = TransferChain()

            # ----- 自定义重命名模板逻辑 -----
            # 如果用户设置了自定义格式，临时覆写系统 settings
            from app.core.config import settings
            _overridden = False
            _old_movie = None
            _old_tv = None
            if self._organize_rename_format:
                _old_movie = settings.MOVIE_RENAME_FORMAT
                _old_tv = settings.TV_RENAME_FORMAT
                settings.MOVIE_RENAME_FORMAT = self._organize_rename_format
                settings.TV_RENAME_FORMAT = self._organize_rename_format
                _overridden = True
                logger.info(f"【光鸭上传】使用自定义重命名模板: {self._organize_rename_format}")

            succeeded = 0
            failed = 0
            try:
                for item in items:
                    if item.type == "dir":
                        continue
                    try:
                        ext = os.path.splitext(item.name)[1].lower().lstrip(".")
                        fileitem = FileItem(
                            storage="光鸭云盘",
                            fileid=str(item.fileid),
                            parent_fileid=str(getattr(item, 'parent_fileid', upload_dir.fileid)),
                            path=item.path or f"{self._upload_dir}/{item.name}",
                            type="file",
                            name=item.name,
                            basename=os.path.splitext(item.name)[0],
                            extension=ext or "unknown",
                            size=item.size or 0,
                        )
                        logger.info(
                            f"【光鸭上传】整理 {item.name}: storage={fileitem.storage}"
                        )
                        ok, msg_or_data = transferchain.do_transfer(
                            fileitem=fileitem,
                            target_directory=target_conf,
                        )
                        logger.info(
                            f"【光鸭上传】do_transfer: ok={ok}, msg={msg_or_data}"
                        )
                        if ok:
                            succeeded += 1
                            logger.info(f"【光鸭上传】整理成功: {item.name}")
                            # 整理成功后自动创建分享链接
                            if self._auto_share:
                                self._auto_create_share(item)
                        else:
                            failed += 1
                            logger.warning(f"【光鸭上传】整理失败 {item.name}: {msg_or_data}")
                    except Exception as e:
                        logger.error(f"【光鸭上传】整理异常 {item.name}: {e}")
                        failed += 1
            finally:
                # 恢复系统重命名模板
                if _overridden:
                    settings.MOVIE_RENAME_FORMAT = _old_movie
                    settings.TV_RENAME_FORMAT = _old_tv
                    logger.info("【光鸭上传】已恢复系统重命名模板")

            msg = f"整理完成: 成功 {succeeded} 个, 失败 {failed} 个"
            logger.info(f"【光鸭上传】{msg}")
            return {"code": 0, "data": {
                "succeeded": succeeded, "failed": failed, "msg": msg
            }}
        except Exception as e:
            logger.error(f"【光鸭上传】整理异常: {e}")
            return {"code": 1, "msg": f"整理异常: {str(e)}"}

    def _auto_create_share(self, item) -> None:
        """
        对整理成功的文件自动创建分享链接并保存记录
        """
        try:
            from datetime import datetime
            url = self._share_file(str(item.fileid), item.name)
            if url:
                self._save_share_record(ShareRecord(
                    file_name=item.name, file_id=str(item.fileid),
                    file_size=item.size or 0, share_url=url,
                    share_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ))
                logger.info(f"【光鸭上传】自动分享成功: {item.name} → {url[:60]}...")
            else:
                logger.warning(f"【光鸭上传】自动分享失败: {item.name}")
        except Exception as e:
            logger.error(f"【光鸭上传】自动分享异常 {item.name}: {e}")

    def _api_reorganize(self, **kwargs) -> Dict:
        """
        手动整理 API 接口 — 委托给 _execute_reorganize
        """
        return self._execute_reorganize()

    # ========================================================
    # 表单
    # ========================================================

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {"cmd": "/guangya", "event": EventType.PluginAction,
             "desc": "光鸭云盘上传状态", "data": {"action": "status"}}
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "watch_dirs", "label": "监控目录（每行一个）", "rows": 3,
                            "placeholder": "/vol3/1000/光鸭待上传"
                        }}
                    ]}
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "upload_dir", "label": "光鸭云盘上传目录", "placeholder": "/待整理"
                        }}
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "organized_dir", "label": "光鸭云盘已整理目录", "placeholder": "/已整理"
                        }}
                    ]}
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "upload_extensions", "label": "上传文件后缀",
                            "placeholder": ".mkv,.mp4,.ts,.iso"
                        }}
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "auto_organize", "label": "自动整理"
                        }}
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "delete_after_upload", "label": "删除本地"
                        }}
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "only_new_files", "label": "仅新文件"
                        }}
                    ]}
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "organize_rename_format", "label": "🔄 整理重命名模板（Jinja2）",
                            "placeholder": "留空=使用系统默认模板",
                            "hint": "自定义重命名格式，例如去掉压制组后缀"
                        }}
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "auto_share", "label": "自动分享",
                            "color": "primary",
                            "hint": "整理成功后自动创建光鸭网盘分享链接"
                        }}
                    ]}
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "run_immediately", "label": "⚡ 保存后立即执行整理",
                            "color": "warning",
                            "hint": "开启后，点击保存将自动对「待整理」目录执行一次整理操作（完成后自动关闭此开关）"
                        }}
                    ]}
                ]}
            ]}
        ], {
            "enabled": False, "watch_dirs": "",
            "upload_dir": "/待整理", "organized_dir": "/已整理",
            "delete_after_upload": False, "auto_organize": True,
            "upload_extensions": ".mkv,.mp4,.ts,.iso,.bdmv,.avi,.wmv,.mov,.flv,.m2ts",
            "only_new_files": True, "run_immediately": False,
            "auto_share": False,
            "organize_rename_format": "{{title}}{% if year %} ({{year}}){% endif %} {tmdbid={{tmdbid}}}/{{title}}{% if year %} ({{year}}){% endif %}{% if videoFormat %} - {{videoFormat}}{% if edition %}.{{edition}}{% endif %}{% if audioCodec %}.{{audioCodec}}{% endif %}{% if videoCodec %}.{{videoCodec}}{% endif %}{% endif %}{{fileExt}}",
        }

    # ========================================================
    # 页面（文件列表 + 分享按钮）
    # ========================================================

    def get_page(self) -> List[dict]:
        """
        文件列表页面 — 显示已整理目录内容，每行带分享/复制链接按钮
        """

        if not self._enabled:
            return [{"component": "VAlert", "props": {"type": "info", "text": "插件未启用，请先配置并启用"}}]

        if not self._guangya_api:
            return [{"component": "VAlert", "props": {"type": "warning", "text": "光鸭云盘 API 未就绪，请检查 GuangyaDisk 插件是否已配置登录"}}]

        # 获取文件列表
        files = self._list_dir(self._organized_dir)
        share_map = {r.file_id: r for r in self._share_records}

        if not files:
            return [{"component": "VAlert", "props": {"type": "info",
                "text": f"【{self._organized_dir}】目录为空。已上传文件会自动整理到这里，然后就可以分享啦 🎯"}},
                    {"component": "VAlert", "props": {"type": "info",
                "text": f"📂 上传队列: {self._upload_queue.qsize()} | 已处理: {len(self._processed_set)} 个文件"}}]

        # 构建表格行
        rows = []
        for f in files:
            ftype = "📁" if f.type == "dir" else "🎬"
            size_str = f"{f.size / 1024 / 1024:.1f}MB" if f.size else "-"
            shared = share_map.get(f.fileid)
            share_icon = "🔗" if shared else "🔲"
            share_text = "已分享" if shared else "未分享"

            # 时间
            mtime = f.modify_time if hasattr(f, 'modify_time') and f.modify_time else ""

            rows.append({
                "cols": 12, "content": [
                    {"component": "VCard", "props": {"variant": "outlined", "class": "mb-2"}, "content": [
                        {"component": "VCardText", "content": [
                            # 文件信息
                            {"component": "VRow", "content": [
                                {"component": "VCol", "props": {"cols": 12, "md": 5},
                                 "content": [{"component": "VListItemTitle",
                                              "props": {"text": f"{ftype} {f.name}"}}]},
                                {"component": "VCol", "props": {"cols": 4, "md": 2},
                                 "content": [{"component": "VChip", "props": {"size": "small", "color": "grey"},
                                              "text": size_str}]},
                                {"component": "VCol", "props": {"cols": 4, "md": 2},
                                 "content": [{"component": "VChip",
                                              "props": {"size": "small", "color": "green" if shared else "default"},
                                              "text": f"{share_icon} {share_text}"}]},
                                # 操作按钮
                                {"component": "VCol", "props": {"cols": 6, "md": 1},
                                 "content": [{"component": "VBtn", "props": {
                                     "size": "small", "color": "primary", "variant": "tonal",
                                     "icon": "mdi-share-variant",
                                 }, "text": "分享",
                                    "events": {
                                        "click": {
                                            "api": f"plugin/GuangyaUploader/share_file?file_id={f.fileid}&file_name={f.name}",
                                            "method": "get",
                                        }
                                    }}]},
                                {"component": "VCol", "props": {"cols": 6, "md": 1},
                                 "content": [{"component": "VBtn", "props": {
                                     "size": "small", "color": "secondary", "variant": "tonal",
                                     "icon": "mdi-link-variant",
                                 }, "text": "链接",
                                    "events": {
                                        "click": {
                                            "api": f"plugin/GuangyaUploader/copylink?file_id={f.fileid}",
                                            "method": "get",
                                        }
                                    }}]},
                            ]},
                            # 如果已分享，显示分享链接
                            *([{"component": "VRow", "content": [
                                {"component": "VCol", "props": {"cols": 12},
                                 "content": [{"component": "VTextField", "props": {
                                     "model": "", "label": "分享链接",
                                     "value": shared.share_url,
                                     "readonly": True,
                                     "hint": f"分享时间: {shared.share_time}"
                                 }}]}
                            ]}] if shared else []),
                        ]}
                    ]}
                ]
            })

        # 顶部状态栏
        status = [{"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 6, "md": 3},
             "content": [{"component": "VCard", "content": [
                 {"component": "VCardTitle", "props": {"text": "📂 文件总数"}},
                 {"component": "VCardText", "props": {"text": str(len(files))}},
             ]}]},
            {"component": "VCol", "props": {"cols": 6, "md": 3},
             "content": [{"component": "VCard", "content": [
                 {"component": "VCardTitle", "props": {"text": "✅ 已分享"}},
                 {"component": "VCardText", "props": {"text": str(len(self._share_records))}},
             ]}]},
            {"component": "VCol", "props": {"cols": 6, "md": 3},
             "content": [{"component": "VCard", "content": [
                 {"component": "VCardTitle", "props": {"text": "⏳ 队列中"}},
                 {"component": "VCardText",
                  "props": {"text": str(self._upload_queue.qsize())}},
             ]}]},
            {"component": "VCol", "props": {"cols": 6, "md": 3},
             "content": [{"component": "VCard", "content": [
                 {"component": "VCardTitle", "props": {"text": "🔄 待整理"}},
                 {"component": "VCardText",
                  "props": {"text": "..."}},
             ]}]},
        ]}]

        return [
            *status,
            {"component": "VAlert", "props": {
                "type": "info",
                "text": f"📁 已整理目录: {self._organized_dir} | 点击「分享」按钮生成链接，点击「链接」获取下载直链"
            }},
            *rows,
        ]
