"""
115网盘订阅追更插件
结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失剧集
"""
import datetime
from pathlib import Path
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.core.config import settings, global_vars
from app.core.event import Event, eventmanager
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper
from app.db.models.site import Site
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType

from .clients import PanSouClient, P115ClientManager, NullbrClient, HDHiveOpenAPIClient, HDHiveOpenAPIError
from .handlers import SearchHandler, SyncHandler, SubscribeHandler, ApiHandler
from .ui import UIConfig
from .utils import download_so_file

lock = Lock()


class P115StrgmSub(_PluginBase):
    """115网盘订阅追更插件"""

    # 插件名称
    plugin_name = "115网盘订阅追更"
    # 插件描述
    plugin_desc = "结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失的电影和剧集。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    # 插件版本
    plugin_version = "1.5.3-modi.4"
    # 插件作者
    plugin_author = "jinyuhao-886"
    # 作者主页
    author_url = "https://github.com/jinyuhao-886"
    # 插件配置项ID前缀
    plugin_config_prefix = "p115strgmsub_"
    plugin_order = 20
    auth_level = 1

    # 私有变量
    _scheduler: Optional[BackgroundScheduler] = None
    _toggle_scheduler: Optional[BackgroundScheduler] = None  # 用于延迟切换/窗口切换

    # 配置属性
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = "0 18-23 * * *"
    _notify: bool = False

    _cookies: str = ""
    _pansou_enabled: bool = True
    _pansou_url: str = "https://so.252035.xyz"
    _pansou_username: str = ""
    _pansou_password: str = ""
    _pansou_auth_enabled: bool = False
    _pansou_channels: str = "QukanMovie"

    _save_path: str = "/我的接收/MoviePilot/TV"
    _movie_save_path: str = "/我的接收/MoviePilot/Movie"
    _only_115: bool = True
    # 订阅过滤模式："exclude" 排除模式（处理除勾选外的全部订阅）/ "include" 指定模式（仅处理勾选的订阅）
    _subscribe_filter_mode: str = "exclude"
    _exclude_subscribes: List[int] = []
    _include_subscribes: List[int] = []
    # 全局兜底排除规则（硬拒绝型）：所有订阅转存前会先过这一关，默认杜绝杜比视界
    _global_exclude: str = r"DoVi|Dolby[\s.]?Vision|DOVI|杜比视界"
    # 搜索源优先级（按列表顺序），为空时默认 Nullbr > HDHive > PanSou
    _search_source_order: List[str] = []

    _nullbr_enabled: bool = False
    _nullbr_appid: str = ""
    _nullbr_api_key: str = ""

    _hdhive_enabled: bool = False
    _hdhive_username: str = ""
    _hdhive_password: str = ""
    _hdhive_cookie: str = ""
    _hdhive_auto_refresh: bool = False
    _hdhive_refresh_before: int = 86400
    _hdhive_query_mode: str = "api"
    # OpenAPI 应用凭证：应用 Secret 放 X-API-Key（沿用 hdhive_api_key 配置键）
    _hdhive_api_key: str = ""
    _hdhive_client_id: str = ""
    _hdhive_redirect_uri: str = ""
    # OAuth 用户授权（授权码为一次性输入，换取 Token 后自动清空）
    _hdhive_auth_code: str = ""
    _hdhive_access_token: str = ""
    _hdhive_refresh_token: str = ""
    _hdhive_token_expires_at: float = 0
    _hdhive_auto_unlock: bool = False
    _hdhive_max_unlock_points: int = 50
    _hdhive_max_points_per_sub: int = 20

    # 是否屏蔽系统订阅（True=已屏蔽系统订阅，False=已恢复系统订阅）
    _block_system_subscribe: bool = False

    _max_transfer_per_sync: int = 50
    _batch_size: int = 20
    _skip_other_season_dirs: bool = True

    # 洗版配置
    _auto_best_version: bool = False
    _min_upgrade_tiers: int = 2
    _self_heal_interval: int = 10  # 分钟
    _enable_cloud_upgrade: bool = False
    _enable_pt_upgrade: bool = False
    _cloud_tv_local_dir: str = ""  # 本地电视剧strm根目录（网盘洗版用）
    _cloud_tv_remote_dir: str = ""  # 115网盘电视剧目录（网盘洗版用）
    _cloud_movie_local_dir: str = ""  # 本地电影strm根目录（网盘洗版用）
    _cloud_movie_remote_dir: str = ""  # 115网盘电影目录（网盘洗版用）
    # 取消屏蔽时间段（仅当 block_system_subscribe=OFF 时生效）
    _unblock_start_time: str = "17:30"
    _unblock_end_time: str = "23:59"
    # 当前是否处于接管态（True=强制仅115，False=用户原始站点）
    _is_blocked: bool = False

    # 运行时对象
    _pansou_client: Optional[PanSouClient] = None
    _p115_manager: Optional[P115ClientManager] = None
    _nullbr_client: Optional[NullbrClient] = None
    _hdhive_client: Optional[Any] = None

    # 处理器
    _search_handler: Optional[SearchHandler] = None
    _subscribe_handler: Optional[SubscribeHandler] = None
    _sync_handler: Optional[SyncHandler] = None
    _api_handler: Optional[ApiHandler] = None

    # ------------------ cron 合法性校验（轻量版,不卡 8 小时间隔） ------------------

    @staticmethod
    def _cron_is_valid(cron_expr: str) -> bool:
        """仅校验 cron 表达式是否合法,不再强制最小间隔"""
        cron_expr = (cron_expr or "").strip()
        if not cron_expr:
            return False
        try:
            tz = pytz.timezone(settings.TZ)
            CronTrigger.from_crontab(cron_expr, timezone=tz)
            return True
        except Exception:
            return False

    # ------------------ 站点解析 ------------------

    def _load_site_records(self) -> List[Dict[str, Any]]:
        with SessionFactory() as db:
            rows = db.execute(text("SELECT id, name, is_active FROM site")).fetchall()
        out = []
        for r in rows:
            out.append({"id": int(r[0]), "name": str(r[1]), "is_active": bool(r[2])})
        return out

    def _resolve_site_ids(self, ids: Optional[List[int]] = None, names: Optional[List[str]] = None) -> List[int]:
        ids = ids or []
        names = names or []

        site_records = self._load_site_records()
        by_name = {s["name"]: s for s in site_records}
        by_id = {s["id"]: s for s in site_records}

        final_ids: List[int] = []
        for sid in ids:
            if sid in by_id:
                final_ids.append(sid)
            else:
                logger.warning(f"站点ID不存在：id={sid}（将跳过）")

        for nm in names:
            rec = by_name.get(nm)
            if not rec:
                logger.warning(f"站点名称不存在：name={nm}（将跳过）")
                continue
            final_ids.append(int(rec["id"]))

        seen = set()
        uniq = []
        for x in final_ids:
            if x not in seen:
                seen.add(x)
                uniq.append(x)

        mapped = []
        for x in uniq:
            rec = by_id.get(x, {})
            mapped.append(f"{rec.get('name','?')}({x})")
        logger.info(f"订阅站点解析结果：ids={uniq} | 映射={mapped}")
        return uniq

    def _ensure_115_site_id(self, db=None) -> int:
        """
        确保 115网盘 站点存在并返回 ID
        :param db: 可选的数据库会话，若未传入则创建新会话
        """
        def _do_ensure(session):
            row = session.execute(text("SELECT id FROM site WHERE name=:n LIMIT 1"), {"n": "115网盘"}).fetchone()
            if row and row[0] is not None:
                return int(row[0])

            # existing = Site.get(session, -1)
            row_ex = session.execute(text("SELECT id FROM site WHERE id=:i"), {"i": -1}).fetchone()
            if not row_ex:
                session.execute(
                    text(
                        "INSERT INTO site (id, name, url, is_active, limit_interval, limit_count, limit_seconds, timeout) "
                        "VALUES (:id, :name, :url, :is_active, :limit_interval ,:limit_count, :limit_seconds, :timeout)"
                    ),
                    {
                        "id": -1,
                        "name": "115网盘",
                        "url": "https://115.com",
                        "is_active": True,
                        "limit_interval": 10000000,
                        "limit_count": 1,
                        "limit_seconds": 10000000,
                        "timeout": 1
                    }
                )
                session.commit()
                logger.info("已插入站点记录：115网盘(id=-1)")
            return -1

        if db is not None:
            return _do_ensure(db)
        else:
            with SessionFactory() as new_db:
                return _do_ensure(new_db)

    def _is_subscribe_excluded(self, subscribe_id: int) -> bool:
        """
        按订阅过滤模式判断订阅是否不归本插件处理

        - exclude 排除模式：勾选的订阅被排除，其余全部处理
        - include 指定模式：仅处理勾选的订阅，其余全部排除
        """
        if self._subscribe_filter_mode == "include":
            return subscribe_id not in set(self._include_subscribes or [])
        return subscribe_id in set(self._exclude_subscribes or [])

    def _apply_sites_to_all_subscribes(self, site_ids: List[int], reason: str):
        """ 应用站点ID到所有订阅 """
        with SessionFactory() as db:
            # 复用 SubscribeOper 实例，避免循环中重复创建
            subscribe_oper = SubscribeOper(db=db)
            subs = subscribe_oper.list() or []
            updated = 0
            excluded = 0
            for s in subs:
                if self._is_subscribe_excluded(s.id):
                    excluded += 1
                    continue
                subscribe_oper.update(s.id, {"sites": site_ids})
                updated += 1
        logger.info(f"{reason}：已更新 {updated} 个订阅（跳过 {excluded} 个排除订阅）")

    # ------------------ 取消屏蔽时间段判断 ------------------

    def _is_time_in_unblock(self, time_str: str = None) -> bool:
        """
        判断指定时间（或当前时间）是否在取消屏蔽时间段内。
        仅在 block_system_subscribe=OFF 时生效。
        支持跨天时段（如 22:00 ~ 06:00）。
        """
        if self._block_system_subscribe:
            # 屏蔽系统订阅开启时，不按时间段判断（始终接管）
            return False
        if not self._unblock_start_time or not self._unblock_end_time:
            return False

        tz = pytz.timezone(settings.TZ)
        now = datetime.datetime.now(tz=tz).strftime("%H:%M")
        check = time_str or now

        u_start = self._unblock_start_time.strip()
        u_end = self._unblock_end_time.strip()

        if u_start < u_end:
            return u_start <= check <= u_end
        else:
            return check >= u_start or check <= u_end




    def _backup_and_enter_blocked(self, reason: str = ""):
        """
        备份所有订阅的原始站点并强制设为仅115网盘。
        - 备份数据通过 save_data 持久化，跨重启不丢失
        - 仅首次进入接管态时备份（_is_blocked=False），不重复覆盖
        """
        self._init_subscribe_handler()
        tz = pytz.timezone(settings.TZ)
        now_str = datetime.datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")

        # 备份当前所有非-1订阅的站点（已屏蔽的不重复备份）
        if not self._is_blocked:
            backup = {}
            with SessionFactory() as db:
                from app.db.subscribe_oper import SubscribeOper
                subs = SubscribeOper(db=db).list()
                for s in (subs or []):
                    if not self._is_subscribe_excluded(s.id):
                        try:
                            sites = getattr(s, "sites", None)
                            if sites is not None:
                                backup[str(s.id)] = sites
                        except Exception:
                            pass
            if backup:
                self.save_data("subscribe_sites_backup", backup)
                logger.info(f"订阅站点备份：已保存 {len(backup)} 个订阅的原始站点")

        # 强制设为仅115
        self._subscribe_handler.set_blocked_sites_only_115()
        self._is_blocked = True
        self._block_system_subscribe = True
        self.__update_config()
        logger.info(f"已接管系统订阅（仅115网盘）：{reason or '时间到达接管时段'}")

    def _restore_and_exit_blocked(self, reason: str = ""):
        """
        从备份恢复所有订阅的原始站点。
        - 读取 save_data 持久化的备份
        - 恢复后清除备份（下次接管重新备份）
        """
        self._init_subscribe_handler()

        backup = self.get_data("subscribe_sites_backup") or {}
        if not backup:
            logger.warning("订阅站点备份为空，无法恢复原始站点")
            # 即使无备份也要切回非接管态，恢复为无限制（所有站点可用）
            self._is_blocked = False
            self._block_system_subscribe = False
            with SessionFactory() as db:
                from app.db.subscribe_oper import SubscribeOper
                oper = SubscribeOper(db=db)
                subs = oper.list() or []
                cleared = 0
                for s in subs:
                    try:
                        sites = getattr(s, "sites", None)
                        # 跳过已排除的订阅和 sites 不是 [-1] 的订阅
                        if self._is_subscribe_excluded(s.id):
                            continue
                        if sites == [-1]:
                            oper.update(s.id, {"sites": None})
                            cleared += 1
                    except Exception:
                        pass
            self.__update_config()
            msg = f"已退出接管：{cleared} 个订阅恢复为无限制（无备份可恢复，使用默认站点）"
            logger.info(f"{msg}（{reason or '时间到达接管结束'}）")
            if self._notify:
                import datetime as _dt
                import pytz as _tz
                _now = _dt.datetime.now(_tz.timezone(settings.TZ)).strftime("%H:%M")
                self._post_message(
                    mtype=NotificationType.Plugin,
                    title="✅ 已退出115网盘接管",
                    text=f"所有订阅已恢复为默认站点（无限制）\n"
                          f"恢复订阅：{cleared} 个\n"
                          f"当前时间：{_now}\n"
                          f"原因：{reason or '到达取消屏蔽时段'}\n"
                          f"⚠️ 无原始备份，恢复为无限制模式"
                )
            return

        # 检查备份是否被污染（全为 [-1]）
        all_minus_one = all(v == [-1] for v in backup.values())
        if all_minus_one:
            logger.warning("订阅站点备份已被污染（全为[-1]），清除备份并使用默认站点")
            self.del_data("subscribe_sites_backup")
            self._is_blocked = False
            self._block_system_subscribe = False
            with SessionFactory() as db:
                from app.db.subscribe_oper import SubscribeOper
                oper = SubscribeOper(db=db)
                subs = oper.list() or []
                cleared = 0
                for s in subs:
                    try:
                        if not self._is_subscribe_excluded(s.id):
                            sites = getattr(s, "sites", None)
                            if sites == [-1]:
                                oper.update(s.id, {"sites": None})
                                cleared += 1
                    except Exception:
                        pass
            self.__update_config()
            logger.info(f"已退出接管：{cleared} 个订阅恢复为无限制（备份被污染，使用默认站点）")
            if self._notify:
                import datetime as _dt
                import pytz as _tz
                _now = _dt.datetime.now(_tz.timezone(settings.TZ)).strftime("%H:%M")
                self._post_message(
                    mtype=NotificationType.Plugin,
                    title="✅ 已退出115网盘接管（备份修复）",
                    text=f"订阅原始备份已被污染（全为[-1]）\n"
                          f"已清除污染备份并恢复为默认站点\n"
                          f"恢复订阅：{cleared} 个\n"
                          f"当前时间：{_now}"
                )
            return

        with SessionFactory() as db:
            from app.db.subscribe_oper import SubscribeOper
            oper = SubscribeOper(db=db)
            restored = 0
            for sid_str, site_ids in backup.items():
                try:
                    sid = int(sid_str)
                    # 跳过已经不存在的订阅
                    sub = oper.get(sid)
                    if not sub:
                        continue
                    oper.update(sid, {"sites": site_ids})
                    restored += 1
                except Exception as e:
                    logger.warning(f"恢复订阅 {sid_str} 站点失败：{e}")

        self.del_data("subscribe_sites_backup")
        self._is_blocked = False
        self._block_system_subscribe = False
        self.__update_config()
        logger.info(f"已退出接管：恢复 {restored} 个订阅的原始站点（{reason or '时间到达接管结束'}）")

    def _apply_block_by_time(self):
        """
        根据屏蔽开关和时间段检查是否需要接管/退出接管。
        由 sync_subscribes 和 init_plugin 调用。

        规则：
        - 屏蔽系统订阅 = 开启 → 始终接管（115网盘接管模式）
        - 屏蔽系统订阅 = 关闭 → 按取消屏蔽时间段：
          - 在取消屏蔽时段内 → 恢复用户原始站点
          - 不在取消屏蔽时段内 → 强制仅115网盘
        """
        if self._block_system_subscribe:
            # 屏蔽开启 → 始终接管
            if not self._is_blocked:
                self._backup_and_enter_blocked(reason="屏蔽开关已开启")
            else:
                logger.debug("屏蔽开启：已在接管态")
        else:
            # 屏蔽关闭 → 按取消屏蔽时间段判断
            if self._is_time_in_unblock():
                if self._is_blocked:
                    self._restore_and_exit_blocked(reason="进入取消屏蔽时段")
                else:
                    logger.debug("取消屏蔽时段内：已处于非接管态")
            else:
                if not self._is_blocked:
                    self._backup_and_enter_blocked(reason="非取消屏蔽时段")
                else:
                    logger.debug("非取消屏蔽时段：已在接管态")

    def _apply_best_version_all(self):
        """根据 _auto_best_version 开关，批量开启/关闭所有电视剧订阅的 best_version"""
        value = 1 if self._auto_best_version else 0
        action = "开启" if value else "关闭"
        with SessionFactory() as db:
            from app.db.subscribe_oper import SubscribeOper
            subscribe_oper = SubscribeOper(db=db)
            subs = subscribe_oper.list() or []
            updated = 0
            for s in subs:
                if s.type == MediaType.TV.value:
                    current = bool(getattr(s, "best_version", False))
                    if current != bool(value):
                        subscribe_oper.update(s.id, {"best_version": value})
                        updated += 1
        if updated:
            logger.info(f"[原生洗版] 已{action} {updated} 个电视剧订阅的原始洗版(best_version={value})")
        else:
            logger.info(f"[原生洗版] 所有电视剧订阅 best_version 已是 {value}，无需变更")

    # ------------------ 事件兜底：SubscribeAdded 保留，SubscribeModified 禁用写入 ------------------

    def _get_subscribe_id_from_event(self, event: Event) -> Optional[int]:
        if not event or not event.event_data:
            return None
        data = event.event_data or {}
        subscribe_id = data.get("subscribe_id") or data.get("id")
        if not subscribe_id and isinstance(data.get("subscribe"), dict):
            subscribe_id = data["subscribe"].get("id")
        try:
            return int(subscribe_id) if subscribe_id is not None else None
        except Exception:
            return None

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event: Event):
        """
        保留：新订阅兜底
        - 已屏蔽系统订阅时：新订阅必拉回仅115
        - 已恢复系统订阅时：新订阅同步窗口站点（保持一致）
        """
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        if self._is_subscribe_excluded(sid):
            logger.info(f"新增订阅不在本插件处理范围（订阅过滤模式：{self._subscribe_filter_mode}），跳过站点同步（subscribe_id={sid}）")
            return
        try:
            self._init_subscribe_handler()

            if not self._is_time_in_unblock():
                # 非取消屏蔽时段：新订阅强制设为仅115
                if hasattr(self._subscribe_handler, "set_sites_for_subscribe_only_115"):
                    self._subscribe_handler.set_sites_for_subscribe_only_115(sid)
                else:
                    with SessionFactory() as db:
                        site_id_115 = self._ensure_115_site_id(db)
                        SubscribeOper(db=db).update(sid, {"sites": [site_id_115]})
                logger.info(f"非取消屏蔽时段：新增订阅已拉回仅115（subscribe_id={sid}）")
            else:
                # 取消屏蔽时段：不干预用户选择
                logger.info(f"取消屏蔽时段：新增订阅保持用户原始站点（subscribe_id={sid}）")

        except Exception as e:
            logger.error(f"SubscribeAdded 兜底失败：{e}")

    @eventmanager.register(EventType.SubscribeModified)
    def on_subscribe_modified(self, event: Event):
        """
        禁用：不再对 subscribe.modified 做拉回写入
        目的：用户手动修改订阅站点时，不再被自动拉回仅115
        """
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        if not self._is_time_in_unblock():
            logger.info(f"非取消屏蔽时段：检测到订阅改动，不自动拉回以避免覆盖用户操作（subscribe_id={sid}）")
        return

    # ------------------ init_plugin ------------------

    def init_plugin(self, config: dict = None):
        self.stop_service()
        download_so_file(Path(__file__).parent / "lib")

        if config:
            self._enabled = config.get("enabled", False)

            self._cron = (config.get("cron", self._cron) or "").strip()
            if self._cron:
                if not self._cron_is_valid(self._cron):
                    logger.warning(
                        f"Cron 表达式无效：{self._cron}，已回退默认 0 18-23 * * *"
                    )
                    self._cron = "0 18-23 * * *"

            self._notify = config.get("notify", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cookies = config.get("cookies", "")

            self._pansou_enabled = config.get("pansou_enabled", True)
            self._pansou_url = config.get("pansou_url", "https://so.252035.xyz/")
            self._pansou_username = config.get("pansou_username", "")
            self._pansou_password = config.get("pansou_password", "")
            self._pansou_auth_enabled = config.get("pansou_auth_enabled", False)
            self._pansou_channels = config.get("pansou_channels", "QukanMovie")

            self._save_path = config.get("save_path", "/我的接收/MoviePilot/TV")
            self._movie_save_path = config.get("movie_save_path", "/我的接收/MoviePilot/Movie")
            self._only_115 = config.get("only_115", True)
            self._subscribe_filter_mode = config.get("subscribe_filter_mode", "exclude") or "exclude"
            self._exclude_subscribes = config.get("exclude_subscribes", []) or []
            self._include_subscribes = config.get("include_subscribes", []) or []
            # 全局兜底排除规则（硬拒绝型）—— 默认杜绝杜比视界
            _ge = config.get("global_exclude", None)
            if _ge:
                self._global_exclude = _ge
            logger.info(f"P115StrgmSub 全局兜底 exclude: {self._global_exclude}")
            if self._subscribe_filter_mode == "include":
                logger.info(f"订阅过滤模式：指定模式，仅处理 {len(self._include_subscribes)} 个勾选订阅")

            self._nullbr_enabled = config.get("nullbr_enabled", False)
            self._nullbr_appid = config.get("nullbr_appid", "")
            self._nullbr_api_key = config.get("nullbr_api_key", "")

            self._hdhive_enabled = config.get("hdhive_enabled", False)
            self._hdhive_query_mode = config.get("hdhive_query_mode", "api")
            self._hdhive_api_key = (config.get("hdhive_api_key", "") or "").strip()
            self._hdhive_client_id = (config.get("hdhive_client_id", "") or "").strip()
            self._hdhive_redirect_uri = (config.get("hdhive_redirect_uri", "") or "").strip()
            self._hdhive_auth_code = (config.get("hdhive_auth_code", "") or "").strip()
            self._hdhive_access_token = config.get("hdhive_access_token", "")
            self._hdhive_refresh_token = config.get("hdhive_refresh_token", "")
            self._hdhive_token_expires_at = float(config.get("hdhive_token_expires_at", 0) or 0)
            self._hdhive_auto_unlock = config.get("hdhive_auto_unlock", False)
            self._hdhive_max_unlock_points = int(config.get("hdhive_max_unlock_points", 50) or 50)
            self._hdhive_max_points_per_sub = int(config.get("hdhive_max_points_per_sub", 20) or 20)
            self._hdhive_username = config.get("hdhive_username", "")
            self._hdhive_password = config.get("hdhive_password", "")
            self._hdhive_cookie = config.get("hdhive_cookie", "")
            self._hdhive_auto_refresh = config.get("hdhive_auto_refresh", False)
            self._hdhive_refresh_before = int(config.get("hdhive_refresh_before", 86400) or 86400)
            self._max_transfer_per_sync = int(config.get("max_transfer_per_sync", 50) or 50)
            self._batch_size = int(config.get("batch_size", 20) or 20)
            self._skip_other_season_dirs = config.get("skip_other_season_dirs", True)

            # 搜索源优先级（兼容逗号分隔字符串）
            raw_order = config.get("search_source_order", []) or []
            if isinstance(raw_order, str):
                self._search_source_order = [x.strip() for x in raw_order.split(",") if x.strip()]
            else:
                self._search_source_order = list(raw_order)
            if self._search_source_order:
                logger.info(f"搜索源自定义优先级：{' > '.join(self._search_source_order)}")

            # 洗版配置
            self._auto_best_version = bool(config.get("auto_best_version", False))
            self._min_upgrade_tiers = int(config.get("min_upgrade_tiers", 2))
            self._self_heal_interval = int(config.get("self_heal_interval", 10))
            self._enable_cloud_upgrade = bool(config.get("enable_cloud_upgrade", False))
            self._enable_pt_upgrade = bool(config.get("enable_pt_upgrade", False))
            self._cloud_tv_local_dir = str(config.get("cloud_tv_local_dir", "") or "")
            self._cloud_tv_remote_dir = str(config.get("cloud_tv_remote_dir", "") or "")
            self._cloud_movie_local_dir = str(config.get("cloud_movie_local_dir", "") or "")
            self._cloud_movie_remote_dir = str(config.get("cloud_movie_remote_dir", "") or "")

            # 取消屏蔽时间段配置
            self._unblock_start_time = str(config.get("unblock_start_time", self._unblock_start_time) or self._unblock_start_time)
            self._unblock_end_time = str(config.get("unblock_end_time", self._unblock_end_time) or self._unblock_end_time)

            self._block_system_subscribe = bool(config.get("block_system_subscribe", False))

        # 初始化客户端/handlers
        self._init_clients()
        self._init_handlers()

        # 配置立即生效：根据当前时间检查接管态
        self._apply_block_by_time()
        self._apply_best_version_all()
        logger.info(f"插件初始化：取消屏蔽时段={self._unblock_start_time}~{self._unblock_end_time}, 洗版={'开启' if self._auto_best_version else '关闭'}, 当前接管态={self._is_blocked}")

        # 立即运行一次
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(
                    func=self.sync_subscribes,
                    trigger='date',
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                )
                if self._scheduler.get_jobs():
                    self._scheduler.start()

            if self._onlyonce:
                self._onlyonce = False
                self.__update_config()

    # ------------------ init clients/handlers ------------------

    def _init_clients(self):
        """初始化客户端"""
        proxy = settings.PROXY
        if proxy:
            logger.info(f"使用 MoviePilot PROXY: {proxy}")

        if self._pansou_enabled and self._pansou_url:
            self._pansou_client = PanSouClient(
                base_url=self._pansou_url,
                username=self._pansou_username,
                password=self._pansou_password,
                auth_enabled=self._pansou_auth_enabled,
                proxy=proxy
            )

        if self._nullbr_enabled:
            if not self._nullbr_appid or not self._nullbr_api_key:
                missing = []
                if not self._nullbr_appid:
                    missing.append("APP ID")
                if not self._nullbr_api_key:
                    missing.append("API Key")
                logger.warning(f"Nullbr 已启用但缺少必要配置：{', '.join(missing)}，将无法使用 Nullbr 查询功能")
                self._nullbr_client = None
            else:
                self._nullbr_client = NullbrClient(app_id=self._nullbr_appid, api_key=self._nullbr_api_key, proxy=proxy)
                logger.info("Nullbr 客户端初始化成功")

        # HDHive OpenAPI 客户端初始化（API 模式搜索/解锁共用；Playwright 模式搜索时动态创建浏览器客户端）
        self._init_hdhive_openapi_client(proxy)
        if self._hdhive_enabled:
            if self._hdhive_query_mode == "playwright" and (not self._hdhive_username or not self._hdhive_password):
                logger.warning("HDHive (Playwright 模式) 已启用但未配置用户名和密码，将无法使用 HDHive 查询功能")
            elif self._hdhive_query_mode == "api" and (not self._hdhive_client or not self._hdhive_client.is_ready):
                logger.warning("HDHive (API 模式) 已启用但未完成 OpenAPI 应用配置和用户授权，将无法使用 HDHive 查询功能")
            else:
                logger.info(f"HDHive 配置已加载（模式：{self._hdhive_query_mode}）")

        if self._cookies:
            self._p115_manager = P115ClientManager(cookies=self._cookies)

    # ------------------ HDHive OpenAPI ------------------

    def _on_hdhive_token_update(self, tokens: Dict[str, Any]):
        """Token 刷新后持久化到插件配置"""
        self._hdhive_access_token = tokens.get("access_token", "")
        self._hdhive_refresh_token = tokens.get("refresh_token", "")
        self._hdhive_token_expires_at = float(tokens.get("token_expires_at", 0) or 0)
        self.__update_config()

    def _init_hdhive_openapi_client(self, proxy=None):
        """
        初始化 HDHive OpenAPI 客户端，并处理一次性授权码换 Token

        新版接入模型：
        1. 在 HDHive 创建 OpenAPI 应用，审核通过后获得 client_id 和应用 Secret
        2. 配置 client_id、应用 Secret、回调地址后保存，从日志中复制授权链接到浏览器完成授权
        3. 将回调地址中的 code 参数填入"授权码"并保存，插件自动换取用户 Token
        """
        self._hdhive_client = None
        if not self._hdhive_api_key:
            return

        client = HDHiveOpenAPIClient(
            app_secret=self._hdhive_api_key,
            client_id=self._hdhive_client_id,
            access_token=self._hdhive_access_token,
            refresh_token=self._hdhive_refresh_token,
            token_expires_at=self._hdhive_token_expires_at,
            proxy=proxy,
            on_token_update=self._on_hdhive_token_update,
        )
        self._hdhive_client = client

        # 一次性授权码换取用户 Token
        if self._hdhive_auth_code:
            auth_code = self._hdhive_auth_code
            self._hdhive_auth_code = ""
            if not self._hdhive_redirect_uri:
                logger.error("HDHive OpenAPI: 已填写授权码但缺少回调地址（必须与发起授权时一致），无法换取 Token")
                self.__update_config()
            else:
                try:
                    data = client.exchange_code(auth_code, self._hdhive_redirect_uri)
                    scopes = data.get("scope") or " ".join(data.get("scopes") or [])
                    logger.info(f"HDHive OpenAPI: 用户授权成功，已获取 Access Token（scope: {scopes}）")
                    self.__update_config()
                except HDHiveOpenAPIError as e:
                    logger.error(f"HDHive OpenAPI: 授权码换取 Token 失败: [{e.code}] {e.message} {e.description}")
                    self.__update_config()
                except Exception as e:
                    logger.error(f"HDHive OpenAPI: 授权码换取 Token 异常: {e}")
                    self.__update_config()

        # 未完成授权时，打印授权链接引导用户操作
        if not client.is_ready:
            if self._hdhive_client_id and self._hdhive_redirect_uri:
                authorize_url = client.build_authorize_url(self._hdhive_redirect_uri)
                logger.warning(
                    f"HDHive OpenAPI: 尚未完成用户授权，请在浏览器打开以下链接完成授权，"
                    f"然后将回调地址中的 code 参数填入插件配置的「授权码」并保存：\n{authorize_url}"
                )
            else:
                logger.warning("HDHive OpenAPI: 请先在 HDHive 申请 OpenAPI 应用，并在插件中配置 Client ID、应用 Secret 和回调地址")

    def _init_subscribe_handler(self):
        self._subscribe_handler = SubscribeHandler(
            exclude_subscribes=self._exclude_subscribes,
            notify=self._notify,
            post_message_func=self.post_message,
            is_excluded_func=self._is_subscribe_excluded
        )

    def _init_handlers(self):
        self._init_subscribe_handler()

        self._search_handler = SearchHandler(
            pansou_client=self._pansou_client,
            nullbr_client=self._nullbr_client,
            hdhive_client=self._hdhive_client,
            pansou_enabled=self._pansou_enabled,
            nullbr_enabled=self._nullbr_enabled,
            hdhive_enabled=self._hdhive_enabled,
            hdhive_query_mode=self._hdhive_query_mode,
            hdhive_auto_unlock=self._hdhive_auto_unlock,
            hdhive_max_unlock_points=self._hdhive_max_unlock_points,
            hdhive_max_points_per_sub=self._hdhive_max_points_per_sub,
            hdhive_username=self._hdhive_username,
            hdhive_password=self._hdhive_password,
            hdhive_cookie=self._hdhive_cookie,
            only_115=self._only_115,
            pansou_channels=self._pansou_channels,
            search_source_order=self._search_source_order
        )
        # 设置持久化函数，用于保存订阅的历史积分花费
        self._search_handler.set_data_funcs(self.get_data, self.save_data)

        self._sync_handler = SyncHandler(
            p115_manager=self._p115_manager,
            search_handler=self._search_handler,
            subscribe_handler=self._subscribe_handler,
            chain=self.chain,
            save_path=self._save_path,
            movie_save_path=self._movie_save_path,
            max_transfer_per_sync=self._max_transfer_per_sync,
            batch_size=self._batch_size,
            skip_other_season_dirs=self._skip_other_season_dirs,
            notify=self._notify,
            post_message_func=self.post_message,
            get_data_func=self.get_data,
            save_data_func=self.save_data,
            global_exclude=self._global_exclude,
            min_upgrade_tiers=self._min_upgrade_tiers,
            self_heal_interval=self._self_heal_interval,
            enable_cloud_upgrade=self._enable_cloud_upgrade,
            enable_pt_upgrade=self._enable_pt_upgrade,
            auto_best_version=self._auto_best_version,
            cloud_tv_local_dir=self._cloud_tv_local_dir,
            cloud_tv_remote_dir=self._cloud_tv_remote_dir,
            cloud_movie_local_dir=self._cloud_movie_local_dir,
            cloud_movie_remote_dir=self._cloud_movie_remote_dir
        )

        self._api_handler = ApiHandler(
            pansou_client=self._pansou_client,
            p115_manager=self._p115_manager,
            only_115=self._only_115,
            save_path=self._save_path,
            get_data_func=self.get_data,
            save_data_func=self.save_data
        )

    # ------------------ 配置写回 ------------------

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "cron": self._cron,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "only_115": self._only_115,
            "save_path": self._save_path,
            "movie_save_path": self._movie_save_path,
            "cookies": self._cookies,
            "pansou_enabled": self._pansou_enabled,
            "pansou_url": self._pansou_url,
            "pansou_username": self._pansou_username,
            "pansou_password": self._pansou_password,
            "pansou_auth_enabled": self._pansou_auth_enabled,
            "pansou_channels": self._pansou_channels,
            "nullbr_enabled": self._nullbr_enabled,
            "nullbr_appid": self._nullbr_appid,
            "nullbr_api_key": self._nullbr_api_key,
            # HDHive 配置
            "hdhive_enabled": self._hdhive_enabled,
            "hdhive_query_mode": self._hdhive_query_mode,
            "hdhive_api_key": self._hdhive_api_key,
            "hdhive_client_id": self._hdhive_client_id,
            "hdhive_redirect_uri": self._hdhive_redirect_uri,
            "hdhive_auth_code": self._hdhive_auth_code,
            "hdhive_access_token": self._hdhive_access_token,
            "hdhive_refresh_token": self._hdhive_refresh_token,
            "hdhive_token_expires_at": self._hdhive_token_expires_at,
            "hdhive_auto_unlock": self._hdhive_auto_unlock,
            "hdhive_max_unlock_points": self._hdhive_max_unlock_points,
            "hdhive_max_points_per_sub": self._hdhive_max_points_per_sub,
            "hdhive_username": self._hdhive_username,
            "hdhive_password": self._hdhive_password,
            "hdhive_cookie": self._hdhive_cookie,
            "hdhive_auto_refresh": self._hdhive_auto_refresh,
            "hdhive_refresh_before": self._hdhive_refresh_before,
            # 其他配置
            "search_source_order": self._search_source_order,
            "subscribe_filter_mode": self._subscribe_filter_mode,
            "exclude_subscribes": self._exclude_subscribes,
            "include_subscribes": self._include_subscribes,
            "global_exclude": self._global_exclude,
            "block_system_subscribe": self._block_system_subscribe,
            "block_start_time": self._unblock_start_time,
            "block_end_time": self._unblock_end_time,
            "auto_best_version": self._auto_best_version,
            "unblock_start_time": self._unblock_start_time,
            "unblock_end_time": self._unblock_end_time,
            "max_transfer_per_sync": self._max_transfer_per_sync,
            "batch_size": self._batch_size,
            "skip_other_season_dirs": self._skip_other_season_dirs,
            "enable_cloud_upgrade": self._enable_cloud_upgrade,
            "enable_pt_upgrade": self._enable_pt_upgrade,
            "cloud_tv_local_dir": self._cloud_tv_local_dir,
            "cloud_tv_remote_dir": self._cloud_tv_remote_dir,
            "cloud_movie_local_dir": self._cloud_movie_local_dir,
            "cloud_movie_remote_dir": self._cloud_movie_remote_dir,
            "min_upgrade_tiers": self._min_upgrade_tiers,
            "self_heal_interval": self._self_heal_interval,
        })

    # ------------------ stop ------------------

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception:
            pass

        try:
            if self._toggle_scheduler:
                self._toggle_scheduler.remove_all_jobs()
                if self._toggle_scheduler.running:
                    self._toggle_scheduler.shutdown()
                self._toggle_scheduler = None
        except Exception:
            pass

    # ======================================================================
    # 必备：get_state / get_form / get_page / get_api / get_service
    # ======================================================================

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return UIConfig.get_form()

    def get_page(self) -> Optional[List[dict]]:
        history = self.get_data('history') or []
        return UIConfig.get_page(history)

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/sync_subscribes",
                "endpoint": self.sync_subscribes,
                "methods": ["GET"],
                "summary": "执行同步订阅追更"
            },
            {
                "path": "/clear_history",
                "endpoint": self.api_clear_history,
                "methods": ["POST"],
                "summary": "清空历史记录"
            }
        ]
    
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程控制命令"""
        return [{
            "cmd": "/p115_sub_action",
            "event": EventType.PluginAction,
            "desc": "115网盘订阅追更",
            "category": "订阅",
            "data": {
                "action": "p115_sub_action"
            }
        }]


    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []

        services = []

        if self._cron and self._cron_is_valid(self._cron):
            try:
                services.append({
                    "id": "P115StrgmSub",
                    "name": "115网盘订阅追更服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_subscribes,
                    "kwargs": {}
                })
            except Exception as e:
                logger.warning(f"Cron 表达式无效：{self._cron}，将回退默认 0 18-23 * * *。错误：{e}")
                services.append({
                    "id": "P115StrgmSub",
                    "name": "115网盘订阅追更服务",
                    "trigger": CronTrigger.from_crontab("0 18-23 * * *"),
                    "func": self.sync_subscribes,
                    "kwargs": {}
                })
        else:
            services.append({
                "id": "P115StrgmSub",
                "name": "115网盘订阅追更服务",
                "trigger": CronTrigger.from_crontab("0 18-23 * * *"),
                "func": self.sync_subscribes,
                "kwargs": {}
            })

        return services

    # ======================================================================
    # 必备：_do_sync（返回 bool）
    # ======================================================================

    def _do_sync(self) -> bool:
        # 至少启用一个搜索源
        if not self._pansou_enabled and not self._nullbr_enabled and not self._hdhive_enabled:
            logger.error("搜索源均未启用（PanSou/Nullbr/HDHive），无法执行")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】配置错误",
                    text="PanSou、Nullbr、HDHive 均未启用，请至少启用一个搜索源。"
                )
            return False

        # 115 客户端检查
        if not self._p115_manager:
            logger.error("115 客户端未初始化，请检查 Cookie 配置")
            return False

        if not self._p115_manager.check_login():
            logger.error("115 登录失败，Cookie 可能已过期")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title="【115网盘订阅追更】登录失败",
                    text="115 Cookie 可能已过期，请更新后重试。"
                )
            return False

        logger.info("开始执行 115 网盘订阅同步...")
        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【115网盘订阅追更】开始执行",
                text="正在扫描订阅列表并同步缺失内容..."
            )

        # reset api counters
        try:
            self._p115_manager.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._pansou_client:
                self._pansou_client.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._nullbr_client:
                self._nullbr_client.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._search_handler:
                self._search_handler.reset_task_spent_points()
        except Exception:
            pass

        # 获取订阅
        with SessionFactory() as db:
            subscribes = SubscribeOper(db=db).list("N,R")

        if not subscribes:
            logger.info("无订阅数据")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】执行完成",
                    text="当前无订阅数据。"
                )
            return True

        tv_subscribes = [s for s in subscribes if s.type == MediaType.TV.value]
        movie_subscribes = [s for s in subscribes if s.type == MediaType.MOVIE.value]

        if not tv_subscribes and not movie_subscribes:
            logger.info("无电影/剧集订阅")
            return True

        history: List[dict] = self.get_data('history') or []
        transfer_details: List[Dict[str, Any]] = []
        transferred_count = 0

        exclude_ids = set(self._exclude_subscribes or [])
        skipped_count = 0

        # 处理电影
        for subscribe in movie_subscribes:
            if global_vars.is_system_stopped:
                break
            if self._is_subscribe_excluded(subscribe.id):
                skipped_count += 1
                continue
            transferred_count = self._sync_handler.process_movie_subscribe(
                subscribe=subscribe,
                history=history,
                transfer_details=transfer_details,
                transferred_count=transferred_count
            )

        # 处理剧集
        for subscribe in tv_subscribes:
            if global_vars.is_system_stopped:
                break
            if self._is_subscribe_excluded(subscribe.id):
                skipped_count += 1
                continue
            transferred_count = self._sync_handler.process_tv_subscribe(
                subscribe=subscribe,
                history=history,
                transfer_details=transfer_details,
                transferred_count=transferred_count,
                exclude_ids=exclude_ids
            )

        if skipped_count:
            mode_label = "指定模式" if self._subscribe_filter_mode == "include" else "排除模式"
            logger.info(f"订阅过滤（{mode_label}）：本次跳过 {skipped_count} 个不在处理范围的订阅")

        self.save_data('history', history)

        logger.info(f"115 网盘订阅同步完成，共转存 {transferred_count} 个文件")

        if self._notify:
            if transferred_count > 0:
                self._sync_handler.send_transfer_notification(transfer_details, transferred_count)
            else:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】执行完成",
                    text="本次同步未发现需要转存的新资源。"
                )

        return True

    # ------------------ API包装（用于 get_api） ------------------

    def api_clear_history(self, apikey: str) -> dict:
        return self._api_handler.clear_history(apikey)

    # ------------------ 同步入口（触发条件1） ------------------

    def sync_subscribes(self):
        with lock:
            tz = pytz.timezone(settings.TZ)
            run_start = datetime.datetime.now(tz=tz)

            success = False
            try:
                success = self._do_sync()
                # 洗版扫描：同步完成后执行
                if success and self._sync_handler:
                    if self._enable_cloud_upgrade:
                        self._sync_handler.auto_upgrade_scan(source='cloud')
                    if self._enable_pt_upgrade:
                        self._sync_handler.auto_upgrade_scan(source='pt')
            except Exception as e:
                logger.error(f"同步任务异常：{e}")
                success = False
            finally:
                # 同步完成后检查接管时段
                self._apply_block_by_time()

    # ------------------ 业务 API（保留） ------------------

    def api_search(self, keyword: str, apikey: str) -> dict:
        return self._api_handler.search(keyword, apikey)

    def api_transfer(self, share_url: str, save_path: str, apikey: str) -> dict:
        return self._api_handler.transfer(share_url, save_path, apikey)

    def api_list_directories(self, path: str = "/", apikey: str = "") -> dict:
        return self._api_handler.list_directories(path, apikey)

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "p115_sub_action":
            return

        logger.info("收到命令，开始执行追更任务")
        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【115网盘订阅追更】开始执行",
            text="已收到远程命令，正在执行追更任务...",
            userid=event_data.get("user")
        )

        self.sync_subscribes()

        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【115网盘订阅追更】执行完成",
            text="远程触发的追更任务已完成。",
            userid=event_data.get("user")
        )
