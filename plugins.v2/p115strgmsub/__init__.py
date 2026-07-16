"""
115网盘订阅追更插件
结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失剧集
"""
import datetime
import time
import threading
from pathlib import Path
from threading import Lock, Thread
from typing import Optional, Any, List, Dict, Tuple

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text

from app.core.config import settings, global_vars
from app.core.event import Event, eventmanager
from app.schemas.event import ResourceDownloadEventData
from app.core.module import ModuleManager
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper
from app.db.models.site import Site
from app.db.systemconfig_oper import SystemConfigOper
from app.log import logger
from app.plugins import _PluginBase
from app.chain.subscribe import SubscribeChain
from app.schemas.types import ChainEventType, EventType, MediaType, NotificationType, SystemConfigKey

from .clients import PanSouClient, P115ClientManager, NullbrClient, HDHiveOpenAPIClient, HDHiveOpenAPIError
from .handlers import SearchHandler, SyncHandler, SubscribeHandler, ApiHandler
from .ui import UIConfig
from .utils import download_so_file

lock = Lock()


class P115StrgmSub(_PluginBase):
    """115网盘订阅追更插件"""

    # 插件名称
    plugin_name = "115网盘订阅追更魔改版"
    # 插件描述
    plugin_desc = "结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失的电影和剧集。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    # 插件版本
    plugin_version = "1.7.1"
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

    # 重复通知缓存：{种子标题: 时间戳}，6小时内同种子不重复通知
    _notified_titles: Dict[str, float] = {}

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

    # TG 频道搜索配置
    _tg_enabled: bool = False
    _tg_bot_token: str = ""
    _tg_channel_ids: str = ""

    # TG 自动转发配置
    _tg_forward_enabled: bool = False
    _tg_forward_target: str = ""
    _tg_forwarder_thread = None
    _tg_forwarder_stop = threading.Event()
    _tg_shared_buffer = []  # 转发线程缓存的消息，供 TG 搜索读取（避免 getUpdates 冲突）
    _tg_shared_buffer_lock = Lock()

    # 是否屏蔽系统订阅（True=已屏蔽系统订阅，False=已恢复系统订阅）
    _block_system_subscribe: bool = False

    _max_transfer_per_sync: int = 50
    _batch_size: int = 20
    _skip_other_season_dirs: bool = True

    # 洗版配置
    _auto_best_version: bool = False
    _upgrade_subscribe_ids: list = []
    _last_scored_ids_hash: str = ""  # 上次评分过的ids hash值，用于保存配置时防重复触发
    _min_upgrade_tiers: int = 2
    _upgrade_threshold: int = 25
    _enable_cloud_upgrade: bool = False
    _enable_pt_upgrade: bool = False
    _last_pt_upgrade_time: float = 0.0  # 上次PT洗版扫描时间戳（TransferComplete事件防抖）
    _upgrade_debounce_seconds: int = 600  # PT洗版防抖间隔（秒），默认10分钟
    _cloud_tv_local_dir: str = ""  # 本地电视剧strm根目录（网盘洗版用）
    _cloud_tv_remote_dir: str = ""  # 115网盘电视剧目录（网盘洗版用）
    _cloud_movie_local_dir: str = ""  # 本地电影strm根目录（网盘洗版用）
    _cloud_movie_remote_dir: str = ""  # 115网盘电影目录（网盘洗版用）
    # MP自定义规则正则（同时用于网盘洗版评分）
    _frame_rate_pattern: str = r"60fps|120fps|50fps|60帧|120帧|50帧"
    _bit_rate_pattern: str = r"10bit|12bit|10-bit|12-bit"
    _vivid_pattern: str = r"HDR[._ ]?[Vv]ivid|菁彩影像|HDRVivid"
    # 自动注册MP过滤规则到系统
    _subscribe_auto_fill: bool = False
    # 新增订阅时自动填充的规则组（内置SubscribeGroup功能）
    # 多行文本格式：category | filter_group | include | exclude
    _subscribe_category_rules: str = ""
    _auto_register_rules: bool = False
    # 优先级规则组预设（none=保留用户现有, no_dovi=非杜比画质优先, dovi=含杜比, custom=自定义）
    _tv_rule_group_preset: str = "none"
    _tv_rule_group_custom: str = ""
    _movie_rule_group_preset: str = "none"
    _movie_rule_group_custom: str = ""
    # 命名规则模板（预设值=当前MP配置，可用系统变量：title/year/tmdbid/videoFormat/edition/audioCodec/videoCodec/releaseGroup/fileExt等）
    _tv_rename_format: str = "{{title}}{% if year %} ({{year}}){% endif %} {tmdbid={{tmdbid}}}/Season {{'%02d'|format(season|int)}}/{{title}}{% if year %} ({{year}}){% endif %} - {{season_episode}} - {% if episode_title %}{{episode_title}}{% else %}第 {{episode}} 集{% endif %} - {{videoFormat}}{% if edition %}.{{edition}}{% endif %}{% if hdr %}.{{hdr}}{% endif %}{% if videoCodec %}.{{videoCodec}}{% endif %}{% if audioCodec %}.{{audioCodec}}{% endif %}{% if releaseGroup %} - {{releaseGroup}}{% endif %}{{fileExt}}"
    _movie_rename_format: str = "{{title}}{% if year %} ({{year}}){% endif %} {tmdbid={{tmdbid}}}/{{title}}{% if year %} ({{year}}){% endif %}{% if videoFormat %} - {{videoFormat}}{% if edition %}.{{edition}}{% endif %}{% if audioCodec %}.{{audioCodec}}{% endif %}{% if videoCodec %}.{{videoCodec}}{% endif %}{% endif %}{% if releaseGroup %} - {{releaseGroup}}{% endif %}{{fileExt}}"
    _auto_apply_naming: bool = False
    # 屏蔽态时间段（block_system_subscribe=OFF 时生效，屏蔽态内保持[-1]不变）
    _block_start_time: str = "18:00"
    _block_end_time: str = "23:59"
    # 开放态时间段（block_system_subscribe=OFF 时生效，开放态内自动恢复用户站点）
    _unblock_start_time: str = "00:00"
    _unblock_end_time: str = "17:30"
    # 当前是否处于接管态（True=强制仅115，False=用户原始站点）
    _is_blocked: bool = False
    # 全局配置是否已应用（安装成功首次执行时才修改MP系统配置）
    _global_config_applied: bool = False

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

    @staticmethod
    def _apply_http_patches():
        """自动部署 115 API 重试补丁（WAF 405 + ConnectionReset 指数退避）"""
        try:
            from .patches import p115client_patch
            p115client_patch.apply()
            from .patches import httpcore_405_patch
            httpcore_405_patch.apply()
            from .patches import httpx_405_patch
            httpx_405_patch.apply()
            logger.info("✅ 115 API 重试补丁全部加载成功")
        except Exception as e:
            logger.warning(f"⚠️ 115 API 重试补丁加载失败: {e}")

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
                self.post_message(
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
                self.post_message(
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
        self.__update_config()
        logger.info(f"已退出接管：恢复 {restored} 个订阅的原始站点（{reason or '时间到达接管结束'}）")

        # 恢复站点后触发一次 PT 订阅搜索（非屏蔽态下让 MP 主动搜 PT）
        try:
            from app.chain.subscribe import SubscribeChain
            SubscribeChain().search(state="R", manual=True)
            logger.info("开放态：已触发 PT 订阅搜索（SubscribeChain.search）")
        except Exception as e:
            logger.warning(f"开放态触发 PT 搜索失败：{e}")

    def _is_time_in_block(self, time_str: str = None) -> bool:
        """
        判断指定时间（或当前时间）是否在屏蔽时间段内。
        仅在 block_system_subscribe=OFF 时生效。
        支持跨天时段（如 22:00 ~ 06:00）。
        """
        if self._block_system_subscribe:
            return False
        if not self._block_start_time or not self._block_end_time:
            return False

        tz = pytz.timezone(settings.TZ)
        now = datetime.datetime.now(tz=tz).strftime("%H:%M")
        check = time_str or now

        b_start = self._block_start_time.strip()
        b_end = self._block_end_time.strip()

        if b_start < b_end:
            return b_start <= check <= b_end
        else:
            return check >= b_start or check <= b_end

    def _apply_block_by_time(self):
        """
        根据屏蔽开关和时间段检查是否需要接管/退出接管。
        由 sync_subscribes 和 init_plugin 调用。

        规则：
        - 屏蔽系统订阅 = 开启 → 始终接管（无视时间）
        - 屏蔽系统订阅 = 关闭 → 判定当前时间：
          - 开放态（unblock时段）→ 恢复用户配置的原始站点
          - 屏蔽态（block时段）→ 接管为仅 115 网盘
          - 都不在（缓冲时段）→ 保持现有状态不变
        """
        if self._block_system_subscribe:
            # 屏蔽开启 → 始终接管
            if not self._is_blocked:
                self._backup_and_enter_blocked(reason="屏蔽开关已开启")
            else:
                # 已在接管态：检查是否有新订阅未锁，补锁
                self._lock_new_subscribes_in_blocked()
                logger.debug("屏蔽开启：已在接管态")
        else:
            # 屏蔽关闭 → 按时间段判断
            if self._is_time_in_unblock():
                # 开放态 → 恢复用户站点
                if self._is_blocked:
                    self._restore_and_exit_blocked(reason="进入开放时段")
                else:
                    # 检查数据库是否还有 [-1] 残留（插件重启后 _is_blocked 重置导致）
                    _still_blocked = False
                    with SessionFactory() as _db:
                        from app.db.subscribe_oper import SubscribeOper
                        for _s in (SubscribeOper(db=_db).list() or []):
                            if not self._is_subscribe_excluded(_s.id) and str(getattr(_s, "sites", "[]")) == "[-1]":
                                _still_blocked = True
                                break
                    if _still_blocked:
                        logger.info("数据库仍有 [-1] 残留，强制恢复（开放时段）")
                        self._restore_and_exit_blocked(reason="数据库残留恢复（开放时段）")
                    else:
                        logger.debug("开放时段内：已处于非接管态")
            else:
                # 非开放态 → 判断是否在屏蔽时段
                if self._is_time_in_block():
                    # 屏蔽态 → 主动接管为仅115
                    if not self._is_blocked:
                        self._backup_and_enter_blocked(reason="进入屏蔽时段")
                    else:
                        # 已在接管态：检查是否有新订阅未锁，补锁
                        self._lock_new_subscribes_in_blocked()
                        logger.debug("屏蔽时段：已在接管态")
                else:
                    # 都不在（缓冲期 17:30~18:00）→ 保持现有状态不变
                    logger.debug("当前不在任何管控时段，保持现有状态不变")

    def _lock_new_subscribes_in_blocked(self):
        """
        在已处于屏蔽态时，检查并锁定新添加但未锁的订阅。
        解决：屏蔽态中添加新订阅后，on_subscribe_added 事件未生效时的兜底补锁。
        """
        self._init_subscribe_handler()
        with SessionFactory() as _db:
            from app.db.subscribe_oper import SubscribeOper
            _unlocked = []
            for _s in (SubscribeOper(db=_db).list() or []):
                if self._is_subscribe_excluded(_s.id):
                    continue
                if str(getattr(_s, "sites", "[]")) != "[-1]":
                    _unlocked.append(_s.id)
            if _unlocked:
                logger.info(f"接管态补锁：发现 {len(_unlocked)} 个未锁订阅，正在锁定")
                for _sid in _unlocked:
                    if hasattr(self._subscribe_handler, "set_sites_for_subscribe_only_115"):
                        self._subscribe_handler.set_sites_for_subscribe_only_115(_sid)
                    else:
                        site_id_115 = self._ensure_115_site_id(_db)
                        SubscribeOper(db=_db).update(_sid, {"sites": [site_id_115]})
                logger.info(f"接管态补锁：已完成 {len(_unlocked)} 个订阅的锁定")
            else:
                logger.debug("接管态补锁：所有非排除订阅已锁定")

    def _apply_best_version_selected(self):
        """
        根据 _upgrade_subscribe_ids 列表，给指定的订阅开启原生洗版（best_version=1），
        取消勾选的订阅则关闭洗版（best_version=0）。
        与 _auto_best_version 独立工作：后者开启时自动管理所有电视剧订阅，不受此方法影响。
        """
        if self._auto_best_version:
            logger.debug("[原生洗版] auto_best_version 已开启，跳过独立洗版订阅管理")
            return

        ids = set(self._upgrade_subscribe_ids or [])
        if not ids:
            logger.info("[原生洗版] 无指定洗版订阅，跳过")
            return

        from app.db.subscribe_oper import SubscribeOper
        from app.schemas.types import MediaType

        with SessionFactory() as db:
            oper = SubscribeOper(db=db)
            turned_on = 0
            turned_off = 0

            # 开启：勾选但 best_version 未开启的
            for sid in ids:
                sub = oper.get(sid)
                if not sub:
                    continue
                if sub.type != MediaType.TV.value:
                    continue
                if not bool(getattr(sub, "best_version", False)):
                    oper.update(sid, {"best_version": 1})
                    turned_on += 1

            # 关闭：所有电视剧订阅中 best_version=1 但不在勾选列表里的
            all_subs = oper.list() or []
            for s in all_subs:
                if s.type != MediaType.TV.value:
                    continue
                if s.id in ids:
                    continue
                if bool(getattr(s, "best_version", False)):
                    oper.update(s.id, {"best_version": 0})
                    turned_off += 1

            if turned_on:
                logger.info(f"[原生洗版] 已为 {turned_on} 个指定订阅开启洗版")
            if turned_off:
                logger.info(f"[原生洗版] 已为 {turned_off} 个取消勾选的订阅关闭洗版")

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
        - 自动填充规则（内置 SubscribeGroup 功能）
        """
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        if self._is_subscribe_excluded(sid):
            logger.info(f"新增订阅不在本插件处理范围（订阅过滤模式：{self._subscribe_filter_mode}），跳过站点同步（subscribe_id={sid}）")
            return

        # 从事件数据中提取 mediainfo（含 MP 计算好的二级分类）
        event_data = event.event_data or {}
        mediainfo_dict = event_data.get("mediainfo") or {}
        event_category = mediainfo_dict.get("category") or ""

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

            # --- 自动填充规则（内置 SubscribeGroup 功能） ---
            if self._subscribe_auto_fill:
                try:
                    with SessionFactory() as db:
                        subscribe = SubscribeOper(db=db).get(sid)
                        if subscribe:
                            update_dict = {}
                            is_tv = subscribe.type == MediaType.TV.value

                            # 从事件数据 mediainfo 获取二级分类（SubscribeGroup 同源方式）
                            category = event_category

                            # mediainfo 未带分类时，尝试用 TMDB 重新识别
                            if not category:
                                try:
                                    from app.schemas.types import MediaType as MediaTypeEnum
                                    mtype = MediaTypeEnum.TV if is_tv else MediaTypeEnum.MOVIE
                                    tmdb_id = mediainfo_dict.get("tmdb_id") or subscribe.tmdbid
                                    if tmdb_id:
                                        media_info = self.chain.recognize_media(mtype=mtype, tmdbid=tmdb_id)
                                        if media_info and media_info.category:
                                            category = media_info.category
                                            logger.info(f"新增订阅 {subscribe.name}：通过 TMDB 识别到二级分类「{category}」")
                                except Exception as e:
                                    logger.warning(f"TMDB 识别二级分类失败: {e}")

                            # 匹配分类规则
                            matched = None
                            cat_rules = self._parse_category_rules()
                            if category and category in cat_rules:
                                matched = cat_rules[category]
                                logger.info(f"新增订阅 {subscribe.name}：二级分类「{category}」→ "
                                            f"规则组「{matched['filter_group']}」"
                                            f"{' + include=' + matched['include'] if matched.get('include') else ''}"
                                            f"{' + exclude=' + matched['exclude'] if matched.get('exclude') else ''}")

                            # 未匹配到分类规则时，尝试按类型兜底
                            if not matched:
                                type_fallback = '未分类_TV' if is_tv else '未分类_Movie'
                                if type_fallback in cat_rules:
                                    matched = cat_rules[type_fallback]
                                    logger.info(f"新增订阅 {subscribe.name}：无精确分类匹配，用「{type_fallback}」兜底→ "
                                                f"规则组「{matched['filter_group']}」"
                                                f"{' + include=' + matched['include'] if matched.get('include') else ''}"
                                                f"{' + exclude=' + matched['exclude'] if matched.get('exclude') else ''}")
                                elif '未分类' in cat_rules:
                                    matched = cat_rules['未分类']
                                    logger.info(f"新增订阅 {subscribe.name}：无精确分类匹配，用「未分类」兜底→ "
                                                f"规则组「{matched['filter_group']}」"
                                                f"{' + include=' + matched['include'] if matched.get('include') else ''}"
                                                f"{' + exclude=' + matched['exclude'] if matched.get('exclude') else ''}")
                                else:
                                    logger.info(f"新增订阅 {subscribe.name}：无匹配分类规则，可添加「未分类_TV」「未分类_Movie」行到文本域做兜底")

                            if matched:
                                if matched.get('filter_group'):
                                    update_dict["filter_groups"] = [matched['filter_group']]
                                if matched.get('include'):
                                    update_dict["include"] = matched['include']
                                if matched.get('exclude'):
                                    update_dict["exclude"] = matched['exclude']

                                if update_dict:
                                    SubscribeOper(db=db).update(sid, update_dict)
                                    logger.info(f"新订阅规则自动填充完成：{subscribe.name} → {update_dict}")
                except Exception as e2:
                    logger.error(f"新订阅规则自动填充失败：{e2}")

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

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        """媒体入库事件触发PT洗版扫描（仅开放态时段，带防抖）"""
        if not event or not self._enabled:
            return
        if not self._enable_pt_upgrade:
            return
        # 屏蔽态时不触发
        if self._is_blocked:
            return
        if not self._is_time_in_unblock():
            return

        import time
        now = time.time()
        elapsed = now - self._last_pt_upgrade_time
        if elapsed < self._upgrade_debounce_seconds:
            remaining = int(self._upgrade_debounce_seconds - elapsed)
            logger.debug(
                f"PT洗版防抖：距上次扫描仅 {elapsed:.0f}s，"
                f"还需 {remaining}s，跳过"
            )
            return

        logger.info("媒体入库事件触发PT洗版扫描...")
        self._last_pt_upgrade_time = now
        try:
            if self._sync_handler:
                self._sync_handler.auto_upgrade_scan(source='pt')
                # PT洗版完成后处理到期的延迟删除
                self._sync_handler.process_expired_deletions()
        except Exception as e:
            logger.error(f"PT洗版扫描异常：{e}")

    @eventmanager.register(ChainEventType.ResourceDownload)
    def on_resource_download(self, event: Event):
        """下载前拦截：候选种子评分不如已有 strm 时取消下载"""
        if not event or not self._enabled:
            return
        if not self._sync_handler:
            return

        event_data: ResourceDownloadEventData = event.event_data
        if not event_data:
            return

        # 拦截所有下载（PT/RSS/刷流等），评分不如现有 strm 就取消
        context = event_data.context
        if not context:
            return

        torrent = context.torrent_info
        media = context.media_info
        meta = context.meta_info
        if not torrent or not media or not meta:
            return

        tmdbid = media.tmdb_id
        if not tmdbid:
            return

        season_list = meta.season_list or [1]
        episode_list = event_data.episodes or meta.episode_list or []
        if not episode_list:
            logger.debug(f"[下载前拦截] {torrent.title} 无剧集信息，放行")
            return

        # 查找匹配的订阅
        from app.db.subscribe_oper import SubscribeOper
        all_subs = []
        for season in season_list:
            all_subs.extend(SubscribeOper().list_by_tmdbid(tmdbid, season))

        if not all_subs:
            return

        # ── 仅115拦截：仅当该剧所有订阅都是 sites=[-1] 时才拦截 ──
        all_only_115 = True
        for s in all_subs:
            if s.type != MediaType.TV.value:
                continue
            sub_sites = getattr(s, 'sites', None) or []
            if sub_sites != [-1]:
                all_only_115 = False
                break

        if all_only_115:
            # 取第一个订阅的名字做展示
            sub_name = all_subs[0].name if all_subs else "未知"
            event_data.cancel = True
            event_data.source = "P115StrgmSub-仅115拦截"
            event_data.reason = f"订阅{sub_name}仅限115网盘，拦截PT下载: {torrent.title}"
            logger.info(
                f"[仅115拦截] ✅ 已拦截 {sub_name} "
                f"({torrent.title}): 所有订阅都是sites=[-1]，拦截PT下载"
            )
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【仅115拦截】阻止PT下载",
                    text=(
                        f"{sub_name} 的所有订阅都设置为仅115网盘，\n"
                        f"已拦截来自PT的下载：{torrent.title}"
                    )
                )
            return

        # ── 找 best_version=True 的订阅做评分拦截 ──
        subscribe = None
        for s in all_subs:
            if s.type == MediaType.TV.value and bool(getattr(s, 'best_version', False)):
                subscribe = s
                break

        if not subscribe:
            return

        # ── 原有评分拦截（仅 best_version） ──
        # 读现有 episode_priority
        try:
            existing = self._sync_handler._read_ep_priority(subscribe)
        except Exception as e:
            logger.warning(f"[下载前拦截] 读取episode_priority失败: {e}")
            return

        # 对候选种子用 MP 规则组打分
        filename = torrent.title or torrent.description or ''
        filesize = torrent.size or 0

        try:
            cand_score = SyncHandler._get_mp_rule_score(
                filename, filesize, subscribe, season_list[0]
            )
        except Exception as e:
            logger.warning(f"[下载前拦截] 评分失败: {e}，放行")
            return

        # 逐集检查
        blocked_any = False
        existing_before = dict(existing)  # 保存旧评分，用于通知
        for ep in episode_list:
            try:
                ep_num = int(ep)
            except (ValueError, TypeError):
                continue
            ep_key = str(ep_num)
            existing_score = existing.get(ep_key, 0)

            if cand_score <= existing_score:
                blocked_any = True
                logger.info(
                    f"[下载前拦截] {subscribe.name} {torrent.title} "
                    f"E{ep_num}: 候选{cand_score}分 ≤ 现有{existing_score}分 → 拦截"
                )
            else:
                logger.info(
                    f"[下载前拦截] {subscribe.name} {torrent.title} "
                    f"E{ep_num}: 候选{cand_score}分 > 现有{existing_score}分 → 放行"
                )
                blocked_any = False
                break  # 只要有一集有提升就不拦整个包

        if blocked_any:
            event_data.cancel = True
            event_data.source = "P115StrgmSub-下载前拦截"
            event_data.reason = f"候选种子{torrent.title}评分{cand_score}分，不低于现有strm"
            logger.info(
                f"[下载前拦截] ✅ 已拦截 {subscribe.name} "
                f"({torrent.title}): {event_data.reason}"
            )
        else:
            # ── 放行分支：防重写入 + 通知 ──
            # 1) 立即写入 episode_priority，防止同 infohash 被多站重复推送
            updated = False
            for ep in episode_list:
                try:
                    ep_num = int(ep)
                except (ValueError, TypeError):
                    continue
                ep_key = str(ep_num)
                if cand_score > existing.get(ep_key, 0):
                    existing[ep_key] = cand_score
                    updated = True
            if updated:
                try:
                    self._sync_handler._save_ep_priority(subscribe, existing)
                    logger.info(
                        f"[下载前拦截] 已写入 episode_priority "
                        f"{subscribe.name} S{season_list[0]:02d}: {cand_score}分 "
                        f"共{len(episode_list)}集"
                    )
                except Exception as e:
                    logger.warning(f"[下载前拦截] 写入 episode_priority 失败: {e}")

            # 2) 发送通知（区分新集和升级）
            if self._notify:
                season_str = season_list[0] if season_list else 1
                new_eps = []
                upgrade_eps = []
                for ep in episode_list:
                    try:
                        ep_num = int(ep)
                    except (ValueError, TypeError):
                        continue
                    old_score = existing_before.get(str(ep_num), 0)
                    if old_score > 0:
                        upgrade_eps.append(ep_num)
                    else:
                        new_eps.append(ep_num)

                if upgrade_eps or new_eps:
                    # 短期重复通知缓存：6小时内同种子标题不重复通知
                    now_ts = time.time()
                    last_ts = self._notified_titles.get(torrent.title, 0)
                    if now_ts - last_ts < 21600:
                        logger.info(
                            f"[下载前拦截] 跳过重复通知：{torrent.title} "
                            f"({subscribe.name}) 已在6小时内通知过"
                        )
                        return
                    self._notified_titles[torrent.title] = now_ts

                    parts = []
                    if new_eps:
                        parts.append(f"新集{len(new_eps)}集")
                    if upgrade_eps:
                        parts.append(f"升级{len(upgrade_eps)}集")
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title=f"【PT下载】{' + '.join(parts)}",
                        text=(
                            f"{subscribe.name} S{season_str:02d}\n"
                            f"种子：{torrent.title}\n"
                            f"评分：{cand_score}分\n"
                            f"已提交PT下载{'，入库后自动清理旧文件' if upgrade_eps else ''}"
                        )
                    )

    # ------------------ init_plugin ------------------

    def init_plugin(self, config: dict = None):
        self.stop_service()
        download_so_file(Path(__file__).parent / "lib")
        self._apply_http_patches()

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

            # TG 频道搜索配置
            self._tg_enabled = config.get("tg_enabled", False)
            self._tg_bot_token = config.get("tg_bot_token", "")
            self._tg_channel_ids = config.get("tg_channel_ids", "")

            # TG 自动转发配置
            self._tg_forward_enabled = config.get("tg_forward_enabled", False)
            self._tg_forward_target = config.get("tg_forward_target", "")

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
            self._upgrade_subscribe_ids = config.get("upgrade_subscribe_ids", []) or []
            self._min_upgrade_tiers = int(config.get("min_upgrade_tiers", 2))
            self._upgrade_threshold = int(config.get("upgrade_threshold", 25))
            self._self_heal_interval = int(config.get("self_heal_interval", 10))
            self._enable_cloud_upgrade = bool(config.get("enable_cloud_upgrade", False))
            self._enable_pt_upgrade = bool(config.get("enable_pt_upgrade", False))
            self._upgrade_debounce_seconds = int(config.get("upgrade_debounce_seconds", 600))
            self._last_pt_upgrade_time = float(config.get("_last_pt_upgrade_time", 0.0))
            self._cloud_tv_local_dir = str(config.get("cloud_tv_local_dir", "") or "")
            self._cloud_tv_remote_dir = str(config.get("cloud_tv_remote_dir", "") or "")
            self._cloud_movie_local_dir = str(config.get("cloud_movie_local_dir", "") or "")
            self._cloud_movie_remote_dir = str(config.get("cloud_movie_remote_dir", "") or "")
            self._subscribe_auto_fill = bool(config.get("subscribe_auto_fill", False))
            raw_cat_rules = config.get("subscribe_category_rules", "")
            if isinstance(raw_cat_rules, list):
                # 兼容旧版 VSelect 格式（升级过渡）
                self._subscribe_category_rules = "\n".join(raw_cat_rules) if raw_cat_rules else ""
            else:
                self._subscribe_category_rules = str(raw_cat_rules or "")
            # 帧率/比特率评分规则
            _fp = config.get("frame_rate_pattern", None)
            if _fp:
                self._frame_rate_pattern = str(_fp)
            _bp = config.get("bit_rate_pattern", None)
            if _bp:
                self._bit_rate_pattern = str(_bp)
            _vp = config.get("vivid_pattern", None)
            if _vp:
                self._vivid_pattern = str(_vp)
            self._auto_register_rules = bool(config.get("auto_register_rules", False))
            self._tv_rule_group_preset = str(config.get("tv_rule_group_preset", "none") or "none")
            self._tv_rule_group_custom = str(config.get("tv_rule_group_custom", "") or "")
            self._movie_rule_group_preset = str(config.get("movie_rule_group_preset", "none") or "none")
            self._movie_rule_group_custom = str(config.get("movie_rule_group_custom", "") or "")
            # 命名规则
            _trf = config.get("tv_rename_format", None)
            if _trf:
                self._tv_rename_format = str(_trf)
            _mrf = config.get("movie_rename_format", None)
            if _mrf:
                self._movie_rename_format = str(_mrf)
            self._auto_apply_naming = bool(config.get("auto_apply_naming", False))

            # 取消屏蔽时间段配置
            self._block_start_time = str(config.get("block_start_time", self._block_start_time) or self._block_start_time)
            self._block_end_time = str(config.get("block_end_time", self._block_end_time) or self._block_end_time)
            self._unblock_start_time = str(config.get("unblock_start_time", self._unblock_start_time) or self._unblock_start_time)
            self._unblock_end_time = str(config.get("unblock_end_time", self._unblock_end_time) or self._unblock_end_time)

            self._block_system_subscribe = bool(config.get("block_system_subscribe", False))

        # 初始化客户端/handlers
        self._init_clients()
        self._init_handlers()

        # 保存配置时立即为勾选的订阅开启原生洗版（best_version=1）
        self._apply_best_version_selected()

        logger.info(f"插件初始化：屏蔽态={self._block_start_time}~{self._block_end_time}, 开放态={self._unblock_start_time}~{self._unblock_end_time}, 洗版={'开启' if self._auto_best_version else '关闭'}, 当前接管态={self._is_blocked}")

        # 启动 TG 私密群转发后台线程
        self._start_tg_forwarder()

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

    def _start_tg_forwarder(self):
        """启动 TG 私密群消息转发后台线程"""
        if not self._enabled or not self._tg_forward_enabled or not self._tg_forward_target:
            logger.info("TG转发未启用或配置不完整，跳过启动")
            self._tg_forwarder_stop.clear()
            return
        if self._tg_forwarder_thread and self._tg_forwarder_thread.is_alive():
            logger.info("TG转发线程已在运行")
            return

        self._tg_forwarder_stop.clear()
        self._tg_forwarder_thread = Thread(
            target=self._tg_forwarder_loop,
            daemon=True,
            name="tg-forwarder"
        )
        self._tg_forwarder_thread.start()
        logger.info(f"TG私密群转发线程已启动，目标群ID: {self._tg_forward_target}")

    def _tg_forwarder_loop(self):
        """TG 私密群转发后台主循环：长轮询 getUpdates → forwardMessage"""
        import json as _json

        # 读取配置
        bot_token = self.get_config().get("tg_bot_token", "")
        src_ids_str = self.get_config().get("tg_channel_ids", "")
        target_id = self._tg_forward_target

        if not bot_token or not src_ids_str or not target_id:
            logger.error("TG转发配置不完整：缺 Bot Token / 来源频道 / 目标群")
            return

        src_ids = [x.strip() for x in src_ids_str.split(",") if x.strip()]
        api_base = f"https://api.telegram.org/bot{bot_token}"
        proxies = {"http": "socks5://192.168.10.112:20170", "https": "socks5://192.168.10.112:20170"}
        last_offset = 0

        logger.info(f"TG转发线程开始运行，监控 {len(src_ids)} 个来源群/频道 → {target_id}")

        while not self._tg_forwarder_stop.is_set():
            try:
                url = f"{api_base}/getUpdates"
                params = {
                    "timeout": 10,
                    "allowed_updates": _json.dumps(["channel_post", "message"]),
                }
                if last_offset:
                    params["offset"] = last_offset

                resp = requests.get(url, params=params, proxies=proxies, timeout=15)
                if resp.status_code != 200:
                    logger.warning(f"TG getUpdates 返回 {resp.status_code}")
                    self._tg_forwarder_stop.wait(5)
                    continue

                data = resp.json()
                if not data.get("ok"):
                    logger.warning(f"TG API 返回 not ok: {data}")
                    self._tg_forwarder_stop.wait(5)
                    continue

                updates = data.get("result", [])
                if updates:
                    # 缓存所有消息到共享缓冲区（供 TG 搜索使用，避免 getUpdates 冲突）
                    try:
                        with self._tg_shared_buffer_lock:
                            for update in updates:
                                post = update.get("channel_post") or update.get("message")
                                if not post:
                                    continue
                                chat = post.get("chat", {})
                                cid = str(chat.get("id", ""))
                                mid = post.get("message_id")
                                if cid and mid:
                                    key = f"{cid}:{mid}"
                                    # 去重
                                    if not any(m.get("_key") == key for m in self._tg_shared_buffer):
                                        entry = {
                                            "_key": key,
                                            "chat_id": cid,
                                            "chat_title": chat.get("title") or chat.get("username") or cid,
                                            "message_id": mid,
                                            "text": post.get("text") or "",
                                            "caption": post.get("caption") or "",
                                            "caption_entities": post.get("caption_entities") or [],
                                            "date": post.get("date"),
                                        }
                                        self._tg_shared_buffer.append(entry)
                            # 限制缓存大小，防止内存泄漏
                            if len(self._tg_shared_buffer) > 500:
                                self._tg_shared_buffer = self._tg_shared_buffer[-300:]
                    except Exception:
                        pass

                for update in updates:
                    update_id = update.get("update_id", 0)
                    if update_id >= last_offset:
                        last_offset = update_id + 1

                    # 判断消息来源
                    msg = update.get("channel_post") or update.get("message") or {}
                    chat = msg.get("chat", {})
                    chat_id = str(chat.get("id", ""))

                    if chat_id not in src_ids:
                        continue

                    msg_id = msg.get("message_id")
                    if not msg_id:
                        continue

                    # 转发到目标群
                    try:
                        fwd = requests.post(
                            f"{api_base}/forwardMessage",
                            json={
                                "chat_id": int(target_id),
                                "from_chat_id": int(chat_id),
                                "message_id": msg_id,
                            },
                            proxies=proxies,
                            timeout=10,
                        )
                        if fwd.status_code == 200:
                            logger.info(f"TG转发: [{chat_id} #{msg_id}] → {target_id}")
                        else:
                            logger.warning(f"TG转发失败 [{chat_id} #{msg_id}]: {fwd.status_code} {fwd.text[:200]}")
                    except Exception as e:
                        logger.error(f"TG转发异常 [{chat_id} #{msg_id}]: {e}")

                # 没有更新时短暂等待，避免空转
                if not updates:
                    self._tg_forwarder_stop.wait(2)

            except requests.exceptions.Timeout:
                # 长轮询超时是预期行为，继续下一轮
                continue
            except (requests.exceptions.ConnectionError, requests.exceptions.SSLError):
                # 代理断连/SSL EOF 是预期行为（v2raya 对空闲连接有限制），静默重试
                self._tg_forwarder_stop.wait(2)
                continue
            except Exception as e:
                logger.error(f"TG转发线程异常: {e}")
                self._tg_forwarder_stop.wait(5)

    def _parse_category_rules(self) -> dict:
        """
        解析二级分类规则映射（新文本域格式）。
        从数据库直读插件配置，不依赖内存缓存，确保规则始终最新。
        输入（多行文本，用 , 逗号分隔字段）：
            国产剧,电视剧非杜比画质优先,,
            综艺,电视剧非杜比画质优先,,直拍|加更|先导|...
        字段顺序：category, filter_group, include, exclude
        输出：
            {
                '国产剧': {'filter_group': '电视剧非杜比画质优先', 'include': '', 'exclude': ''},
                '综艺': {'filter_group': '电视剧非杜比画质优先', 'include': '', 'exclude': '直拍|加更|...'},
            }
        """
        # 从数据库直读插件配置
        try:
            from app.db import SessionFactory
            from sqlalchemy import text
            with SessionFactory() as db:
                row = db.execute(
                    text("SELECT value FROM systemconfig WHERE key='plugin.P115StrgmSub'")
                ).fetchone()
                if row:
                    import json
                    val = json.loads(row[0])
                    raw = val.get('subscribe_category_rules', '')
                    if isinstance(raw, list):
                        raw = '\n'.join(raw) if raw else ''
                    else:
                        raw = str(raw or '')
                else:
                    raw = self._subscribe_category_rules or ''
        except Exception:
            raw = self._subscribe_category_rules or ''
        result = {}
        for line in raw.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 2:
                continue
            category = parts[0]
            filter_group = parts[1]
            include = parts[2] if len(parts) > 2 else ''
            exclude = parts[3] if len(parts) > 3 else ''
            if category and filter_group:
                result[category] = {
                    'filter_group': filter_group,
                    'include': include,
                    'exclude': exclude,
                }
        return result

    @staticmethod
    def _get_available_rule_groups() -> list:
        """
        从 MP 系统配置中读取所有已注册的规则组名称。
        :return: ['电视剧非杜比画质优先', '电视剧杜比画质优先', ...]
        """
        try:
            from app.db.systemconfig_oper import SystemConfigOper, SystemConfigKey
            oper = SystemConfigOper()
            groups = oper.get(SystemConfigKey.UserFilterRuleGroups) or []
            return [g['name'] for g in groups if g.get('name')]
        except Exception as e:
            logger.warning(f"读取可用规则组失败: {e}")
            return []
    def _register_filter_rules(self):
        """
        向MP注册自定义过滤规则（VIVID/10BIT/60FPS扩展），
        让优先级规则组中的 Vivid/10bit/120fps 等规则 ID 能被 MP 正常识别匹配。
        同时根据 preset 设置应用优先级规则组。
        仅在 auto_register_rules=True 时执行。
        """
        if not self._auto_register_rules:
            logger.info("自动注册过滤规则已关闭，跳过")
            return
        try:
            oper = SystemConfigOper()
            existing = oper.get(SystemConfigKey.CustomFilterRules) or []

            # 从插件配置读取规则正则
            custom_rules = {
                "VIVID": {
                    "id": "VIVID",
                    "name": "HDR Vivid（菁彩影像）",
                    "include": self._vivid_pattern,
                    "exclude": "",
                },
                "10BIT": {
                    "id": "10BIT",
                    "name": "10bit/12bit 色深",
                    "include": self._bit_rate_pattern,
                    "exclude": "",
                },
                "60FPS": {
                    "id": "60FPS",
                    "name": "高帧率（60fps/120fps/50fps）",
                    "include": self._frame_rate_pattern,
                    "exclude": "",
                },
            }

            # 检查/更新自定义规则
            existing_ids = {r.get("id") for r in existing if r.get("id")}
            need_update = False
            for rid, rule in custom_rules.items():
                if rid not in existing_ids:
                    existing.append(dict(rule))
                    need_update = True
                    logger.info(f"新增 MP 自定义过滤规则: {rid} → {rule['include']}")
                else:
                    for e in existing:
                        if e.get("id") == rid:
                            if e.get("include") != rule.get("include"):
                                e["include"] = rule["include"]
                                e["name"] = rule["name"]
                                e["exclude"] = rule.get("exclude", "")
                                need_update = True
                                logger.info(f"更新 MP 自定义过滤规则: {rid} → {rule['include']}")
                            break

            if need_update:
                oper.set(SystemConfigKey.CustomFilterRules, existing)
                logger.info("自定义过滤规则已写入 MP 数据库")

            # 应用规则组预设
            self._apply_rule_group_presets(oper)

            # 触发 FilterModule 热重载
            try:
                mm = ModuleManager()
                fm = mm._running_modules.get("FilterModule")
                if fm and hasattr(fm, "init_module"):
                    fm.init_module()
                    logger.info("FilterModule 规则集已热重载")
            except Exception as e:
                logger.warning(f"FilterModule 热重载失败（下次MP重启生效）: {e}")

        except Exception as e:
            logger.error(f"注册自定义过滤规则失败: {e}")

    PRESET_TV_NO_DOVI = (
        "!DOLBY & VIVID & 10BIT & 60FPS & H265 & 4K "
        "> !DOLBY & VIVID & 10BIT & H265 & 4K "
        "> !DOLBY & VIVID & H265 & 4K "
        "> !DOLBY & HDR & H265 & 4K "
        "> !DOLBY & HDR & H264 & 4K "
        "> !DOLBY & SDR & H265 & 4K "
        "> !DOLBY & SDR & H264 & 4K "
        "> !DOLBY & 4K "
        "> !DOLBY & HDR & H265 & 1080P "
        "> !DOLBY & HDR & H264 & 1080P "
        "> !DOLBY & SDR & H265 & 1080P "
        "> !DOLBY & SDR & H264 & 1080P "
        "> !DOLBY & 1080P"
    )

    PRESET_TV_DOVI = (
        "DOLBY & 10BIT & 60FPS & H265 & 4K "
        "> DOLBY & 10BIT & H265 & 4K "
        "> DOLBY & H265 & 4K "
        "> DOLBY & HDR & H265 & 4K "
        "> DOLBY & HDR & H264 & 4K "
        "> DOLBY & 4K "
        "> DOLBY & H265 & 1080P "
        "> DOLBY & 1080P"
    )

    PRESET_MOVIE_NO_DOVI = (
        "REMUX & VIVID & 10BIT & 60FPS & H265 & 4K "
        "> REMUX & VIVID & 10BIT & H265 & 4K "
        "> REMUX & VIVID & H265 & 4K "
        "> REMUX & HDR & H265 & 4K "
        "> REMUX & HDR & H265 & 1080P "
        "> REMUX & HDR & H264 & 1080P "
        "> REMUX & SDR & H265 & 4K "
        "> REMUX & !DOLBY & 4K "
        "> REMUX & !DOLBY & 1080P "
        "> BLURAY & HDR & H265 & 4K "
        "> BLURAY & HDR & H264 & 4K "
        "> BLURAY & HDR & H265 & 1080P "
        "> BLURAY & HDR & H264 & 1080P "
        "> BLURAY & SDR & H265 & 4K "
        "> BLURAY & SDR & H264 & 4K "
        "> BLURAY & !DOLBY & 4K "
        "> BLURAY & !DOLBY & 1080P "
        "> HDR & H265 & 4K "
        "> HDR & H264 & 4K "
        "> SDR & H265 & 4K "
        "> SDR & H264 & 4K "
        "> !DOLBY & 4K "
        "> HDR & H265 & 1080P "
        "> HDR & H264 & 1080P "
        "> SDR & H265 & 1080P "
        "> SDR & H264 & 1080P "
        "> !DOLBY & 1080P"
    )

    PRESET_MOVIE_DOVI = (
        "REMUX & DOLBY & VIVID & 10BIT & 60FPS & H265 & 4K "
        "> REMUX & DOLBY & VIVID & 10BIT & H265 & 4K "
        "> REMUX & DOLBY & VIVID & H265 & 4K "
        "> REMUX & DOLBY & HDR & H265 & 4K "
        "> REMUX & DOLBY & HDR & H264 & 4K "
        "> REMUX & DOLBY & 4K "
        "> REMUX & DOLBY & 1080P "
        "> REMUX & !DOLBY & VIVID & 10BIT & 60FPS & H265 & 4K "
        "> REMUX & !DOLBY & VIVID & 10BIT & H265 & 4K "
        "> REMUX & !DOLBY & VIVID & H265 & 4K "
        "> REMUX & !DOLBY & HDR & H265 & 4K "
        "> REMUX & !DOLBY & HDR & H265 & 1080P "
        "> REMUX & !DOLBY & HDR & H264 & 1080P "
        "> REMUX & SDR & H265 & 4K "
        "> REMUX & !DOLBY & 4K "
        "> REMUX & !DOLBY & 1080P "
        "> BLURAY & !DOLBY & VIVID & 10BIT & 60FPS & H265 & 4K "
        "> BLURAY & !DOLBY & VIVID & 10BIT & H265 & 4K "
        "> BLURAY & !DOLBY & VIVID & H265 & 4K "
        "> BLURAY & !DOLBY & HDR & H265 & 4K "
        "> BLURAY & !DOLBY & HDR & H264 & 4K "
        "> BLURAY & !DOLBY & HDR & H265 & 1080P "
        "> BLURAY & !DOLBY & HDR & H264 & 1080P "
        "> BLURAY & SDR & H265 & 4K "
        "> BLURAY & SDR & H264 & 4K "
        "> BLURAY & !DOLBY & 4K "
        "> BLURAY & !DOLBY & 1080P "
        "> !DOLBY & HDR & H265 & 4K "
        "> !DOLBY & HDR & H264 & 4K "
        "> !DOLBY & SDR & H265 & 4K "
        "> !DOLBY & SDR & H264 & 4K "
        "> !DOLBY & 4K "
        "> !DOLBY & HDR & H265 & 1080P "
        "> !DOLBY & HDR & H264 & 1080P "
        "> !DOLBY & SDR & H265 & 1080P "
        "> !DOLBY & SDR & H264 & 1080P "
        "> !DOLBY & 1080P"
    )

    def _apply_rule_group_presets(self, oper=None):
        """
        根据 preset 设置写入或更新 MP 优先级规则组。
        保留用户原有规则组，新增/更新预设规则组。
        """
        try:
            if oper is None:
                oper = SystemConfigOper()
            groups = oper.get(SystemConfigKey.UserFilterRuleGroups) or []
            subscribe_refs = oper.get(SystemConfigKey.SubscribeFilterRuleGroups) or []
            best_refs = oper.get(SystemConfigKey.BestVersionFilterRuleGroups) or []

            changed = False

            # === 一次性创建全部4套预设规则组 ===
            preset_configs = [
                # (name, media_type, rule_string)
                ("电视剧非杜比画质优先", "电视剧", self.PRESET_TV_NO_DOVI),
                ("电视剧杜比画质优先", "电视剧", self.PRESET_TV_DOVI),
                ("电影含杜比画质优先", "电影", self.PRESET_MOVIE_DOVI),
                ("电影非杜比画质优先", "电影", self.PRESET_MOVIE_NO_DOVI),
            ]
            for group_name, media_type, rule_string in preset_configs:
                result = self._upsert_rule_group(
                    groups, group_name, media_type, rule_string
                )
                if result:
                    changed = True
                    if group_name not in subscribe_refs:
                        subscribe_refs.append(group_name)
                    if group_name not in best_refs:
                        best_refs.append(group_name)

            if changed:
                oper.set(SystemConfigKey.UserFilterRuleGroups, groups)
                oper.set(SystemConfigKey.SubscribeFilterRuleGroups, subscribe_refs)
                oper.set(SystemConfigKey.BestVersionFilterRuleGroups, best_refs)
                logger.info(f"4套预设规则组已全部写入，订阅引用: {subscribe_refs}")
        except Exception as e:
            logger.error(f"应用规则组预设失败: {e}")

    def _upsert_rule_group(self, groups, group_name, media_type, rule_string):
        """查找或创建规则组，返回 True 表示有变更"""
        for g in groups:
            if g.get("name") == group_name:
                if g.get("rule_string", "").strip() == rule_string.strip():
                    return False  # 无变化
                g["rule_string"] = rule_string
                g["media_type"] = media_type
                logger.info(f"更新规则组: {group_name}")
                return True
        # 不存在则新增
        groups.append({
            "name": group_name,
            "rule_string": rule_string,
            "media_type": media_type
        })
        logger.info(f"新建规则组: {group_name}")
        return True

    # _apply_one_preset 已废弃，全部预设一次性创建

    def _apply_naming_rules(self):
        """
        应用命名规则模板到 MP 系统设置。
        仅在 auto_apply_naming=True 时执行。
        """
        if not self._auto_apply_naming:
            return
        try:
            from app.core.config import settings
            changed = False

            # 电视剧命名
            current_tv = getattr(settings, 'TV_RENAME_FORMAT', None)
            if current_tv and current_tv != self._tv_rename_format:
                ok, msg = settings.update_setting('TV_RENAME_FORMAT', self._tv_rename_format)
                if ok:
                    logger.info(f"电视剧命名规则已更新")
                    changed = True
                else:
                    logger.warning(f"电视剧命名规则更新失败: {msg}")

            # 电影命名
            current_movie = getattr(settings, 'MOVIE_RENAME_FORMAT', None)
            if current_movie and current_movie != self._movie_rename_format:
                ok, msg = settings.update_setting('MOVIE_RENAME_FORMAT', self._movie_rename_format)
                if ok:
                    logger.info(f"电影命名规则已更新")
                    changed = True
                else:
                    logger.warning(f"电影命名规则更新失败: {msg}")

            if not changed:
                logger.debug("命名规则无需更新")
        except Exception as e:
            logger.error(f"应用命名规则失败: {e}")

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
            search_source_order=self._search_source_order,
            tg_enabled=self._tg_enabled,
            tg_bot_token=self._tg_bot_token,
            tg_channel_ids=self._tg_channel_ids,
            tg_shared_buffer=self._tg_shared_buffer,
            tg_shared_buffer_lock=self._tg_shared_buffer_lock,
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
            min_upgrade_tiers=self._min_upgrade_tiers,
            upgrade_threshold=self._upgrade_threshold,
            self_heal_interval=self._self_heal_interval,
            enable_cloud_upgrade=self._enable_cloud_upgrade,
            enable_pt_upgrade=self._enable_pt_upgrade,
            auto_best_version=self._auto_best_version,
            cloud_tv_local_dir=self._cloud_tv_local_dir,
            cloud_tv_remote_dir=self._cloud_tv_remote_dir,
            cloud_movie_local_dir=self._cloud_movie_local_dir,
            cloud_movie_remote_dir=self._cloud_movie_remote_dir,
            frame_rate_pattern=self._frame_rate_pattern,
            bit_rate_pattern=self._bit_rate_pattern,
            vivid_pattern=self._vivid_pattern
        )

        # 启动时触发一次兜底清理（保存配置即触发，相当于手动开关）
        if self._enabled and self._sync_handler and self._enable_cloud_upgrade:
            import time
            self._last_cloud_cleanup = time.time()
            try:
                self._sync_handler.auto_upgrade_scan(source='cloud')
            except Exception as e:
                logger.error(f"启动兜底清理异常：{e}")

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
            # TG 频道搜索配置
            "tg_enabled": self._tg_enabled,
            "tg_bot_token": self._tg_bot_token,
            "tg_channel_ids": self._tg_channel_ids,
            "tg_forward_enabled": self._tg_forward_enabled,
            "tg_forward_target": self._tg_forward_target,
            # 其他配置
            "search_source_order": self._search_source_order,
            "subscribe_filter_mode": self._subscribe_filter_mode,
            "exclude_subscribes": self._exclude_subscribes,
            "include_subscribes": self._include_subscribes,
            "block_system_subscribe": self._block_system_subscribe,
            "block_start_time": self._block_start_time,
            "block_end_time": self._block_end_time,
            "auto_best_version": self._auto_best_version,
            "upgrade_subscribe_ids": self._upgrade_subscribe_ids,
            "unblock_start_time": self._unblock_start_time,
            "unblock_end_time": self._unblock_end_time,
            "max_transfer_per_sync": self._max_transfer_per_sync,
            "batch_size": self._batch_size,
            "skip_other_season_dirs": self._skip_other_season_dirs,
            "enable_cloud_upgrade": self._enable_cloud_upgrade,
            "enable_pt_upgrade": self._enable_pt_upgrade,
            "upgrade_debounce_seconds": self._upgrade_debounce_seconds,
            "cloud_tv_local_dir": self._cloud_tv_local_dir,
            "cloud_tv_remote_dir": self._cloud_tv_remote_dir,
            "cloud_movie_local_dir": self._cloud_movie_local_dir,
            "cloud_movie_remote_dir": self._cloud_movie_remote_dir,
            "min_upgrade_tiers": self._min_upgrade_tiers,
            "upgrade_threshold": self._upgrade_threshold,
            "self_heal_interval": self._self_heal_interval,
            "frame_rate_pattern": self._frame_rate_pattern,
            "bit_rate_pattern": self._bit_rate_pattern,
            "vivid_pattern": self._vivid_pattern,
            "subscribe_auto_fill": self._subscribe_auto_fill,
            "subscribe_category_rules": self._subscribe_category_rules,
        })

    # ------------------ stop ------------------

    def stop_service(self):
        """停止服务"""
        # 停止 TG 转发线程
        try:
            self._tg_forwarder_stop.set()
            if self._tg_forwarder_thread and self._tg_forwarder_thread.is_alive():
                self._tg_forwarder_thread.join(timeout=5)
                logger.info("TG转发线程已停止")
        except Exception:
            pass
        self._tg_forwarder_thread = None

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
        return UIConfig.get_form(
            available_rule_groups=self._get_available_rule_groups()
        )

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
            },
            {
                "path": "/apply_filter_rules",
                "endpoint": self.api_apply_filter_rules,
                "methods": ["POST"],
                "summary": "手动应用MP过滤规则（自定义规则+优先级规则组预设）"
            },
            {
                "path": "/batch_re_score",
                "endpoint": self.api_batch_re_score,
                "methods": ["POST"],
                "summary": "整理记录评分：对已选订阅已有strm批量评分写入episode_priority"
            },
            {
                "path": "/force_re_score",
                "endpoint": self.api_force_re_score,
                "methods": ["POST"],
                "summary": "强制重评分：清空episode_priority缓存，重新扫描磁盘strm文件评分（覆盖旧评分）"
            }
        ]
    
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程控制命令"""
        return [
            {
                "cmd": "/p115_sub_action",
                "event": EventType.PluginAction,
                "desc": "115网盘订阅追更",
                "category": "订阅",
                "data": {
                    "action": "p115_sub_action"
                }
            },
            {
                "cmd": "/pt_sub_search",
                "event": EventType.PluginAction,
                "desc": "手动执行PT订阅",
                "category": "订阅",
                "data": {
                    "action": "pt_sub_search"
                }
            }
        ]


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

        # 5分钟定时检查：自动处理开放态/屏蔽态切换
        services.append({
            "id": "P115StrgmSub_BlockCheck",
            "name": "115网盘接管定时检查（5分钟）",
            "trigger": IntervalTrigger(seconds=300),
            "func": self._apply_block_by_time,
            "kwargs": {}
        })

        return services

    # ======================================================================
    # 必备：_do_sync（返回 bool）
    # ======================================================================

    def _do_sync(self) -> bool:
        # 至少启用一个搜索源
        if not self._pansou_enabled and not self._hdhive_enabled and not self._tg_enabled:
            logger.error("搜索源均未启用（PanSou/HDHive/TG），无法执行")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更】配置错误",
                    text="PanSou、HDHive、TG 均未启用，请至少启用一个搜索源。"
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
                self._search_handler.reset_tg_cache()
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

        # ⭐ 集数守护：扫描媒体库 strm 文件，同步订阅进度 / 完结通知
        try:
            with SessionFactory() as db:
                all_subs = SubscribeOper(db=db).list() or []
            completed = self._sync_handler.guardian_check(all_subs)
            if completed > 0:
                logger.info(f"[集数守护] 本次完成 {completed} 个订阅")
        except Exception as e:
            import traceback
            logger.error(f"[集数守护] 执行异常: {e}\n{traceback.format_exc()}")

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

    def api_apply_filter_rules(self) -> dict:
        """API: 手动应用MP过滤规则，返回日志摘要"""
        from io import StringIO
        import logging
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.logger.addHandler(handler)
        try:
            self._register_filter_rules()
            logger.info("✅ 手动应用过滤规则完成")
        finally:
            logger.logger.removeHandler(handler)
        return {
            "success": True,
            "message": log_capture.getvalue()
        }

    def _batch_re_score(self) -> dict:
        """
        批量评分：对已选独立洗版订阅的已有转存记录评分并写入 episode_priority。
        保存配置时自动触发，也可通过 API 调用。
        :return: {"success": bool, "message": str, "results": [str, ...]}
        """
        import re
        import json
        from app.db import SessionFactory
        from app.db.subscribe_oper import SubscribeOper
        from app.db.transferhistory_oper import TransferHistoryOper
        from app.schemas.types import MediaType

        subscribe_ids = self._upgrade_subscribe_ids
        if not subscribe_ids:
            msg = "未选择任何订阅，请先在「单独开启洗版的订阅」中选择要评分的订阅"
            logger.info(f"[自动评分] {msg}")
            return {"success": False, "message": msg, "results": []}

        # 收集所有订阅
        with SessionFactory() as db:
            oper = SubscribeOper(db=db)
            all_subs = oper.list() or []

        target_subs = [s for s in all_subs if s.id in subscribe_ids and s.type == MediaType.TV.value]
        if not target_subs:
            msg = "选择的订阅中没有电视剧订阅，或订阅不存在"
            logger.info(f"[自动评分] {msg}")
            return {"success": False, "message": msg, "results": []}

        total_processed = 0
        total_updated = 0
        results = []

        for sub in target_subs:
            try:
                tmdbid = getattr(sub, 'tmdbid', None) or getattr(sub, 'tvdbid', None)
                season = sub.season or 1
                sub_name = f"{sub.name} S{season:02d}"

                # 查询该订阅的所有转存记录
                with SessionFactory() as db:
                    th_oper = TransferHistoryOper(db=db)
                    records = th_oper.list_by_tmdbid(tmdbid) if hasattr(th_oper, 'list_by_tmdbid') else []

                if not records:
                    # 尝试模糊查title
                    with SessionFactory() as db:
                        from sqlalchemy import text
                        rows = db.execute(
                            text("SELECT * FROM transferhistory WHERE title LIKE :t AND seasons LIKE :s ORDER BY id DESC LIMIT 50"),
                            {"t": f"%{sub.name}%", "s": f"%{season}%"}
                        ).fetchall()
                        records = rows if rows else []

                if not records:
                    results.append(f"{sub_name}: 无转存记录（请先转存后再评分）")
                    continue

                # 对每条记录评分
                episode_scores = {}
                for rec in records:
                    src_fileitem = getattr(rec, 'src_fileitem', None) or getattr(rec, 'src', None)
                    episodes_str = getattr(rec, 'episodes', None)
                    if not src_fileitem or not episodes_str:
                        continue

                    if isinstance(src_fileitem, str):
                        try:
                            src_data = json.loads(src_fileitem)
                        except:
                            src_data = {}
                    else:
                        src_data = src_fileitem

                    filename = src_data.get('name', src_data.get('basename', ''))
                    filesize = src_data.get('size', 0)

                    if not filename:
                        continue

                    # 解析集号
                    ep_match = re.search(r'S?0*E(\d+)', episodes_str) if episodes_str else None
                    if ep_match:
                        episodes_list = [int(ep_match.group(1))]
                    else:
                        try:
                            episodes_list = json.loads(episodes_str) if episodes_str else []
                        except:
                            episodes_list = []

                    # ★ 使用 MP 原生规则组评分（与 PT 选种同源）
                    # 这样 strm 文件和 PT 种子的评分在同一条规则尺上，
                    # 同品质的 strm 不会被低品质 PT 种子"升级"
                    from app.schemas import TorrentInfo
                    from app.core.context import MediaInfo
                    from app.modules.filter import FilterModule

                    try:
                        # 获取订阅的规则组（优先用订阅自带的，否则用系统默认洗版规则组）
                        rule_group_names = getattr(sub, 'filter_groups', None) or []
                        if not rule_group_names:
                            from app.db.systemconfig_oper import SystemConfigOper, SystemConfigKey
                            rule_group_names = SystemConfigOper().get(
                                SystemConfigKey.BestVersionFilterRuleGroups) or []

                        # 创建最小 MediaInfo（只需 type 字段供规则组过滤）
                        fake_mediainfo = MediaInfo(type=MediaType(sub.type))

                        # 构造假 TorrentInfo 走 MP 过滤器
                        fake_torrent = TorrentInfo(
                            title=filename,
                            size=filesize or 0,
                            description='',
                            labels=[]
                        )

                        filter_module = FilterModule()
                        filter_module.init_module()
                        matched = filter_module.filter_torrents(
                            rule_groups=rule_group_names,
                            torrent_list=[fake_torrent],
                            mediainfo=fake_mediainfo
                        )

                        if matched:
                            score = matched[0].pri_order
                            if score is None or score < 60:
                                score = 60
                            logger.debug(
                                f'[自动评分] {sub_name} E{episodes_list} '
                                f'规则组评分={score} (规则组={rule_group_names})')
                        else:
                            # 规则组无匹配 → 保底分
                            score = 60
                            logger.debug(
                                f'[自动评分] {sub_name} E{episodes_list} '
                                f'未匹配到任何规则组规则 → 保底60分')
                    except Exception as e:
                        logger.warning(f'[自动评分] 规则组评分失败（回退基础评分）: {e}')
                        # 回退：简单的分辨率+编码基础评分
                        score = 40
                        if re.search(r'2160p|4K|UHD', filename, re.IGNORECASE):
                            score += 20
                        elif re.search(r'1080p', filename, re.IGNORECASE):
                            score += 10
                        if re.search(r'HDR', filename, re.IGNORECASE):
                            score += 12
                        if re.search(r'H265|HEVC|x265|H\.265', filename, re.IGNORECASE):
                            score += 10
                        size_gb = filesize / (1024**3) if filesize else 0
                        if size_gb >= 5:
                            score += 15
                        elif size_gb >= 3:
                            score += 12
                        elif size_gb >= 2:
                            score += 8
                        elif size_gb >= 1:
                            score += 5
                        score = min(score, 100)

                    for ep in episodes_list:
                        ep_key = str(ep)
                        if ep_key not in episode_scores or score > episode_scores[ep_key]:
                            episode_scores[ep_key] = score

                if not episode_scores:
                    results.append(f"{sub_name}: 未能从转存记录中解析到可评分的文件")
                    continue

                # ★ 合并现有 episode_priority：已有评分的集数不覆盖（避免小文件顶掉大文件）
                existing_ep = getattr(sub, 'episode_priority', None)
                if isinstance(existing_ep, str):
                    try:
                        existing_ep = json.loads(existing_ep)
                    except:
                        existing_ep = None
                if isinstance(existing_ep, dict):
                    for ek, ev in existing_ep.items():
                        if isinstance(ev, dict):
                            old_score = int(ev.get("score", 0))
                        elif isinstance(ev, (int, float)):
                            old_score = int(ev)
                        else:
                            old_score = 0
                        # ★ 已有评分的保留原值，只补缺失的集
                        if ek in existing_ep and old_score > 0:
                            if ek not in episode_scores:
                                episode_scores[ek] = old_score
                        elif ek not in episode_scores or old_score > episode_scores[ek]:
                            episode_scores[ek] = old_score

                # 写入 episode_priority（纯int格式，MP API验证要求）
                SubscribeOper().update(sub.id, {"episode_priority": episode_scores})
                total_updated += len(episode_scores)
                total_processed += 1
                ep_list = sorted(episode_scores.keys(), key=int)
                score_detail = ", ".join([f"E{e}={episode_scores[e]}分" for e in ep_list[:10]])
                if len(ep_list) > 10:
                    score_detail += f"... 共{len(ep_list)}集"
                results.append(f"{sub_name}: 已评分{len(ep_list)}集 ({score_detail})")

            except Exception as e:
                results.append(f"{sub.name} S{sub.season or 1}: 出错 - {e}")
                logger.error(f"[自动评分] 批量评分出错 {sub.name}: {e}")

        summary = f"处理了 {total_processed}/{len(target_subs)} 个订阅，更新了 {total_updated} 集评分"
        # 日志输出结果
        logger.info(f"[自动评分] {summary}")
        for r in results:
            logger.info(f"[自动评分] {r}")
        return {
            "success": True,
            "message": summary + "\n" + "\n".join(results),
            "results": results
        }

    def api_batch_re_score(self) -> dict:
        """API: 整理记录评分 - 调用内部 _batch_re_score()"""
        return self._batch_re_score()

    def _force_re_score(self) -> dict:
        """
        强制重评分：清空 episode_priority 缓存，重新评分并覆盖旧数据。
        从转存记录读取实际115网盘文件大小用于50%体积评分。
        同时清理无对应 strm 文件的脏数据。
        :return: {"success": bool, "message": str, "results": [str, ...]}
        """
        import os
        import re
        import json
        from app.db import SessionFactory
        from app.db.subscribe_oper import SubscribeOper
        from app.db.transferhistory_oper import TransferHistoryOper
        from app.schemas.types import MediaType
        from app.schemas import TorrentInfo
        from app.core.context import MediaInfo
        from app.modules.filter import FilterModule
        from sqlalchemy import text

        subscribe_ids = self._upgrade_subscribe_ids
        if not subscribe_ids:
            msg = "未选择任何订阅，请先在「单独开启洗版的订阅」中选择要评分的订阅"
            logger.info(f"[强制重评分] {msg}")
            return {"success": False, "message": msg, "results": [msg]}

        with SessionFactory() as db:
            oper = SubscribeOper(db=db)
            all_subs = oper.list() or []

        target_subs = [s for s in all_subs if s.id in subscribe_ids and s.type == MediaType.TV.value]
        if not target_subs:
            msg = "选择的订阅中没有电视剧订阅"
            logger.info(f"[强制重评分] {msg}")
            return {"success": False, "message": msg, "results": [msg]}

        results = []
        total_updated = 0
        total_cleaned = 0

        for sub in target_subs:
            try:
                tmdbid = getattr(sub, 'tmdbid', None)
                season = sub.season or 1
                sub_name = f"{sub.name} S{season:02d}"

                # =========================================================
                # 1. 从转存记录收集实际文件信息
                # =========================================================
                records = []
                with SessionFactory() as db:
                    try:
                        th_oper = TransferHistoryOper(db=db)
                        raw = th_oper.list_by_tmdbid(tmdbid) if hasattr(th_oper, 'list_by_tmdbid') else []
                        records = list(raw) if raw else []
                    except Exception:
                        pass
                    if not records:
                        rows = db.execute(
                            text("SELECT * FROM transferhistory WHERE tmdbid = :t ORDER BY id DESC LIMIT 100"),
                            {"t": tmdbid}
                        ).fetchall()
                        if rows:
                            col_names = [d[0] for d in db.execute(text("PRAGMA table_info(transferhistory)")).fetchall()]
                            for row in rows:
                                records.append(dict(zip(col_names, row)))

                # 按剧集整理实际文件信息 {ep: {name, size, filename}}
                ep_fileinfo = {}
                for rec in records:
                    ep_str = getattr(rec, 'episodes', None) if not isinstance(rec, dict) else rec.get('episodes')
                    sfi_raw = getattr(rec, 'src_fileitem', None) if not isinstance(rec, dict) else rec.get('src_fileitem')
                    if not ep_str or not sfi_raw:
                        continue
                    src_data = {}
                    if isinstance(sfi_raw, str):
                        try:
                            src_data = json.loads(sfi_raw)
                        except Exception:
                            continue
                    elif isinstance(sfi_raw, dict):
                        src_data = sfi_raw
                    else:
                        continue
                    fsize = int(src_data.get("size", 0))
                    fname = src_data.get("name", src_data.get("basename", ""))
                    if not fname or not fsize:
                        continue
                    ep_match = re.search(rf'S{season:02d}E(\d+)', fname, re.IGNORECASE)
                    if not ep_match:
                        ep_match = re.search(rf'S0*?E(\d+)', ep_str) if ep_str else None
                    if ep_match:
                        ep_num = ep_match.group(1)
                        if ep_num not in ep_fileinfo or fsize > ep_fileinfo[ep_num]['size']:
                            ep_fileinfo[ep_num] = {"name": fname, "size": fsize, "filename": fname}

                if not ep_fileinfo:
                    results.append(f"{sub_name}: 未找到转存记录，请先确保已转存过文件")
                    continue

                # =========================================================
                # 2. 扫描磁盘 strm 文件，确认哪些集还存在
                # =========================================================
                existing_eps = set()
                media_base = "/media"
                found = False
                for root, dirs, files in os.walk(media_base):
                    for f in files:
                        if not f.endswith('.strm'):
                            continue
                        rel = os.path.join(root, f).replace(media_base, "")
                        if f"tmdbid={tmdbid}" not in rel and sub.name not in rel:
                            continue
                        ep_match = re.search(rf'S{season:02d}E(\d+)', rel, re.IGNORECASE)
                        if ep_match:
                            existing_eps.add(ep_match.group(1))
                            found = True

                if not existing_eps:
                    results.append(f"{sub_name}: 磁盘未找到 strm 文件，跳过")
                    continue

                # =========================================================
                # 3. 评分：只用转存记录中的实际文件大小
                # =========================================================
                new_scores = {}
                rule_group_names = getattr(sub, 'filter_groups', None) or []
                if not rule_group_names:
                    from app.db.systemconfig_oper import SystemConfigOper, SystemConfigKey
                    rule_group_names = SystemConfigOper().get(SystemConfigKey.BestVersionFilterRuleGroups) or []

                fallback_rules = rule_group_names  # for logging

                for ep_key in sorted(existing_eps, key=int):
                    info = ep_fileinfo.get(ep_key)
                    if not info:
                        continue  # 有 strm 但没转存记录？跳过该集

                    fsize = info["size"]
                    fname = info["filename"]

                    try:
                        fake_mediainfo = MediaInfo(type=MediaType.TV)
                        fake_torrent = TorrentInfo(
                            title=fname,
                            size=fsize or 0,
                            description='',
                            labels=[]
                        )
                        filter_module = FilterModule()
                        filter_module.init_module()
                        matched = filter_module.filter_torrents(
                            rule_groups=rule_group_names,
                            torrent_list=[fake_torrent],
                            mediainfo=fake_mediainfo
                        )
                        if matched:
                            rule_score = matched[0].pri_order or 60
                            if rule_score < 60:
                                rule_score = 60
                        else:
                            rule_score = 60
                    except Exception as e:
                        logger.warning(f"[强制重评分] 规则组评分失败（回退基础分）: {e}")
                        rule_score = 40
                        if re.search(r'2160p|4K|UHD', fname, re.IGNORECASE):
                            rule_score += 20
                        elif re.search(r'1080p', fname, re.IGNORECASE):
                            rule_score += 10
                        if re.search(r'HDR|DV|DoVi|Dolby', fname, re.IGNORECASE):
                            rule_score += 15
                        if re.search(r'H265|HEVC|x265|H\\.265', fname, re.IGNORECASE):
                            rule_score += 10
                        rule_score = min(rule_score, 100)

                    # 综合评分：50%体积 + 50%画质
                    # 用 SyncHandler._calc_size_score 同款逻辑简化版
                    size_gb = fsize / (1024**3)
                    if size_gb >= 5:
                        size_score = 100
                    elif size_gb >= 3:
                        size_score = 80
                    elif size_gb >= 2:
                        size_score = 60
                    elif size_gb >= 1:
                        size_score = 40
                    else:
                        size_score = 20

                    normalized_rule = min(rule_score, 100)
                    final_score = int(size_score * 0.50 + normalized_rule * 0.50)
                    final_score = max(final_score, 60)  # 保底60分

                    new_scores[ep_key] = final_score
                    logger.debug(f"[强制重评分] {sub_name} E{ep_key}: 体积{size_gb:.1f}GB→{size_score}分×50% + 画质{rule_score}分×50% = {final_score}分")

                if not new_scores:
                    results.append(f"{sub_name}: 未能从转存记录解析到可评分的集")
                    continue

                # =========================================================
                # 4. 清理脏数据 + 写入新评分（强制覆盖）
                # =========================================================
                old_raw = getattr(sub, 'episode_priority', None)
                cleaned_eps = []
                if isinstance(old_raw, (str, dict)):
                    try:
                        old_data = json.loads(old_raw) if isinstance(old_raw, str) else old_raw
                        if isinstance(old_data, dict):
                            for ek in old_data:
                                if ek not in new_scores:
                                    cleaned_eps.append(ek)
                    except Exception:
                        pass

                SubscribeOper().update(sub.id, {"episode_priority": new_scores})
                total_updated += len(new_scores)
                total_cleaned += len(cleaned_eps)

                ep_list = sorted(new_scores.keys(), key=int)
                score_detail = ", ".join([f"E{e}={new_scores[e]}分" for e in ep_list[:15]])
                if len(ep_list) > 15:
                    score_detail += f"... 共{len(ep_list)}集"
                msg_parts = [f"{sub_name}: 已重评分{len(ep_list)}集 ({score_detail})"]
                if cleaned_eps:
                    msg_parts.append(f"清理了 {len(cleaned_eps)} 条脏数据: {', '.join(sorted(cleaned_eps, key=int))}")
                results.append("; ".join(msg_parts))
                logger.info(f"[强制重评分] {'; '.join(msg_parts)}")

            except Exception as e:
                results.append(f"{sub.name}: 出错 - {e}")
                logger.error(f"[强制重评分] 出错 {sub.name}: {e}")

        summary = f"处理了 {len(target_subs)} 个订阅，更新了 {total_updated} 集评分"
        if total_cleaned > 0:
            summary += f"，清理了 {total_cleaned} 条脏数据"
        return {
            "success": True,
            "message": summary + "\n" + "\n".join(results),
            "results": results
        }

    def api_force_re_score(self) -> dict:
        """API: 强制重评分 - 调用内部 _force_re_score()"""
        return self._force_re_score()


    def _apply_global_config_once(self):
        """安装确认后首次执行时，应用一次系统级配置。
        放在 sync_subscribes() 开头调用，确保插件加载成功（PIP 依赖已安装）后才修改 MP 系统配置。
        """
        if self._global_config_applied:
            return
        try:
            self._register_filter_rules()
            self._apply_naming_rules()
            self._apply_block_by_time()
            self._apply_best_version_all()
            self._apply_best_version_selected()
            # 批量评分（已选独立洗版订阅且ids有变化时自动触发）
            if self._upgrade_subscribe_ids:
                current_hash = str(sorted(str(i) for i in self._upgrade_subscribe_ids))
                if current_hash != self._last_scored_ids_hash:
                    logger.info("检测到独立洗版订阅列表有变化，自动触发整理记录评分")
                    self._batch_re_score()
                    self._last_scored_ids_hash = current_hash
            self._global_config_applied = True
            logger.info("✓ 插件全局配置已应用（过滤规则 / 命名模板 / 接管态 / 洗版）")
        except Exception as e:
            logger.error(f"插件全局配置应用失败（下次首次执行重试）: {e}")

    def sync_subscribes(self):
        # 首次成功运行时才应用系统级配置（避免安装失败却污染MP配置）
        self._apply_global_config_once()
        with lock:
            tz = pytz.timezone(settings.TZ)
            run_start = datetime.datetime.now(tz=tz)

            success = False
            try:
                success = self._do_sync()
            except Exception as e:
                logger.error(f"同步任务异常：{e}")
                success = False
            finally:
                if self._sync_handler:
                    # 自愈清理：每次同步都检查文件完整性（自愈间隔 by _self_heal_interval）
                    import time
                    if self._enable_cloud_upgrade:
                        now = time.time()
                        if now - getattr(self, '_last_cloud_cleanup', 0) > 86400:
                            self._last_cloud_cleanup = now
                            self._sync_handler.auto_upgrade_scan(source='cloud')
                    # 进度自愈：每次同步独立执行（不受 86400 限制）
                    self._sync_handler._self_heal_cleanup()
                    # 处理到期延迟删除
                    self._sync_handler.process_expired_deletions()
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

    @eventmanager.register(EventType.PluginAction)
    def remote_pt_search(self, event: Event):
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "pt_sub_search":
            return

        logger.info("收到命令，开始执行PT订阅搜索")
        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【PT订阅搜索】开始执行",
            text="已收到远程命令，正在搜索PT订阅...",
            userid=event_data.get("user")
        )

        # 屏蔽态时临时恢复站点（守护5分钟内自动回锁到[-1]）
        backup = self.get_data("subscribe_sites_backup") or {}
        if backup:
            logger.info("当前处于屏蔽态，临时恢复订阅站点以执行PT搜索（守护自动回锁）")
            self._init_subscribe_handler()
            with SessionFactory() as db:
                from app.db.subscribe_oper import SubscribeOper
                oper = SubscribeOper(db=db)
                for sid_str, site_ids in backup.items():
                    oper.update(int(sid_str), {"sites": site_ids})
                for s in oper.list() or []:
                    if str(s.id) not in backup and not self._is_subscribe_excluded(s.id):
                        if str(getattr(s, "sites", "[]")) == "[-1]":
                            oper.update(s.id, {"sites": None})
            logger.info(f"已恢复 {len(backup)} 个订阅的原始站点")

        try:
            SubscribeChain().search(state="R")
        except Exception as e:
            logger.error(f"PT订阅搜索失败: {e}")
            self.post_message(
                mtype=NotificationType.Plugin,
                channel=event_data.get("channel"),
                title="【PT订阅搜索】执行失败",
                text=f"搜索出错：{str(e)}",
                userid=event_data.get("user")
            )
            return

        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【PT订阅搜索】执行完成",
            text="PT订阅搜索已完成。",
            userid=event_data.get("user")
        )

