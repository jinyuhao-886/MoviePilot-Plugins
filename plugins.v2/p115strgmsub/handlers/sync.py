"""
同步处理模块
负责核心的同步逻辑：处理电影订阅、处理电视剧订阅
"""
import datetime
from typing import List, Dict, Any, Set, Optional, Callable
import json
import os
import threading
from pathlib import Path

from app.core.config import global_vars
from app.core.metainfo import MetaInfo
from app.chain.download import DownloadChain
from app.db import SessionFactory
from sqlalchemy import text
from app.db.subscribe_oper import SubscribeOper
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.log import logger
from app.schemas import MediaInfo
from app.schemas.types import MediaType, NotificationType
from app.utils.string import StringUtils

from ..utils import FileMatcher, SubscribeFilter
from .search import SearchHandler
from .subscribe import SubscribeHandler


class SyncHandler:
    """同步处理器"""

    def __init__(
        self,
        p115_manager,
        search_handler: SearchHandler,
        subscribe_handler: SubscribeHandler,
        chain,
        save_path: str,
        movie_save_path: str,
        max_transfer_per_sync: int = 50,
        batch_size: int = 20,
        skip_other_season_dirs: bool = True,
        notify: bool = False,
        post_message_func: Callable = None,
        get_data_func: Callable = None,
        save_data_func: Callable = None,
        min_upgrade_tiers: int = 2,
        self_heal_interval: int = 10,
        enable_cloud_upgrade: bool = False,
        enable_pt_upgrade: bool = False,
        auto_best_version: bool = False,
        cloud_tv_local_dir: str = "",
        cloud_tv_remote_dir: str = "",
        cloud_movie_local_dir: str = "",
        cloud_movie_remote_dir: str = "",
        frame_rate_pattern: str = r"60fps|120fps",
        bit_rate_pattern: str = r"TrueHD|DTS-HD|DTS5\.1|ATMOS|LPCM|FLAC",
        vivid_pattern: str = r"HDR[._ ]?[Vv]ivid|菁彩影像|HDRVivid",
        upgrade_mode: str = "smart",
        upgrade_threshold: int = 25
    ):
        """
        初始化同步处理器

        :param p115_manager: 115 客户端管理器
        :param search_handler: 搜索处理器
        :param subscribe_handler: 订阅处理器
        :param chain: MediaChain 实例
        :param save_path: 电视剧转存目录
        :param movie_save_path: 电影转存目录
        :param max_transfer_per_sync: 单次同步最大转存数量
        :param batch_size: 批量转存每批文件数
        :param skip_other_season_dirs: 跳过其他季目录
        :param notify: 是否发送通知
        :param post_message_func: 发送消息的函数
        :param get_data_func: 获取数据的函数
        :param save_data_func: 保存数据的函数
        :param min_upgrade_tiers: 最小洗版层级差
        :param self_heal_interval: 自愈检查间隔（分钟）
        :param enable_cloud_upgrade: 启用网盘洗版
        :param enable_pt_upgrade: 启用PT洗版
        :param auto_best_version: 自动开启原生洗版
        :param cloud_tv_local_dir: 本地电视剧strm根目录（网盘洗版用）
        :param cloud_tv_remote_dir: 115网盘电视剧目录（网盘洗版用）
        :param cloud_movie_local_dir: 本地电影strm根目录（网盘洗版用）
        :param cloud_movie_remote_dir: 115网盘电影目录（网盘洗版用）
        :param frame_rate_pattern: 帧率正则，匹配加 100 分
        :param bit_rate_pattern: 比特深度正则，匹配加 100 分（已保留但后续改用 bit_depth_pattern）
        :param vivid_pattern: HDR Vivid 加分正则，匹配在 effect 基础上额外 +50
        """
        self._p115_manager = p115_manager
        self._search_handler = search_handler
        self._subscribe_handler = subscribe_handler
        self._chain = chain
        self._save_path = save_path
        self._movie_save_path = movie_save_path
        self._max_transfer_per_sync = max_transfer_per_sync
        self._batch_size = batch_size
        self._skip_other_season_dirs = skip_other_season_dirs
        self._notify = notify
        self._post_message = post_message_func
        self._get_data = get_data_func
        self._save_data = save_data_func
        self._min_upgrade_tiers = min_upgrade_tiers
        self._self_heal_interval = self_heal_interval
        self._enable_cloud_upgrade = enable_cloud_upgrade
        self._enable_pt_upgrade = enable_pt_upgrade
        self._auto_best_version = auto_best_version
        self._cloud_tv_local_dir = cloud_tv_local_dir or ""
        self._cloud_tv_remote_dir = cloud_tv_remote_dir or ""
        self._cloud_movie_local_dir = cloud_movie_local_dir or ""
        self._cloud_movie_remote_dir = cloud_movie_remote_dir or ""
        self._frame_rate_pattern = frame_rate_pattern or r"60fps|120fps"
        self._bit_depth_pattern = bit_rate_pattern or r"10bit|12bit|10-bit"
        self._vivid_pattern = vivid_pattern or r"HDR[._ ]?[Vv]ivid|菁彩影像|HDRVivid"
        self._upgrade_mode = upgrade_mode
        self._upgrade_threshold = upgrade_threshold

        # 延迟删除队列配置
        self._pending_delay = 60  # 1分钟延迟
        self._pending_key = "pending_deletions_v2"

    def process_movie_subscribe(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int
    ) -> int:
        """
        处理单个电影订阅

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :return: 更新后的转存数量
        """
        try:
            logger.info(f"处理电影订阅：{subscribe.name} ({subscribe.year})")

            # 加载该订阅的历史积分花费（用 tmdb_id 作为唯一标识）
            sub_key = f"tmdb_{subscribe.tmdbid}_movie" if subscribe.tmdbid else f"{subscribe.name}_movie"
            if hasattr(self._search_handler, 'reset_sub_spent_points'):
                self._search_handler.reset_sub_spent_points(sub_key)

            # 检查历史记录是否已成功转存
            movie_history_score = -1  # -1 表示未转存过
            movie_perfect_match = False
            for h in history:
                if (h.get("title") == subscribe.name
                        and h.get("type") == "电影"
                        and h.get("status") == "成功"):
                    score = h.get("filter_score", 0)
                    perfect = h.get("perfect_match", False)
                    if score > movie_history_score:
                        movie_history_score = score
                        movie_perfect_match = perfect

            # best_version=1 表示开启洗版（非严格模式）
            is_best_version = bool(subscribe.best_version)

            if movie_history_score >= 0:
                if not is_best_version or movie_perfect_match:
                    logger.info(f"电影 {subscribe.name} 已在历史记录中(洗版:{is_best_version}, 完美匹配:{movie_perfect_match})，跳过")
                    return transferred_count
                else:
                    logger.info(f"电影 {subscribe.name} 洗版中，历史分数 {movie_history_score}，尝试寻找更优资源")

            # 生成元数据
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.type = MediaType.MOVIE

            # 识别媒体信息
            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=MediaType.MOVIE,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True
            )
            if not mediainfo:
                logger.warn(f"无法识别媒体信息：{subscribe.name}")
                return transferred_count

            # 搜索网盘资源
            p115_results = self._search_handler.search_resources(
                mediainfo=mediainfo,
                media_type=MediaType.MOVIE
            )

            if not p115_results:
                logger.info(f"未找到电影 {mediainfo.title} 的 115 网盘资源")
                return transferred_count

            logger.info(f"找到 {len(p115_results)} 个 115 网盘资源")

            # 创建订阅过滤条件
            # exclude/filter 是硬拒绝：命中即丢弃
            effective_exclude = getattr(subscribe, 'exclude', None)
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality,
                resolution=subscribe.resolution,
                effect=subscribe.effect,
                include=getattr(subscribe, 'include', None),
                exclude=effective_exclude,
                filter=getattr(subscribe, 'filter', None),
                framerate=self._frame_rate_pattern,
                bit_depth=self._bit_depth_pattern,
                vivid_pattern=self._vivid_pattern,
                strict=not is_best_version
            )
            if subscribe_filter.has_filters():
                mode_text = "洗版模式" if is_best_version else "严格模式"
                logger.info(f"电影 {subscribe.name} 过滤条件({mode_text}) - 质量: {subscribe.quality}, 分辨率: {subscribe.resolution}, 特效: {subscribe.effect}")

            # 遍历搜索结果，尝试找到并转存电影
            movie_transferred = False
            for resource in p115_results:
                if movie_transferred:
                    break

                share_url = resource.get("url", "")
                resource_title = resource.get("title", "")

                # 检查是否是刚搜索出尚未真正解锁的延期解锁 HDHive 资源
                if resource.get("need_unlock") and not share_url:
                    slug = resource.get("slug")
                    if slug:
                        logger.info(f"遇到需要解锁的收费资源 {resource_title} (slug: {slug})，尝试消耗积分解锁...")
                        unlocked_url = self._search_handler.unlock_hdhive_resource(slug, resource.get("unlock_points", 0))
                        if not unlocked_url:
                            logger.error(f"未能解锁收费资源: {resource_title}")
                            continue
                        share_url = unlocked_url
                        # 更新当前字典以便历史存入或下次能沿用这个 url
                        resource["url"] = share_url
                        resource["need_unlock"] = False

                if not share_url:
                    continue

                logger.info(f"检查分享：{resource_title} - {share_url}")

                try:
                    # 先检查分享链接是否有效
                    share_status = self._p115_manager.check_share_status(share_url)
                    if not share_status.is_valid:
                        logger.warning(f"分享链接无效：{share_url}，原因：{share_status.status_text}")
                        continue

                    share_files = self._p115_manager.list_share_files(share_url)
                    if not share_files:
                        logger.info(f"分享链接无内容：{share_url}")
                        continue

                    # 匹配电影文件
                    matched_file = FileMatcher.match_movie_file(
                        share_files, mediainfo.title,
                        subscribe_filter=subscribe_filter
                    )

                    if matched_file:
                        file_name = matched_file.get('name', '')
                        logger.info(f"找到匹配文件：{file_name}")

                        # 计算当前文件的过滤分数和是否完美匹配
                        _, current_score = subscribe_filter.match(file_name) if subscribe_filter.has_filters() else (True, 0)
                        is_perfect = subscribe_filter.is_perfect_match(file_name) if subscribe_filter.has_filters() else True

                        # 洗版模式下检查是否需要升级资源
                        if is_best_version and movie_history_score >= 0:
                            if current_score <= movie_history_score:
                                logger.info(f"电影 {mediainfo.title} 已有分数 {movie_history_score}，当前 {current_score}，跳过")
                                continue
                            else:
                                logger.info(f"电影 {mediainfo.title} 洗版：旧分数 {movie_history_score} -> 新分数 {current_score}")

                        # 构建转存路径
                        save_dir = f"{self._movie_save_path}/{mediainfo.title} ({mediainfo.year})" if mediainfo.year else f"{self._movie_save_path}/{mediainfo.title}"
                        logger.info(f"转存目标路径: {save_dir}")

                        # 执行转存
                        success = self._p115_manager.transfer_file(
                            share_url=share_url,
                            file_id=matched_file.get("id"),
                            save_path=save_dir
                        )

                        # 记录历史
                        history_item = {
                            "title": mediainfo.title,
                            "year": mediainfo.year,
                            "type": "电影",
                            "status": "成功" if success else "失败",
                            "share_url": share_url,
                            "file_name": file_name,
                            "filter_score": current_score,
                            "perfect_match": is_perfect,
                            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        history.append(history_item)

                        if success:
                            transferred_count += 1
                            movie_transferred = True
                            movie_history_score = current_score
                            score_info = f"(分数:{current_score}, 完美匹配:{is_perfect})" if subscribe_filter.has_filters() else ""
                            logger.info(f"成功转存电影：{mediainfo.title} {score_info}")

                            # 收集转存详情用于通知
                            transfer_details.append({
                                "type": "电影",
                                "title": mediainfo.title,
                                "year": mediainfo.year,
                                "image": mediainfo.get_poster_image(),
                                "file_name": file_name
                            })

                            # 添加下载历史记录
                            try:
                                DownloadHistoryOper().add(
                                    path=save_dir,
                                    type=mediainfo.type.value,
                                    title=mediainfo.title,
                                    year=mediainfo.year,
                                    tmdbid=mediainfo.tmdb_id,
                                    imdbid=mediainfo.imdb_id,
                                    tvdbid=mediainfo.tvdb_id,
                                    doubanid=mediainfo.douban_id,
                                    image=mediainfo.get_poster_image(),
                                    downloader="115网盘",
                                    download_hash=matched_file.get("id"),
                                    torrent_name=resource_title,
                                    torrent_description=file_name,
                                    torrent_site="115网盘",
                                    username="P115StrgmSub",
                                    date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    note={"source": f"Subscribe|{subscribe.name}", "share_url": share_url}
                                )
                                logger.debug(f"已记录电影 {mediainfo.title} 下载历史")
                            except Exception as e:
                                logger.warning(f"记录下载历史失败：{e}")

                            # 电影转存成功后完成订阅
                            self._subscribe_handler.check_and_finish_subscribe(
                                subscribe=subscribe,
                                mediainfo=mediainfo,
                                success_episodes=[1]
                            )
                            # 订阅完成，清除该订阅的历史积分记录
                            if hasattr(self._search_handler, 'clear_sub_points'):
                                self._search_handler.clear_sub_points(sub_key)
                        else:
                            logger.error(f"转存失败：{mediainfo.title}")

                except Exception as e:
                    logger.error(f"处理分享链接出错：{share_url}, 错误：{str(e)}")
                    continue

        except Exception as e:
            logger.error(f"处理电影订阅 {subscribe.name} 出错：{str(e)}")

        return transferred_count

    def process_tv_subscribe(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int,
        exclude_ids: Set[int]
    ) -> int:
        """
        处理单个电视剧订阅

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :param exclude_ids: 排除的订阅ID集合
        :return: 更新后的转存数量
        """
        try:
            # 洗版模式派发到独立转存逻辑
            if bool(subscribe.best_version):
                return self._process_tv_subscribe_upgrade(
                    subscribe=subscribe,
                    history=history,
                    transfer_details=transfer_details,
                    transferred_count=transferred_count,
                    exclude_ids=exclude_ids
                )

            logger.info(f"订阅信息：{subscribe.name}，开始集数：{subscribe.start_episode}, 总集数：{subscribe.total_episode}, 缺失集数：{subscribe.lack_episode}")
            logger.info(f"处理订阅：{subscribe.name} (S{subscribe.season or 1})")

            # 加载该订阅的历史积分花费（用 tmdb_id + 季数作为唯一标识）
            sub_key = f"tmdb_{subscribe.tmdbid}_S{subscribe.season or 1}" if subscribe.tmdbid else f"{subscribe.name}_S{subscribe.season or 1}"
            if hasattr(self._search_handler, 'reset_sub_spent_points'):
                self._search_handler.reset_sub_spent_points(sub_key)

            # 早期检查：如果订阅显示没有缺失集数，跳过处理
            if subscribe.lack_episode == 0:
                logger.info(f"{subscribe.name} S{subscribe.season or 1} 订阅显示媒体库已完整(lack_episode=0)，跳过")
                return transferred_count

            # 生成元数据
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.begin_season = subscribe.season or 1
            meta.type = MediaType.TV

            # 识别媒体信息
            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta,
                mtype=MediaType.TV,
                tmdbid=subscribe.tmdbid,
                doubanid=subscribe.doubanid,
                cache=True
            )

            if not mediainfo:
                logger.warn(f"无法识别媒体信息：{subscribe.name}")
                return transferred_count

            # 构造总集数信息
            totals = {}
            if subscribe.season and subscribe.total_episode:
                totals = {subscribe.season: subscribe.total_episode}

            # 获取缺失剧集
            downloadchain = DownloadChain()
            exist_flag, no_exists = downloadchain.get_no_exists_info(
                meta=meta,
                mediainfo=mediainfo,
                totals=totals
            )

            if exist_flag:
                logger.info(f"{mediainfo.title_year} S{meta.begin_season} 媒体库中已完整存在")
                # 媒体库已完整，调用完成订阅逻辑
                total_ep = subscribe.total_episode or 0
                start_ep = subscribe.start_episode or 1
                if total_ep > 0:
                    all_episodes = list(range(start_ep, total_ep + 1))
                    self._subscribe_handler.check_and_finish_subscribe(
                        subscribe=subscribe,
                        mediainfo=mediainfo,
                        success_episodes=all_episodes
                    )
                elif subscribe.lack_episode != 0:
                    SubscribeOper().update(subscribe.id, {"lack_episode": 0})
                # 订阅已完整，清除历史积分记录
                if hasattr(self._search_handler, 'clear_sub_points'):
                    self._search_handler.clear_sub_points(sub_key)
                return transferred_count

            # 获取缺失的集数列表
            season = meta.begin_season or 1
            missing_episodes = []
            mediakey = mediainfo.tmdb_id or mediainfo.douban_id

            if no_exists and mediakey:
                season_info = no_exists.get(mediakey, {})
                not_exist_info = season_info.get(season)
                if not_exist_info:
                    missing_episodes = not_exist_info.episodes or []
                    if not missing_episodes and not_exist_info.total_episode:
                        start_ep = not_exist_info.start_episode or 1
                        missing_episodes = list(range(start_ep, not_exist_info.total_episode + 1))

            if not missing_episodes:
                logger.info(f"{mediainfo.title_year} S{season} 没有缺失剧集信息")
                return transferred_count

            # 过滤掉小于开始集数的剧集
            if subscribe.start_episode:
                original_count = len(missing_episodes)
                missing_episodes = [ep for ep in missing_episodes if ep >= subscribe.start_episode]
                if len(missing_episodes) < original_count:
                    logger.info(f"根据订阅设置，过滤掉小于 {subscribe.start_episode} 的剧集")

            # best_version=1 表示开启洗版
            is_best_version = bool(subscribe.best_version)

            # 从历史记录中排除已成功转存的集数
            transferred_episodes = set()
            episode_history_scores: Dict[int, int] = {}
            for h in history:
                if (h.get("title") == mediainfo.title
                        and h.get("season") == season
                        and h.get("status") == "成功"):
                    ep = h.get("episode")
                    score = h.get("filter_score", 0)
                    perfect = h.get("perfect_match", False)

                    if not is_best_version:
                        transferred_episodes.add(ep)
                    else:
                        if perfect:
                            transferred_episodes.add(ep)
                        else:
                            if ep not in episode_history_scores or score > episode_history_scores[ep]:
                                episode_history_scores[ep] = score

            # 构建转存路径（标题 + 年份，格式如 "权力的游戏 (2011)"）
            show_folder = f"{mediainfo.title} ({mediainfo.year})" if mediainfo.year else mediainfo.title
            save_dir = f"{self._save_path}/{show_folder}/Season {season}"

            # 检查网盘目录中已存在的剧集
            existing_episodes_in_cloud = FileMatcher.check_existing_episodes(
                self._p115_manager, mediainfo, season, save_dir
            )

            # 合并已存在的集数
            all_existing = transferred_episodes | existing_episodes_in_cloud

            # 洗版模式下，需要升级的集数不应该被排除
            if is_best_version and episode_history_scores:
                episodes_to_upgrade = set(episode_history_scores.keys())
                all_existing = all_existing - episodes_to_upgrade
                if episodes_to_upgrade:
                    logger.info(f"{mediainfo.title_year} S{season} 洗版模式：{len(episodes_to_upgrade)} 集待升级")

            if all_existing:
                missing_episodes = [ep for ep in missing_episodes if ep not in all_existing]
                logger.info(
                    f"{mediainfo.title_year} S{season} 跳过已存在的 {len(all_existing)} 集 "
                    f"(历史记录:{len(transferred_episodes)}, 网盘:{len(existing_episodes_in_cloud)})"
                )

            if not missing_episodes:
                logger.info(f"{mediainfo.title_year} S{season} 所有缺失剧集已存在于网盘")
                # 网盘中已存在所有缺失集数，更新订阅状态
                if existing_episodes_in_cloud:
                    self._subscribe_handler.check_and_finish_subscribe(
                        subscribe=subscribe,
                        mediainfo=mediainfo,
                        success_episodes=list(existing_episodes_in_cloud)
                    )
                    # 缺失集数已全部补齐，清除历史积分记录
                    if hasattr(self._search_handler, 'clear_sub_points'):
                        self._search_handler.clear_sub_points(sub_key)
                return transferred_count

            # 过滤掉尚未播出的剧集，避免浪费搜索和解锁资源
            if mediainfo.tmdb_id:
                try:
                    from app.chain.tmdb import TmdbChain
                    tmdb_episodes = TmdbChain().tmdb_episodes(
                        tmdbid=mediainfo.tmdb_id, season=season
                    )
                    if tmdb_episodes:
                        today = datetime.date.today().isoformat()
                        aired_episodes = set()
                        for ep in tmdb_episodes:
                            if ep.air_date and ep.air_date <= today and ep.episode_number:
                                aired_episodes.add(ep.episode_number)
                        if aired_episodes:
                            not_aired = [ep for ep in missing_episodes if ep not in aired_episodes]
                            if not_aired:
                                missing_episodes = [ep for ep in missing_episodes if ep in aired_episodes]
                                logger.info(
                                    f"{mediainfo.title_year} S{season} 跳过 {len(not_aired)} 集未播出剧集：{not_aired}"
                                )
                                if not missing_episodes:
                                    logger.info(f"{mediainfo.title_year} S{season} 所有缺失剧集均未播出，跳过")
                                    return transferred_count
                        else:
                            logger.info(f"{mediainfo.title_year} S{season} TMDB剧集播出日期数据为空，跳过播出过滤")
                    else:
                        logger.info(f"{mediainfo.title_year} S{season} TMDB未返回剧集信息，跳过播出过滤")
                except Exception as e:
                    logger.warning(f"{mediainfo.title_year} S{season} 查询TMDB剧集播出日期失败：{e}，将继续处理所有缺失剧集")

            logger.info(f"{mediainfo.title_year} S{season} 待转存剧集：{missing_episodes}")

            # 创建订阅过滤条件
            # exclude/filter 是硬拒绝：命中即丢弃
            effective_exclude = getattr(subscribe, 'exclude', None)
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality,
                resolution=subscribe.resolution,
                effect=subscribe.effect,
                include=getattr(subscribe, 'include', None),
                exclude=effective_exclude,
                filter=getattr(subscribe, 'filter', None),
                framerate=self._frame_rate_pattern,
                bit_depth=self._bit_depth_pattern,
                vivid_pattern=self._vivid_pattern,
                strict=not is_best_version
            )
            if subscribe_filter.has_filters():
                mode_text = "洗版模式" if is_best_version else "严格模式"
                logger.info(f"{mediainfo.title} S{season} 过滤条件({mode_text}) - 质量: {subscribe.quality}, 分辨率: {subscribe.resolution}, 特效: {subscribe.effect}")

            # 成功转存的集数列表
            success_episodes = []

            # 智能回退搜索：按源迭代
            enabled_sources = self._search_handler.get_enabled_sources()

            if not enabled_sources:
                logger.warning(f"没有可用的搜索源，跳过 {mediainfo.title} S{season} 的搜索")
                return transferred_count

            for source_index, source in enumerate(enabled_sources):
                if not missing_episodes:
                    logger.info(f"{mediainfo.title_year} S{season} 所有缺失剧集已转存完成，不再查询后续源")
                    break

                if transferred_count >= self._max_transfer_per_sync:
                    logger.info(f"已达单次同步上限 {self._max_transfer_per_sync}，剩余 {len(missing_episodes)} 集将在下次同步处理")
                    break

                logger.info(f"[{source.upper()}] 开始搜索 {mediainfo.title} S{season}（当前缺失: {len(missing_episodes)} 集）")

                # 搜索当前源
                p115_results = self._search_handler.search_single_source(
                    source=source,
                    mediainfo=mediainfo,
                    media_type=MediaType.TV,
                    season=season
                )

                if not p115_results:
                    remaining_sources = enabled_sources[source_index + 1:]
                    if remaining_sources:
                        logger.info(f"[{source.upper()}] 未找到资源，将尝试下一个源: {remaining_sources[0].upper()}")
                    else:
                        logger.info(f"[{source.upper()}] 未找到资源，已无更多可用源")
                    continue

                logger.info(f"[{source.upper()}] 找到 {len(p115_results)} 个 115 网盘资源")

                # 遍历搜索结果
                for resource in p115_results:
                    if transferred_count >= self._max_transfer_per_sync:
                        logger.info(f"已达单次同步上限 {self._max_transfer_per_sync}，剩余 {len(missing_episodes)} 集将在下次同步处理")
                        break

                    share_url = resource.get("url", "")
                    resource_title = resource.get("title", "")

                    # 检查是否是刚搜索出尚未真正解锁的延期解锁 HDHive 资源
                    if resource.get("need_unlock") and not share_url:
                        slug = resource.get("slug")
                        if slug:
                            logger.info(f"遇到需要解锁的收费资源 {resource_title} (slug: {slug})，尝试消耗积分解锁...")
                            unlocked_url = self._search_handler.unlock_hdhive_resource(slug, resource.get("unlock_points", 0))
                            if not unlocked_url:
                                logger.error(f"未能解锁收费资源: {resource_title}")
                                continue
                            share_url = unlocked_url
                            # 更新当前字典以便存入历史或记录这个 url
                            resource["url"] = share_url
                            resource["need_unlock"] = False

                    if not share_url:
                        continue

                    logger.info(f"检查分享：{resource_title} - {share_url}")

                    try:
                        # 检查分享链接是否有效
                        share_status = self._p115_manager.check_share_status(share_url)
                        if not share_status.is_valid:
                            logger.warning(f"分享链接无效：{share_url}，原因：{share_status.status_text}")
                            continue

                        # 列出分享内容
                        share_files = self._p115_manager.list_share_files(
                            share_url,
                            target_season=(season if self._skip_other_season_dirs else None)
                        )
                        if not share_files:
                            logger.info(f"分享链接无内容：{share_url}")
                            continue

                        logger.info(f"分享包含 {len(share_files)} 个文件/目录")

                        # 收集该分享中所有匹配的文件
                        matched_items = []

                        for episode in missing_episodes[:]:
                            matched_file = FileMatcher.match_episode_file(
                                share_files,
                                mediainfo.title,
                                season,
                                episode,
                                subscribe_filter=subscribe_filter
                            )

                            if matched_file:
                                file_name = matched_file.get('name', '')
                                logger.info(f"找到匹配文件：{file_name} -> E{episode:02d}")

                                _, current_score = subscribe_filter.match(file_name) if subscribe_filter.has_filters() else (True, 0)
                                is_perfect = subscribe_filter.is_perfect_match(file_name) if subscribe_filter.has_filters() else True

                                is_upgrade = False
                                if is_best_version and episode in episode_history_scores:
                                    old_score = episode_history_scores[episode]
                                    if current_score <= old_score:
                                        logger.info(f"E{episode:02d} 已有分数 {old_score}，当前 {current_score}，跳过")
                                        continue
                                    else:
                                        logger.info(f"E{episode:02d} 洗版：旧分数 {old_score} -> 新分数 {current_score}")
                                        is_upgrade = True

                                matched_items.append({
                                    "file": matched_file,
                                    "episode": episode,
                                    "score": current_score,
                                    "is_perfect": is_perfect,
                                    "is_upgrade": is_upgrade
                                })

                        if not matched_items:
                            logger.info(f"该分享未匹配到 S{season} 的任何缺失剧集，可能是季数不匹配或文件名无法识别")
                            continue

                        # 检查转存配额限制
                        remaining_quota = self._max_transfer_per_sync - transferred_count
                        if len(matched_items) > remaining_quota:
                            logger.info(f"匹配 {len(matched_items)} 集，但受配额限制仅转存 {remaining_quota} 集")
                            matched_items = matched_items[:remaining_quota]

                        # 批量转存
                        file_ids = [item["file"]["id"] for item in matched_items]
                        logger.info(f"准备批量转存 {len(file_ids)} 个文件到: {save_dir}")

                        success_ids, failed_ids = self._p115_manager.transfer_files_batch(
                            share_url=share_url,
                            file_ids=file_ids,
                            save_path=save_dir,
                            batch_size=self._batch_size
                        )

                        success_id_set = set(success_ids)
                        batch_success_episodes = []

                        # 处理结果
                        for item in matched_items:
                            file_id = item["file"]["id"]
                            episode = item["episode"]
                            file_name = item["file"]["name"]
                            current_score = item["score"]
                            is_perfect = item["is_perfect"]
                            is_upgrade = item["is_upgrade"]
                            success = file_id in success_id_set

                            history_item = {
                                "title": mediainfo.title,
                                "season": season,
                                "episode": episode,
                                "type": "电视剧",
                                "status": "成功" if success else "失败",
                                "share_url": share_url,
                                "file_name": file_name,
                                "filter_score": current_score,
                                "perfect_match": is_perfect,
                                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            history.append(history_item)

                            if success:
                                transferred_count += 1
                                episode_history_scores[episode] = current_score

                                if episode in missing_episodes:
                                    missing_episodes.remove(episode)

                                if not is_upgrade:
                                    success_episodes.append(episode)

                                score_info = f"(分数:{current_score}, 完美匹配:{is_perfect})" if subscribe_filter.has_filters() else ""
                                upgrade_info = " [洗版升级]" if is_upgrade else ""
                                logger.info(f"成功转存：{mediainfo.title} S{season:02d}E{episode:02d} {score_info}{upgrade_info}")

                                # 收集转存详情
                                existing_detail = next(
                                    (d for d in transfer_details
                                     if d.get("title") == mediainfo.title and d.get("season") == season),
                                    None
                                )
                                if existing_detail:
                                    existing_detail["episodes"].append(episode)
                                else:
                                    transfer_details.append({
                                        "type": "电视剧",
                                        "title": mediainfo.title,
                                        "year": mediainfo.year,
                                        "season": season,
                                        "episodes": [episode],
                                        "image": mediainfo.get_poster_image()
                                    })

                                batch_success_episodes.append(episode)
                            else:
                                logger.error(f"转存失败：{mediainfo.title} S{season:02d}E{episode:02d}")

                        # 记录下载历史
                        if batch_success_episodes:
                            try:
                                episodes_str = StringUtils.format_ep(batch_success_episodes)
                                DownloadHistoryOper().add(
                                    path=save_dir,
                                    type=mediainfo.type.value,
                                    title=mediainfo.title,
                                    year=mediainfo.year,
                                    tmdbid=mediainfo.tmdb_id,
                                    imdbid=mediainfo.imdb_id,
                                    tvdbid=mediainfo.tvdb_id,
                                    doubanid=mediainfo.douban_id,
                                    seasons=f"S{season:02d}",
                                    episodes=episodes_str,
                                    image=mediainfo.get_poster_image(),
                                    downloader="115网盘",
                                    download_hash=share_url,
                                    torrent_name=resource_title,
                                    torrent_site="115网盘",
                                    username="P115StrgmSub",
                                    date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    note={"source": f"Subscribe|{subscribe.name}", "share_url": share_url}
                                )
                                logger.debug(f"已记录 {mediainfo.title} S{season:02d} {episodes_str} 下载历史")
                            except Exception as e:
                                logger.warning(f"记录下载历史失败：{e}")

                        if not missing_episodes:
                            break

                    except Exception as e:
                        logger.error(f"处理分享链接出错：{share_url}, 错误：{str(e)}")
                        continue

                # 当前源处理完成
                if missing_episodes:
                    remaining_sources = enabled_sources[source_index + 1:]
                    if remaining_sources:
                        logger.info(f"[{source.upper()}] 处理完成，仍有 {len(missing_episodes)} 集缺失，继续查询下一个源: {remaining_sources[0].upper()}")
                    else:
                        logger.info(f"[{source.upper()}] 处理完成，仍有 {len(missing_episodes)} 集缺失，已无更多可用源")

            # 更新订阅状态
            # 将网盘已存在的集数和本次成功转存的集数合并
            all_success_episodes = list(set(success_episodes) | existing_episodes_in_cloud)
            if all_success_episodes:
                self._subscribe_handler.check_and_finish_subscribe(
                    subscribe=subscribe,
                    mediainfo=mediainfo,
                    success_episodes=all_success_episodes
                )
                # 如果订阅已完成（缺失集数归零），清除该订阅的历史积分记录
                total_ep = subscribe.total_episode or 0
                start_ep = subscribe.start_episode or 1
                if total_ep > 0:
                    expected = set(range(start_ep, total_ep + 1))
                    downloaded = set(subscribe.note or []).union(set(all_success_episodes))
                    if not (expected - downloaded):
                        if hasattr(self._search_handler, 'clear_sub_points'):
                            self._search_handler.clear_sub_points(sub_key)

        except Exception as e:
            logger.error(f"处理订阅 {subscribe.name} 出错：{str(e)}")

        return transferred_count

    # ==================== 洗版体积评分 ====================

    @staticmethod
    def _query_file_size_from_db(tmdbid: int, season: int, episode: int) -> int:
        """
        从 MP transferhistory 表查询文件大小
        仅支持走 MP 整理流程的转存文件，分享链接生成的 strm 查不到返回 0
        """
        try:
            from app.db import SessionFactory
            from sqlalchemy import text
            seasons_str = f"S{season:02d}"
            episodes_str = f"E{episode:02d}"
            with SessionFactory() as db:
                result = db.execute(
                    text(
                        "SELECT json_extract(src_fileitem, '$.size') "
                        "FROM transferhistory "
                        "WHERE tmdbid = :tmdbid AND seasons = :seasons AND episodes = :episodes "
                        "ORDER BY id DESC LIMIT 1"
                    ),
                    {"tmdbid": tmdbid, "seasons": seasons_str, "episodes": episodes_str}
                ).scalar()
                return int(result) if result else 0
        except Exception as e:
            logger.warning(f"查询文件大小失败 tmdbid={tmdbid} S{season}E{episode}: {e}")
            return 0

    @staticmethod
    def _calc_size_score(existing_size: int, candidate_size: int) -> int:
        """
        计算体积得分 (0~100)
        候选文件比现有文件大多少分
        """
        if existing_size <= 0:
            return 0  # 无现有文件则体积分为0
        ratio = candidate_size / existing_size
        if ratio < 0.85:
            return -50  # 明显变小，淘汰
        if ratio < 1.0:
            return 0
        if ratio < 1.15:
            return 30
        if ratio < 1.30:
            return 60
        if ratio < 1.50:
            return 80
        return 100

    @staticmethod
    def _calc_total_upgrade_score(
        rule_score: int,
        existing_size: int,
        candidate_size: int,
        mode: str = "smart"
    ) -> int:
        """
        计算综合洗版评分 (0~100)
        :param rule_score: MP 规则组评分 (93-100)，来自 _get_mp_rule_score()
        :param existing_size: 现有文件大小（字节）
        :param candidate_size: 候选文件大小（字节）
        :param mode: 'simple'=纯体积, 'smart'=体积×0.75+画质×0.25
        """
        size_score = SyncHandler._calc_size_score(existing_size, candidate_size)
        if mode == "simple":
            return max(size_score, 0)
        else:
            normalized_rule = min(rule_score, 100)  # 已在 93-100 范围
            total = size_score * 0.75 + normalized_rule * 0.25
            return max(int(total), 0)

    @staticmethod
    def _get_mp_rule_score(filename: str, filesize: int, subscribe, season: int) -> int:
        """
        使用 MP 原生规则组评分，与 PT 选种同源。
        :return: pri_order (93-100), 规则组无匹配时返回 60
        """
        try:
            from app.schemas import TorrentInfo
            from app.core.context import MediaInfo
            from app.modules.filter import FilterModule
            from app.schemas.types import MediaType

            rule_group_names = getattr(subscribe, 'filter_groups', None) or []
            if not rule_group_names:
                from app.db.systemconfig_oper import SystemConfigOper, SystemConfigKey
                rule_group_names = SystemConfigOper().get(SystemConfigKey.BestVersionFilterRuleGroups) or []

            fake_mediainfo = MediaInfo(type=MediaType.TV)
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
                return score if score >= 60 else 60
            return 60
        except Exception as e:
            logger.warning(f"MP规则组评分失败（回退基础分）: {e}")
            return 60

    def _read_ep_priority(self, subscribe) -> dict:
        """读取 episode_priority，返回纯 int 格式 {ep: score}"""
        raw = getattr(subscribe, 'episode_priority', None) or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}
        result = {}
        if isinstance(raw, dict):
            for key, val in raw.items():
                if isinstance(val, dict):
                    result[key] = int(val.get("score", 0))
                elif isinstance(val, (int, float)):
                    result[key] = int(val)
                else:
                    result[key] = 0
        return result

    def _save_ep_priority(self, subscribe, priority: dict):
        """写入 episode_priority（纯 int 格式）"""
        from app.db.subscribe_oper import SubscribeOper
        from app.schemas.types import MediaType
        try:
            # 统一转为纯 int 格式
            clean = {}
            for k, v in priority.items():
                if isinstance(v, dict):
                    clean[k] = int(v.get("score", 0))
                elif isinstance(v, (int, float)):
                    clean[k] = int(v)
                else:
                    clean[k] = 0
            SubscribeOper().update(subscribe.id, {"episode_priority": clean})
        except Exception as e:
            logger.warning(f"更新 episode_priority 失败: {e}")

    def _get_existing_ep_size(self, subscribe, episode: int, local_dir: str) -> int:
        """
        获取现有文件的真实大小。
        直接从 MP 数据库查询 transferhistory。
        episode_priority 已改为纯 int（无大小信息），不再缓存 size。
        """
        return self._query_file_size_from_db(
            tmdbid=subscribe.tmdbid,
            season=subscribe.season or 1,
            episode=episode
        )

    def _process_tv_subscribe_upgrade(
        self,
        subscribe,
        history: List[dict],
        transfer_details: List[Dict[str, Any]],
        transferred_count: int,
        exclude_ids: Set[int]
    ) -> int:
        """
        洗版模式专用转存逻辑（独立于普通转存）

        流程：
        1. 分析本地 strm 已有画质评分
        2. 搜索全部集数（含已存在的）
        3. 仅转存画质提升达到层级的集数
        4. 不调用 check_and_finish_subscribe，保持订阅活跃

        :param subscribe: 订阅对象
        :param history: 历史记录列表
        :param transfer_details: 转存详情列表
        :param transferred_count: 当前已转存数量
        :param exclude_ids: 排除的订阅ID集合
        :return: 更新后的转存数量
        """
        import re
        from pathlib import Path
        from app.db.subscribe_oper import SubscribeOper

        try:
            season = subscribe.season or 1
            logger.info(f"【洗版转存】{subscribe.name} S{season:02d}")

            # ---- 1. 识别媒体信息 ----
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.begin_season = season
            meta.type = MediaType.TV

            mediainfo: MediaInfo = self._chain.recognize_media(
                meta=meta, mtype=MediaType.TV,
                tmdbid=subscribe.tmdbid, doubanid=subscribe.doubanid, cache=True
            )
            if not mediainfo:
                logger.warn(f"【洗版转存】无法识别媒体信息 {subscribe.name}")
                return transferred_count

            # ---- 2. 构建过滤条件（宽松模式） ----
            effective_exclude = getattr(subscribe, 'exclude', None)
            subscribe_filter = SubscribeFilter(
                quality=subscribe.quality, resolution=subscribe.resolution,
                effect=subscribe.effect, include=getattr(subscribe, 'include', None),
                exclude=effective_exclude, filter=getattr(subscribe, 'filter', None),
                framerate=self._frame_rate_pattern,
                bit_depth=self._bit_depth_pattern,
                vivid_pattern=self._vivid_pattern,
                strict=False  # 洗版模式宽松匹配
            )

            # 层级阈值 = 每条规则 100 分 × 最低提升层级数
            tier_threshold = self._upgrade_threshold

            # ---- 3. 扫描本地 strm 目录，获取已有画质评分 ----
            show_name = subscribe.name
            show_year = subscribe.year or ""
            tmdbid = subscribe.tmdbid

            # 确定本地目录路径
            candidate_bases = []
            sub_save = getattr(subscribe, 'save_path', None)
            if sub_save:
                candidate_bases.append(sub_save)
            if self._save_path:
                candidate_bases.append(self._save_path)
            candidate_bases.append("/media/电视剧")
            seen = set()
            unique_bases = []
            for b in candidate_bases:
                if b and b not in seen:
                    seen.add(b)
                    unique_bases.append(b)

            local_dir = None
            for base in unique_bases:
                test_dir = f"{base}/{show_name} ({show_year}) {{tmdbid={tmdbid}}}/Season {season:02d}"
                if Path(test_dir).exists():
                    local_dir = test_dir
                    break

            # 读取已有的 episode_priority（兼容新旧格式）
            existing_ep_pri = self._read_ep_priority(subscribe)

            # 扫描本地 strm 文件评分（含体积）
            local_scores = {}  # episode_num -> {"score": int, "size": int}
            if local_dir and Path(local_dir).exists():
                strm_files = list(Path(local_dir).glob("*.strm"))
                for sf in strm_files:
                    fname = sf.name.replace('.strm', '')
                    ep_match = re.search(r'[Ee](\d{2,4})', fname) or re.search(r'第\s*(\d+)\s*集', fname)
                    if not ep_match:
                        continue
                    episode = int(ep_match.group(1))

                    # MP 规则组评分 + 体积评分 = 综合分
                    ep_size = self._get_existing_ep_size(subscribe, episode, local_dir)
                    pri_order = self._get_mp_rule_score(fname, ep_size, subscribe, season)
                    total_score = self._calc_total_upgrade_score(
                        rule_score=pri_order,
                        existing_size=ep_size or 1,
                        candidate_size=ep_size or 1,
                        mode=self._upgrade_mode
                    )

                    if episode not in local_scores or total_score > local_scores[episode]:
                        local_scores[episode] = total_score

            # 合并 episode_priority 中的历史记录
            for ep_key, ep_val in existing_ep_pri.items():
                try:
                    ep_num = int(ep_key)
                    hist_score = int(ep_val) if not isinstance(ep_val, dict) else int(ep_val.get("score", 0))
                    if ep_num not in local_scores or hist_score > local_scores[ep_num]:
                        local_scores[ep_num] = hist_score
                except (ValueError, TypeError):
                    pass

            upgrade_log_prefix = f"【洗版转存】{mediainfo.title} S{season:02d}"

            if not local_scores:
                logger.info(f"{upgrade_log_prefix} 本地无 strm 文件，回退到普通转存逻辑")
                return self.process_tv_subscribe(
                    subscribe=subscribe, history=history,
                    transfer_details=transfer_details,
                    transferred_count=transferred_count,
                    exclude_ids=exclude_ids
                )

            logger.info(f"{upgrade_log_prefix} 本地已有 {len(local_scores)} 集 strm 文件，"
                        f"阈值为 {tier_threshold} 分（upgrade_threshold={self._upgrade_threshold}）")

            # ---- 4. 构造待搜索的集数列表 ----
            total_ep = subscribe.total_episode or 0
            start_ep = subscribe.start_episode or 1
            all_expected_episodes = set(range(start_ep, total_ep + 1)) if total_ep > 0 else set()

            # 需要升级 = 已有评分但未满分的 + 完全缺失的
            episodes_to_search = set()
            for ep_num in sorted(local_scores.keys()):
                if local_scores[ep_num] < 100:  # 满分100
                    episodes_to_search.add(ep_num)

            if all_expected_episodes:
                missing_eps = sorted(all_expected_episodes - set(local_scores.keys()))
                if missing_eps:
                    logger.info(f"{upgrade_log_prefix} 本地缺失 {len(missing_eps)} 集：{missing_eps}，一并搜索")
                    episodes_to_search |= set(missing_eps)

            if not episodes_to_search:
                logger.info(f"{upgrade_log_prefix} 所有集数已达满分（300），无需洗版")
                return transferred_count

            episodes_to_search = sorted(episodes_to_search)

            # TMDB 播出日期过滤
            if mediainfo.tmdb_id:
                try:
                    from app.chain.tmdb import TmdbChain
                    tmdb_eps = TmdbChain().tmdb_episodes(tmdbid=mediainfo.tmdb_id, season=season)
                    if tmdb_eps:
                        today = datetime.date.today().isoformat()
                        aired = {ep.episode_number for ep in tmdb_eps
                                 if ep.air_date and ep.air_date <= today and ep.episode_number}
                        if aired:
                            not_aired = [ep for ep in episodes_to_search if ep not in aired]
                            if not_aired:
                                episodes_to_search = [ep for ep in episodes_to_search if ep in aired]
                                logger.info(f"{upgrade_log_prefix} 跳过 {len(not_aired)} 集未播出")
                except Exception as e:
                    logger.warning(f"{upgrade_log_prefix} TMDB 播出日期查询失败：{e}")

            if not episodes_to_search:
                logger.info(f"{upgrade_log_prefix} 无可搜索的集数")
                return transferred_count

            logger.info(f"{upgrade_log_prefix} 待搜索 {len(episodes_to_search)} 集：{episodes_to_search}")

            # ---- 5. 搜索源 + 匹配转存 ----
            enabled_sources = self._search_handler.get_enabled_sources()
            if not enabled_sources:
                logger.warning(f"{upgrade_log_prefix} 没有可用的搜索源")
                return transferred_count

            show_folder = f"{mediainfo.title} ({mediainfo.year})" if mediainfo.year else mediainfo.title
            save_dir = f"{self._save_path}/{show_folder}/Season {season}"

            new_priority = dict(existing_ep_pri)
            upgrade_downloaded = 0
            upgrade_notices = []  # 用于通知

            for source_index, source in enumerate(enabled_sources):
                if not episodes_to_search:
                    break
                if transferred_count >= self._max_transfer_per_sync:
                    logger.info(f"{upgrade_log_prefix} 已达单次同步上限 {self._max_transfer_per_sync}")
                    break

                logger.info(f"{upgrade_log_prefix} [{source.upper()}] 开始搜索")

                p115_results = self._search_handler.search_single_source(
                    source=source, mediainfo=mediainfo,
                    media_type=MediaType.TV, season=season
                )

                if not p115_results:
                    remaining = enabled_sources[source_index + 1:]
                    if remaining:
                        logger.info(f"{upgrade_log_prefix} [{source.upper()}] 未找到资源，继续下一个源")
                    else:
                        logger.info(f"{upgrade_log_prefix} [{source.upper()}] 未找到资源，已无更多源")
                    continue

                for resource in p115_results:
                    if not episodes_to_search:
                        break
                    if transferred_count >= self._max_transfer_per_sync:
                        break

                    share_url = resource.get("url", "")
                    resource_title = resource.get("title", "")

                    # HDHive 解锁
                    if resource.get("need_unlock") and not share_url:
                        slug = resource.get("slug")
                        if slug:
                            unlocked = self._search_handler.unlock_hdhive_resource(
                                slug, resource.get("unlock_points", 0)
                            )
                            if not unlocked:
                                continue
                            share_url = unlocked
                            resource["url"] = share_url
                            resource["need_unlock"] = False

                    if not share_url:
                        continue

                    share_status = self._p115_manager.check_share_status(share_url)
                    if not share_status.is_valid:
                        continue

                    share_files = self._p115_manager.list_share_files(
                        share_url,
                        target_season=(season if self._skip_other_season_dirs else None)
                    )
                    if not share_files:
                        continue

                    # 匹配需要升级的集数
                    matched_items = []
                    for episode in episodes_to_search[:]:
                        matched_file = FileMatcher.match_episode_file(
                            share_files, mediainfo.title, season, episode,
                            subscribe_filter=subscribe_filter
                        )
                        if not matched_file:
                            continue

                        file_name = matched_file.get('name', '')
                        # 候选文件大小（115 API 搜索已自带）
                        candidate_size = int(matched_file.get('size', 0)) or 0

                        # 现有文件信息（纯 int 评分）
                        old_score = local_scores.get(episode, 0)
                        existing_size = self._get_existing_ep_size(subscribe, episode, local_dir) if old_score > 0 else 0

                        # 候选文件用 MP 规则组评分 + 体积评分 = 综合分
                        cand_pri = self._get_mp_rule_score(file_name, candidate_size, subscribe, season)
                        new_score = self._calc_total_upgrade_score(
                            rule_score=cand_pri,
                            existing_size=existing_size or candidate_size,
                            candidate_size=candidate_size or existing_size,
                            mode=self._upgrade_mode
                        )

                        score_gap = new_score - old_score

                        # 判断是否值得升级
                        if old_score > 0 and score_gap < tier_threshold:
                            logger.info(
                                f"{upgrade_log_prefix} E{episode:02d} 提升+{score_gap}<{tier_threshold} "
                                f"（{old_score}→{new_score}），跳过"
                            )
                            continue

                        logger.info(
                            f"{upgrade_log_prefix} E{episode:02d} {old_score}→{new_score}"
                            f"（提升+{score_gap}>={tier_threshold}）✓"
                            if score_gap >= tier_threshold else
                            f"{upgrade_log_prefix} E{episode:02d} 新文件评分 {new_score}（无历史评分）"
                        )

                        matched_items.append({
                            "file": matched_file,
                            "episode": episode,
                            "new_score": new_score,
                            "old_score": old_score,
                            "score_gap": score_gap,
                            "file_name": file_name,
                            "candidate_size": candidate_size,
                        })

                    if not matched_items:
                        continue

                    # 批量转存
                    file_ids = [item["file"]["id"] for item in matched_items]
                    success_ids, failed_ids = self._p115_manager.transfer_files_batch(
                        share_url=share_url, file_ids=file_ids,
                        save_path=save_dir, batch_size=self._batch_size
                    )

                    success_id_set = set(success_ids)
                    for item in matched_items:
                        file_id = item["file"]["id"]
                        episode = item["episode"]
                        new_score = item["new_score"]
                        old_score = item["old_score"]
                        file_name = item["file_name"]
                        success = file_id in success_id_set

                        if success:
                            transferred_count += 1
                            upgrade_downloaded += 1
                            candidate_size = item.get("candidate_size", 0)
                            new_priority[str(episode)] = new_score

                            if episode in episodes_to_search:
                                episodes_to_search.remove(episode)

                            # 收集升级通知
                            upgrade_notices.append({
                                "episode": episode,
                                "old_score": old_score,
                                "new_score": new_score,
                                "file_name": file_name,
                            })

                            # 立即删除旧strm（新文件已转存到115，strm尚未创建，无竞态）
                            if local_dir and Path(local_dir).exists():
                                ep_patterns = [f"E{episode:02d}", f"E{episode:03d}", f"S{season:02d}E{episode:02d}"]
                                for sf in Path(local_dir).glob("*.strm"):
                                    for p in ep_patterns:
                                        if p in sf.name.replace('.strm', ''):
                                            try:
                                                sf.unlink()
                                                logger.info(f"[洗版清理] 已删除旧strm：{sf.name}")
                                            except Exception as e:
                                                logger.error(f"[洗版清理] 删除strm失败 {sf.name}: {e}")
                                            break

                            # 收集转存详情（用于汇总通知）
                            existing_detail = next(
                                (d for d in transfer_details
                                 if d.get("title") == mediainfo.title and d.get("season") == season),
                                None
                            )
                            if existing_detail:
                                existing_detail["episodes"].append(episode)
                            else:
                                transfer_details.append({
                                    "type": "电视剧",
                                    "title": mediainfo.title,
                                    "year": mediainfo.year,
                                    "season": season,
                                    "episodes": [episode],
                                    "image": mediainfo.get_poster_image()
                                })

                            logger.info(
                                f"{upgrade_log_prefix} 转存成功 E{episode:02d}"
                                f" {old_score}→{new_score}（{file_name}）"
                            )

            # ---- 6. 更新 episode_priority ----
            if new_priority != existing_ep_pri:
                try:
                    SubscribeOper().update(subscribe.id, {"episode_priority": new_priority})
                    logger.info(f"{upgrade_log_prefix} 已更新 episode_priority（{len(new_priority)} 集）")
                except Exception as e:
                    logger.warning(f"{upgrade_log_prefix} 更新 episode_priority 失败：{e}")

            # ---- 7. 发送洗版通知（事件驱动，仅显示真正升级的集数） ----
            if upgrade_notices and self._notify and self._post_message:
                real_upgrades = [n for n in upgrade_notices if n['old_score'] > 0]
                if real_upgrades:
                    lines = []
                    for n in real_upgrades:
                        lines.append(
                            f"S{season:02d} E{n['episode']:02d} "
                            f"评分 {n['old_score']}→{n['new_score']}分"
                        )
                        if n.get('file_name'):
                            lines.append(f"  资源：{n['file_name']}")
                    title = f"【网盘洗版】转存升级"
                    text = f"{mediainfo.title} 共升级 {len(real_upgrades)} 集\n\n" + "\n".join(lines[:15])
                    self._post_message(
                        mtype=NotificationType.Plugin,
                        title=title,
                        text=text
                    )

            # 不调用 check_and_finish_subscribe——保持订阅活跃以持续搜索更优资源
            if upgrade_downloaded:
                logger.info(f"{upgrade_log_prefix} 洗版转存完成，共升级 {upgrade_downloaded} 集")
            else:
                logger.info(f"{upgrade_log_prefix} 洗版转存完成，未发现可升级资源")

        except Exception as e:
            logger.error(f"【洗版转存】{subscribe.name} 出错：{e}")
            import traceback
            logger.error(traceback.format_exc())

        return transferred_count

    # ==================== 洗版 ====================

    @staticmethod
    def _count_filter_tiers(subscribe) -> int:
        """计算订阅过滤规则链的总层级数（用于层级差判定）"""
        tiers = 0
        if subscribe.quality:
            tiers += 1
        if subscribe.resolution:
            tiers += 1
        if subscribe.effect:
            tiers += 1
        if getattr(subscribe, 'include', None):
            tiers += 1
        filter_groups = getattr(subscribe, 'filter_groups', None)
        if filter_groups:
            if isinstance(filter_groups, str):
                try:
                    filter_groups = json.loads(filter_groups)
                except Exception:
                    filter_groups = None
            if isinstance(filter_groups, list):
                tiers += len(filter_groups)
            elif isinstance(filter_groups, dict):
                rules = filter_groups.get('rules', [])
                tiers += len(rules) if isinstance(rules, list) else 0
        return max(tiers, 1)

    # ==================== 延迟删除机制 ====================

    def _add_pending_deletion(self, strm_path: Path, subscribe, file_name: str,
                              old_score: int, best_score: int, source: str = "cloud"):
        """
        将旧strm文件加入延迟删除队列，1分钟后自动清理
        避免新strm还没生成时误删文件
        """
        import time
        episode = 0
        import re
        ep_match = re.search(r'[Ee](\d{2,4})', strm_path.name.replace('.strm', ''))
        if ep_match:
            episode = int(ep_match.group(1))

        pending = self._get_data(self._pending_key) or {}
        pending.setdefault("items", [])
        pending["items"].append({
            "strm_path": str(strm_path),
            "sub_id": subscribe.id,
            "sub_name": subscribe.name,
            "season": subscribe.season or 1,
            "episode": episode,
            "file_name": file_name,
            "score": old_score,
            "best_score": best_score,
            "delete_at": time.time() + self._pending_delay,
            "source": source
        })
        self._save_data(self._pending_key, pending)
        logger.info(f"[延迟删除] 已加入队列：{strm_path.name}（{old_score}→{best_score}）{self._pending_delay}秒后删除")

    def process_expired_deletions(self):
        """
        处理到期的延迟删除任务
        删除本地strm并尝试清理115文件（联动daemon兜底）
        """
        import time
        from pathlib import Path

        pending = self._get_data(self._pending_key) or {}
        items = pending.get("items", [])
        if not items:
            return

        now = time.time()
        remaining = []
        deleted_count = 0
        deleted_details = []

        for item in items:
            if item["delete_at"] <= now:
                strm_path = Path(item["strm_path"])
                fname = strm_path.name
                if strm_path.exists():
                    # 删除本地strm
                    try:
                        strm_path.unlink()
                        deleted_count += 1
                        logger.info(f"[延迟删除] 已删除strm：{fname}")
                        deleted_details.append(item)
                    except Exception as e:
                        logger.error(f"[延迟删除] 删除strm失败 {fname}: {e}")
                else:
                    logger.debug(f"[延迟删除] strm已不存在：{fname}，跳过")
            else:
                remaining.append(item)

        if deleted_count:
            logger.info(f"[延迟删除] 本次共删除 {deleted_count} 个旧文件")

        # 更新存储
        pending["items"] = remaining
        self._save_data(self._pending_key, pending)

        return deleted_count, deleted_details

    def auto_upgrade_scan(self, source: str = "cloud"):
        """
        自动洗版扫描 + 虚拟种子 + 自愈清理

        :param source: 'cloud' 网盘洗版（115转存后触发）, 'pt' PT洗版（MP下载后触发）
        """
        from app.db.subscribe_oper import SubscribeOper
        from ..utils import SubscribeFilter
        import re

        # --- 自愈清理 ---
        self._self_heal_cleanup()

        source_label = "网盘" if source == "cloud" else "PT"

        # --- PT洗版：自动开启原生洗版 ---
        with SessionFactory() as db:
            all_subs = SubscribeOper(db=db).list() or []

        if source == "pt" and self._auto_best_version:
            auto_opened = 0
            for s in all_subs:
                if s.type == MediaType.TV.value and not bool(getattr(s, 'best_version', False)):
                    SubscribeOper().update(s.id, {"best_version": 1})
                    auto_opened += 1
            if auto_opened:
                logger.info(f"[PT洗版] 自动开启 {auto_opened} 个电视剧订阅的原始洗版(best_version)")
            # 重新读取（因为改了subscribe数据）
            with SessionFactory() as db:
                all_subs = SubscribeOper(db=db).list() or []

        # --- 筛选已开洗版的电视剧订阅 ---
        tv_subs = []
        for s in all_subs:
            if s.type != MediaType.TV.value:
                continue
            if not bool(getattr(s, 'best_version', False)):
                continue
            tv_subs.append(s)

        if not tv_subs:
            logger.info(f"[{source_label}洗版] 没有已开启洗版的电视剧订阅")
            return

        logger.info(f"[{source_label}洗版] 共 {len(tv_subs)} 个已开洗版的订阅")

        # --- 执行扫描 ---
        upgrade_notices = []  # 收集需要通知的升级
        total_115_deleted = 0
        deleted_details = []  # 收集低分清理详情
        for subscribe in tv_subs:
            try:
                result = self._upgrade_scan_single_sub(subscribe)
                if result:
                    if result.get("upgrades"):
                        upgrade_notices.extend(result["upgrades"])
                    dc = result.get("deleted_count", 0)
                    if dc:
                        total_115_deleted += dc
                        for d in result.get("deleted_details", []):
                            deleted_details.append(d)
            except Exception as e:
                logger.error(f"[{source_label}洗版] 出错 {subscribe.name} S{subscribe.season or 1}：{e}")

        # --- 扫描结果（通知已由事件驱动发送，此处仅日志） ---
        if upgrade_notices:
            logger.info(f"[{source_label}洗版] 发现 {len(upgrade_notices)} 处升级机会（通知已由事件驱动发送）")
        if total_115_deleted:
            logger.info(f"[{source_label}洗版] 清理了 {total_115_deleted} 个低分文件（通知已由事件驱动发送）")

    def _upgrade_scan_single_sub(self, subscribe):
        """对单个订阅执行洗版扫描，返回升级通知列表"""
        from ..utils import SubscribeFilter
        from app.db.subscribe_oper import SubscribeOper
        import re

        season = subscribe.season or 1
        total_tiers = self._count_filter_tiers(subscribe)

        # 构建 SubscribeFilter
        effective_exclude = getattr(subscribe, 'exclude', None)
        subscribe_filter = SubscribeFilter(
            quality=subscribe.quality,
            resolution=subscribe.resolution,
            effect=subscribe.effect,
            include=getattr(subscribe, 'include', None),
            exclude=effective_exclude,
            filter=getattr(subscribe, 'filter', None),
            filter_group_rules=getattr(subscribe, 'filter_groups', None),
            framerate=self._frame_rate_pattern,
            bit_depth=self._bit_depth_pattern,
            vivid_pattern=self._vivid_pattern,
            strict=False
        )

        # 扫描本地strm目录（尝试多个路径）
        show_name = subscribe.name
        show_year = subscribe.year or ""
        tmdbid = subscribe.tmdbid

        # 优先级：subscribe.save_path > self._save_path > /media/电视剧
        candidate_bases = []
        sub_save = getattr(subscribe, 'save_path', None)
        if sub_save:
            candidate_bases.append(sub_save)
        if self._save_path:
            candidate_bases.append(self._save_path)
        candidate_bases.append("/media/电视剧")
        seen = set()
        unique_bases = []
        for b in candidate_bases:
            if b and b not in seen:
                seen.add(b)
                unique_bases.append(b)

        local_dir = None
        for base in unique_bases:
            test_dir = f"{base}/{show_name} ({show_year}) {{tmdbid={tmdbid}}}/Season {season:02d}"
            if Path(test_dir).exists():
                local_dir = test_dir
                break

        if not local_dir:
            logger.debug(f"洗版扫描：未找到 {subscribe.name} S{season:02d} 的本地strm目录"
                         f"（尝试路径：{unique_bases}）")
            return []

        local_path = Path(local_dir)
        strm_files = list(local_path.glob("*.strm"))
        if not strm_files:
            logger.debug(f"洗版扫描：{local_dir} 无strm文件")
            return []

        logger.info(f"洗版扫描：{subscribe.name} S{season:02d} 发现 {len(strm_files)} 个strm文件")

        # 读取现有的 episode_priority（兼容新旧格式）
        old_priority = self._read_ep_priority(subscribe)

        new_priority = dict(old_priority)
        upgrades = []
        deleted_count = 0
        deleted_details = []

        # 按剧集分组评分（含体积）
        episode_groups = {}  # ep_key -> [(strm_path, score, fname, size), ...]
        for sf in strm_files:
            fname = sf.name.replace('.strm', '')
            ep_match = re.search(r'[Ee](\d{2,4})', fname) or re.search(r'第\s*(\d+)\s*集', fname)
            if not ep_match:
                continue
            episode = int(ep_match.group(1))

            # MP 规则组评分 + 体积评分 = 综合分
            ep_size = self._get_existing_ep_size(subscribe, episode, local_dir)
            pri_order = self._get_mp_rule_score(fname, ep_size, subscribe, season)
            # 综合分：体积×0.75 + 画质×0.25
            total_score = self._calc_total_upgrade_score(
                rule_score=pri_order,
                existing_size=ep_size or 1,
                candidate_size=ep_size or 1,
                mode=self._upgrade_mode
            )

            ep_key = str(episode)
            episode_groups.setdefault(ep_key, []).append((sf, total_score, fname, ep_size, pri_order))

        # 逐集处理：保留最高分，删除低分旧文件
        for ep_key, candidates in episode_groups.items():
            # 按分数降序排列
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_path, best_score, best_fname, best_size, best_pri_order = candidates[0]

            episode = int(ep_key)
            old_score = old_priority.get(ep_key, 0)
            tier_threshold = self._upgrade_threshold

            if best_score > old_score:
                # 更新 episode_priority：存 MP 规则组评分（pri_order），用于 PT 选种对齐
                # best_score 是综合分（含体积），best_pri_order 是纯规则组评分
                new_priority[ep_key] = max(best_pri_order, int(old_priority.get(ep_key, 0)))

                if old_score > 0:
                    tier_gap = best_score - old_score
                    upgrades.append({
                        'name': subscribe.name,
                        'season': season,
                        'episode': episode,
                        'old_score': old_score,
                        'new_score': best_score,
                        'new_file': best_fname,
                        'tier_gap': tier_gap,
                        'threshold': tier_threshold,
                        'enough': tier_gap >= tier_threshold,
                    })
                    logger.info(f"洗版扫描：{subscribe.name} E{episode:02d} {old_score}→{best_score}")

            # 删除低分旧文件
            for sf_path, sf_score, sf_fname, sf_size, sf_pri in candidates[1:]:
                # 删115云端（尝试但不要求成功，联动删除daemon兜底）
                try:
                    self._delete_old_115_file(sf_path, subscribe)
                except Exception as e:
                    logger.warning(f"洗版删除：115删除尝试失败（由联动daemon兜底）{sf_fname}: {e}")
                # 删本地strm（成功才算有效清理计数）
                try:
                    if sf_path.exists():
                        sf_path.unlink()
                        logger.info(f"洗版删除：已删除本地strm {sf_fname}")
                        deleted_count += 1
                        ep_num = int(ep_key)
                        quality_hint = sf_fname.split(' - ')[-1] if ' - ' in sf_fname else sf_fname
                        deleted_details.append({
                            'sub_name': subscribe.name,
                            'sub_season': season,
                            'episode': ep_num,
                            'file': sf_fname,
                            'score': sf_score,
                            'best_score': best_score,
                            'quality': quality_hint,
                            'reason': f"评分{sf_score}分低于最高分{best_score}分"
                        })
                except Exception as e:
                    logger.error(f"洗版删除：删除strm失败 {sf_fname}: {e}")

        # 写入 episode_priority
        if new_priority != old_priority:
            SubscribeOper().update(subscribe.id, {"episode_priority": new_priority})
            logger.info(f"洗版扫描：已更新 {subscribe.name} S{season:02d} episode_priority "
                       f"({len(new_priority)} 集)")

        if deleted_count:
            logger.info(f"洗版删除：{subscribe.name} S{season:02d} 共删除 {deleted_count} 个旧文件")

        return {
            "upgrades": upgrades,
            "deleted_count": deleted_count,
            "deleted_details": deleted_details,
            "sub_name": subscribe.name,
            "sub_season": season
        }

    def _delete_old_115_file(self, strm_path: Path, subscribe) -> bool:
        """根据旧strm文件路径，删除115上对应的文件

        :param strm_path: 本地旧strm文件路径
        :param subscribe: 订阅对象（用于获取目录映射）
        :return: 是否删除成功
        """
        try:
            # 获取strm文件名（不含ext）
            fname = strm_path.name.replace('.strm', '')
            is_tv = True  # 洗版仅支持电视剧

            # 确定115目录路径
            if is_tv:
                cloud_base = self._cloud_tv_remote_dir or self._save_path
            else:
                cloud_base = self._cloud_movie_remote_dir or self._movie_save_path

            if not cloud_base:
                logger.warning(f"删除115文件失败：未配置网盘目录（{strm_path.name}）")
                return False

            # 从本地路径推导115路径：替换本地根目录为网盘根目录
            local_base = self._cloud_tv_local_dir if is_tv else self._cloud_movie_local_dir
            if local_base and str(strm_path.parent).startswith(local_base):
                # 有自定义本地目录映射
                rel_path = str(strm_path.parent)[len(local_base):]
                cloud_dir = f"{cloud_base}{rel_path}"
            else:
                # 无映射时，取strm父目录（Season XX）的上两级作为相对路径
                parts = strm_path.parts
                try:
                    season_idx = [i for i, p in enumerate(parts) if p.startswith('Season ')][-1]
                    # 裁剪到Season目录级别，用cloud_base替换
                    local_base_guess = str(Path(*parts[:season_idx - 2]))
                    rel = str(Path(*parts[season_idx - 2:season_idx + 1]))
                    cloud_dir = f"{cloud_base}/{rel}"
                except (IndexError, ValueError):
                    # 退回到strm父目录
                    cloud_dir = f"{cloud_base}/{strm_path.parent.name}"
                    # 但如果cloud_tv_remote_dir被设置，直接用它+节目/Season
                    if self._cloud_tv_remote_dir:
                        season_name = strm_path.parent.name  # e.g. Season 01
                        show_dir = strm_path.parent.parent.name  # e.g. ShowName (Year) {tmdbid=xxx}
                        cloud_dir = f"{self._cloud_tv_remote_dir}/{show_dir}/{season_name}"

            # 在115目录中查找同名文件
            found = self._p115_manager.find_file_in_dir(cloud_dir, fname)
            if not found:
                # 尝试没有tmdbid的路径
                if self._cloud_tv_remote_dir:
                    show_parent = strm_path.parent.parent
                    show_name_clean = show_parent.name.split(' {tmdbid=')[0]
                    alt_cloud_dir = f"{self._cloud_tv_remote_dir}/{show_name_clean}/{strm_path.parent.name}"
                    found = self._p115_manager.find_file_in_dir(alt_cloud_dir, fname)

            if not found:
                logger.debug(f"洗版删除：115目录未找到 {fname}（路径：{cloud_dir}）")
                return False

            file_id = found.get("file_id") or found.get("fid")
            if not file_id:
                logger.warning(f"洗版删除：文件信息缺少file_id: {found}")
                return False

            if self._p115_manager.delete_file(file_id):
                logger.info(f"洗版删除：已从115回收站删除 {strm_path.name}（file_id={file_id}）")
                return True
            return False

        except Exception as e:
            logger.error(f"洗版删除异常 {strm_path.name}: {e}")
            return False

    def _self_heal_cleanup(self):
        """
        自愈清理：遍历所有 episode_priority 非空的订阅，
        检查每个记录的 strm 文件是否存在，不存在则清除该记录
        """
        from app.db.subscribe_oper import SubscribeOper

        try:
            with SessionFactory() as db:
                oper = SubscribeOper(db=db)
                rows = db.execute(text(
                    "SELECT id, episode_priority, name, season, save_path, tmdbid, year "
                    "FROM subscribe WHERE episode_priority IS NOT NULL AND episode_priority != '{}'"
                )).fetchall()
        except Exception as e:
            logger.warning(f"自愈清理：查询失败 {e}")
            return

        if not rows:
            return

        cleaned_count = 0
        for row in rows:
            try:
                sid, raw_ep_pri, name, season, save_path, tmdbid, year = row
                if not raw_ep_pri:
                    continue
                if isinstance(raw_ep_pri, str):
                    ep_pri = json.loads(raw_ep_pri)
                else:
                    ep_pri = dict(raw_ep_pri)

                if not ep_pri or not isinstance(ep_pri, dict):
                    continue

                season = season or 1
                save_path = save_path or self._save_path
                year_str = f" ({year})" if year else ""
                tmdb_str = f" {{tmdbid={tmdbid}}}" if tmdbid else ""

                # 尝试多个路径
                candidate_dirs = [
                    f"{save_path}/{name}{year_str}{tmdb_str}/Season {season:02d}",
                    f"/media/电视剧/{name}{year_str}{tmdb_str}/Season {season:02d}",
                ]
                if self._save_path and self._save_path != save_path:
                    candidate_dirs.append(
                        f"{self._save_path}/{name}{year_str}{tmdb_str}/Season {season:02d}"
                    )

                # 找到第一个存在的目录
                found_dir = None
                for cd in candidate_dirs:
                    if Path(cd).exists():
                        found_dir = cd
                        break
                if not found_dir:
                    # 目录都不存在 -> 跳过（可能strm还没生成）
                    continue

                to_remove = []
                for ep_key in list(ep_pri.keys()):
                    if ep_key.endswith('_file'):
                        continue
                    ep_num = int(ep_key)
                    found = False
                    for pattern in [
                        f"S{season:02d}E{ep_num:02d}", f"S{season:02d}E{ep_num:03d}",
                        f"E{ep_num:02d}", f"E{ep_num:03d}",
                    ]:
                        for f in Path(found_dir).glob(f"*{pattern}*"):
                            if f.exists():
                                found = True
                                break
                        if found:
                            break
                    if not found:
                        to_remove.append(ep_key)

                if to_remove:
                    for k in to_remove:
                        ep_pri.pop(k, None)
                    SubscribeOper().update(sid, {"episode_priority": ep_pri})
                    cleaned_count += len(to_remove)
                    logger.info(f"自愈清理：{name} S{season:02d} 清除 {len(to_remove)} 条无效记录：{to_remove}")
            except Exception as e:
                logger.warning(f"自愈清理单条失败: {e}")

        if cleaned_count:
            logger.info(f"自愈清理完成：共清理 {cleaned_count} 条记录")

    def send_transfer_notification(self, transfer_details: List[Dict[str, Any]], total_count: int):
        """
        发送转存完成通知

        :param transfer_details: 转存详情列表
        :param total_count: 转存总数
        """
        if not transfer_details or not self._post_message:
            return

        text_lines = []
        first_image = None

        for detail in transfer_details:
            if detail.get("type") == "电影":
                title = detail.get("title", "未知")
                year = detail.get("year", "")
                text_lines.append(f"{title} ({year})")
                if not first_image and detail.get("image"):
                    first_image = detail.get("image")
            else:
                title = detail.get("title", "未知")
                season = detail.get("season", 1)
                episodes = detail.get("episodes", [])
                episodes.sort()
                if len(episodes) <= 5:
                    ep_str = ", ".join([f"E{e:02d}" for e in episodes])
                else:
                    ep_str = f"E{episodes[0]:02d}-E{episodes[-1]:02d} 共{len(episodes)}集"
                text_lines.append(f"{title} S{season:02d} {ep_str}")
                if not first_image and detail.get("image"):
                    first_image = detail.get("image")

        if len(text_lines) > 10:
            text_lines = text_lines[:10]
            text_lines.append(f"... 等共 {len(transfer_details)} 项")

        self._post_message(
            mtype=NotificationType.Plugin,
            title=f"【115网盘订阅追更】转存完成",
            text=f"本次共转存 {total_count} 个文件\n\n" + "\n".join(text_lines)
        )
