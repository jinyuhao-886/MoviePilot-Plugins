"""
UI配置模块
负责生成插件的配置表单和详情页面
"""
from typing import List, Dict, Any, Tuple
from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.schemas.types import MediaType
from app.log import logger
from app.db import SessionFactory
from sqlalchemy import text


class UIConfig:
    """UI配置管理类"""

    @staticmethod
    def get_subscribe_options() -> List[Dict[str, Any]]:
        """
        获取订阅选项列表（电影和电视剧）
        :return: 订阅选项列表 [{"title": "显示名", "value": id}, ...]
        """
        try:
            with SessionFactory() as db:
                subscribes = SubscribeOper(db=db).list("N,R")
            if not subscribes:
                return []

            options = []
            for s in subscribes:
                type_label = "[剧]" if s.type == MediaType.TV.value else "[影]"
                if s.type == MediaType.TV.value:
                    display = f"{type_label} {s.name} ({s.year}) S{s.season or 1}" if s.year else f"{type_label} {s.name} S{s.season or 1}"
                else:
                    display = f"{type_label} {s.name} ({s.year})" if s.year else f"{type_label} {s.name}"
                options.append({"title": display, "value": s.id})
            return options
        except Exception as e:
            logger.error(f"获取订阅列表失败: {e}")
            return []

    @staticmethod
    def get_site_name_options() -> List[Dict[str, Any]]:
        """
        获取站点名称列表（用于多选）
        items: [{'title': '站点名', 'value': '站点名'}]
        """
        try:
            with SessionFactory() as db:
                rows = db.execute(text("SELECT name FROM site ORDER BY name")).fetchall()
            items = []
            for r in rows:
                name = str(r[0])
                if not name:
                    continue
                items.append({"title": name, "value": name})
            return items
        except Exception as e:
            logger.error(f"获取站点列表失败: {e}")
            return []

    @staticmethod
    def get_subscribe_options_grouped() -> List[Dict[str, Any]]:
        """
        获取按类型分组的订阅选项（用于洗版选择）
        一级：电影订阅 / 电视剧订阅
        二级：具体订阅名称
        :return: [{'title': '电影名称', 'value': id, 'group': '电影订阅'}, ...]
        """
        try:
            with SessionFactory() as db:
                subscribes = SubscribeOper(db=db).list("N,R")
            if not subscribes:
                return []
            items = []
            for s in subscribes:
                group = "电影订阅" if s.type == MediaType.MOVIE.value else "电视剧订阅"
                if s.type == MediaType.TV.value:
                    display = f"{s.name} ({s.year}) S{s.season or 1}" if s.year else f"{s.name} S{s.season or 1}"
                else:
                    display = f"{s.name} ({s.year})" if s.year else f"{s.name}"
                items.append({"title": display, "value": s.id, "group": group})
            return items
        except Exception as e:
            logger.error(f"获取洗版订阅列表失败: {e}")
            return []

    @staticmethod
    def get_form() -> Tuple[List[dict], Dict[str, Any]]:
        """
        获取插件配置表单
        :return: (表单schema, 默认配置)
        """
        subscribe_options = UIConfig.get_subscribe_options()
        site_name_items = UIConfig.get_site_name_options()

        form_schema = [
            {
                'component': 'VForm',
                'content': [
                    # 基本开关 + 执行周期
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'block_system_subscribe', 'label': '屏蔽系统订阅'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{
                                 'component': 'VCronField',
                                 'props': {
                                     'model': 'cron',
                                     'label': '执行周期（Cron）',
                                     'placeholder': '30 2,10,18 * * *',
                                     'hint': '5段 Cron：分 时 日 月 周；例：2,10,18 * * * 表示2点、10点、18点的30分执行',
                                     'persistent-hint': True,
                                     'clearable': True
                                 }
                             }]}
                        ]
                    },



                    # 115网盘说明
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{
                                'component': 'VAlert',
                                'props': {
                                    'type': 'warning',
                                    'variant': 'tonal',
                                    'text': '115网盘配置：请从浏览器获取Cookie（包含UID、CID、SEID、KID等字段）'
                                }
                            }]
                        }]
                    },
                    # 转存目录 + 115 Cookie
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VTextField', 'props': {'model': 'save_path', 'label': '电视剧转存目录', 'placeholder': '/我的接收/MoviePilot/TV'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VTextField', 'props': {'model': 'movie_save_path', 'label': '电影转存目录', 'placeholder': '/我的接收/MoviePilot/Movie'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VTextField', 'props': {'model': 'cookies', 'label': '115 Cookie', 'type': 'password', 'placeholder': 'UID=xxx; CID=xxx; SEID=xxx'}}]}
                        ]
                    },
                    # 接管时间段配置
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12},
                             'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '接管时间段配置：屏蔽系统订阅=ON时始终屏蔽；=OFF时按时间段判定'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                             'content': [{'component': 'VTextField', 'props': {'model': 'block_start_time', 'label': '屏蔽态开始时间', 'placeholder': '18:00', 'hint': '屏蔽态内保持[-1]不变'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                             'content': [{'component': 'VTextField', 'props': {'model': 'block_end_time', 'label': '屏蔽态结束时间', 'placeholder': '23:59', 'hint': '支持跨天（如22:00~06:00）'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                             'content': [{'component': 'VTextField', 'props': {'model': 'unblock_start_time', 'label': '开放态开始时间', 'placeholder': '00:00', 'hint': '开放态内自动恢复用户配置的站点'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                             'content': [{'component': 'VTextField', 'props': {'model': 'unblock_end_time', 'label': '开放态结束时间', 'placeholder': '17:30', 'hint': '支持跨天（如20:00~06:00）'}}]}
                        ]
                    },
                    # 搜索源模块（可折叠）
                    {
                        'component': 'VExpansionPanels',
                        'props': {'variant': 'accordion', 'multiple': True},
                        'content': [{
                            'component': 'VExpansionPanel',
                            'content': [
                                {'component': 'VExpansionPanelTitle', 'text': '🔍 搜索源配置'},
                                {'component': 'VExpansionPanelText', 'content': [
                                    # 搜索源优先级
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{
                                                'component': 'VSelect',
                                                'props': {
                                                    'model': 'search_source_order',
                                                    'label': '搜索源优先级（按选择顺序排序）',
                                                    'items': [
                                                        {'title': 'PanSou (盘搜)', 'value': 'pansou'},
                                                        {'title': 'HDHive (影巢)', 'value': 'hdhive'}
                                                    ],
                                                    'multiple': True,
                                                    'chips': True,
                                                    'clearable': True,
                                                    'closable-chips': True,
                                                    'hint': '按选择的先后顺序依次搜索，前面的源搜到结果就不再查询后面的；留空使用默认优先级 HDHive > PanSou；未选入的已启用源会自动排在末尾',
                                                    'persistent-hint': True
                                                }
                                            }]
                                        }]
                                    },
                                    # PanSou说明
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{
                                                'component': 'VAlert',
                                                'props': {'type': 'info', 'variant': 'tonal', 'text': 'PanSou搜索服务：网盘资源聚合搜索，用于搜索115网盘分享链接'}
                                            }]
                                        }]
                                    },
                                    # PanSou 配置
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 6, 'md': 3},
                                             'content': [{'component': 'VSwitch', 'props': {'model': 'pansou_enabled', 'label': '启用 PanSou'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                                             'content': [{'component': 'VTextField', 'props': {'model': 'pansou_url', 'label': 'PanSou API 地址', 'placeholder': 'https://your-pansou-api.com'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                                             'content': [{'component': 'VTextField', 'props': {'model': 'pansou_channels', 'label': 'TG 搜索频道', 'placeholder': '频道,用逗号分隔'}}]}
                                        ]
                                    },
                                    # PanSou 认证
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 6, 'md': 3},
                                             'content': [{'component': 'VSwitch', 'props': {'model': 'pansou_auth_enabled', 'label': '启用认证'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                                             'content': [{'component': 'VTextField', 'props': {'model': 'pansou_username', 'label': 'PanSou 用户名', 'placeholder': '启用认证时填写'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                                             'content': [{'component': 'VTextField', 'props': {"clearable": True, 'model': 'pansou_password', 'label': 'PanSou 密码', 'type': 'password', 'placeholder': '启用认证时填写'}}]}
                                        ]
                                    },
                                    # HDHive说明
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': 'HDHive资源查询：基于TMDB ID查询115网盘资源。API模式使用OpenAPI应用查询；Playwright模式使用浏览器模拟获取分享链接（需安装 playwright 和 chromium）'}}]
                                        }]
                                    },
                                    # HDHive OpenAPI 接入说明
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'tonal',
                                                'text': 'HDHive 已升级为 OpenAPI 应用 + OAuth 用户授权，旧个人 API Key 已失效。接入步骤：'
                                                        '① 在影巢申请 OpenAPI 应用（回调模式选 redirect，scope 勾选 query/unlock），获得 Client ID 和应用 Secret；'
                                                        '② 在下方填写 Client ID、应用 Secret、回调地址（须与应用配置一致）并保存；'
                                                        '③ 打开插件日志中输出的授权链接，登录影巢确认授权；'
                                                        '④ 授权后浏览器跳转到回调地址，复制地址栏中 code= 后面的授权码填入下方「授权码」并保存，插件会自动换取并维护用户 Token。'}}]
                                        }]
                                    },
                                    # HDHive 配置
                                    {
                                        'component': 'VRow',
                                        'content': [
                                             {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSwitch', 'props': {'model': 'hdhive_enabled', 'label': '启用 HDHive'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSelect', 'props': {'model': 'hdhive_query_mode', 'label': '查询模式',
                                                 'items': [{'title': 'API 模式', 'value': 'api'}, {'title': 'Playwright 模式', 'value': 'playwright'}]}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {"clearable": True, 'model': 'hdhive_client_id', 'label': 'HDHive Client ID', 'placeholder': 'OpenAPI 应用公开 ID（app_xxx）'}}]}
                                        ]
                                    },
                                    # HDHive OpenAPI 凭证
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {"clearable": True, 'model': 'hdhive_api_key', 'label': 'HDHive 应用 Secret', 'type': 'password', 'placeholder': 'OpenAPI 应用 Secret（X-API-Key）'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {"clearable": True, 'model': 'hdhive_redirect_uri', 'label': '回调地址', 'placeholder': '须与 OpenAPI 应用配置完全一致'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {"clearable": True, 'model': 'hdhive_auth_code', 'label': '授权码', 'placeholder': '授权后回调地址中的 code 参数，保存后自动换取 Token',
                                                 'hint': '一次性使用，换取 Token 成功后自动清空', 'persistent-hint': True}}]}
                                        ]
                                    },
                                    # HDHive 账号密码配置
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {'model': 'hdhive_username', 'label': 'HDHive 用户名', 'placeholder': 'Playwright 模式下需要'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 5},
                                             'content': [{'component': 'VTextField', 'props': {"clearable": True, 'model': 'hdhive_password', 'label': 'HDHive 密码', 'type': 'password', 'placeholder': 'Playwright 模式下需要'}}]}
                                        ]
                                    },
                                    # HDHive 积分配置
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSwitch', 'props': {'model': 'hdhive_auto_unlock', 'label': '自动解锁资源', 'hint': '关闭时仅查询免费资源'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {'model': 'hdhive_max_unlock_points', 'label': '累计解锁总预算', 'type': 'number', 'placeholder': '50', 'hint': '一次任务最多允许消耗的积分总和'}}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {'model': 'hdhive_max_points_per_sub', 'label': '单订阅解锁预算', 'type': 'number', 'placeholder': '20', 'hint': '处理单个订阅时允许消耗的最大积分'}}]}
                                        ]
                                    },
                                ]}  # end VExpansionPanelText
                            ]  # end VExpansionPanel content
                        }]  # end VExpansionPanel (搜索源)
                    },  # end VExpansionPanels (搜索源模块)
                    # 风控防护说明
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'tonal', 'text': '风控防护：批量转存和单次上限可有效避免115网盘风控，建议保持默认值或适当调低'}}]
                        }]
                    },
                    # 风控防护配置
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 6, 'md': 3},
                             'content': [{'component': 'VTextField', 'props': {'model': 'max_transfer_per_sync', 'label': '单次同步上限', 'type': 'number', 'placeholder': '50', 'hint': '每次同步最多转存文件数'}}]},
                            {'component': 'VCol', 'props': {'cols': 6, 'md': 3},
                             'content': [{'component': 'VTextField', 'props': {'model': 'batch_size', 'label': '批量转存大小', 'type': 'number', 'placeholder': '20', 'hint': '每批转存文件数'}}]},
                            {'component': 'VCol', 'props': {'cols': 6, 'md': 6},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'skip_other_season_dirs', 'label': '多季剧集快速转存', 'hint': '跳过其他季目录以减少API调用，资源搜索不到的时候需要关闭此功能'}}]}
                        ]
                    },
                    # 订阅过滤模式
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12, 'md': 4},
                            'content': [{
                                'component': 'VSelect',
                                'props': {
                                    'model': 'subscribe_filter_mode',
                                    'label': '订阅过滤模式',
                                    'items': [
                                        {'title': '排除模式（处理除勾选外的全部订阅）', 'value': 'exclude'},
                                        {'title': '指定模式（仅处理勾选的订阅）', 'value': 'include'}
                                    ],
                                    'hint': '以PT订阅为主、网盘为辅时建议用指定模式，只勾选少数需要网盘补充的订阅',
                                    'persistent-hint': True
                                }
                            }]
                        }]
                    },
                    # 排除订阅（排除模式下生效）
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{'component': 'VSelect', 'props': {'model': 'exclude_subscribes', 'label': '排除订阅（排除模式下生效：选择不需要本插件处理的订阅）',
                                'multiple': True, 'chips': True, 'clearable': True, 'closable-chips': True, 'items': subscribe_options}}]
                        }]
                    },
                    # 指定订阅（指定模式下生效）
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{'component': 'VSelect', 'props': {'model': 'include_subscribes', 'label': '指定订阅（指定模式下生效：仅勾选的订阅由本插件处理）',
                                'multiple': True, 'chips': True, 'clearable': True, 'closable-chips': True, 'items': subscribe_options}}]
                        }]
                    },
                    # 规则自动填充（内置SubscribeGroup）
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '规则自动填充（内置 SubscribeGroup）：新增订阅时自动填充过滤规则组。可按二级分类选择规则组，无匹配时用通用规则组兜底。'}}]
                        }]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 2},
                             'content': [{'component': 'VSwitch', 'props': {'model': 'subscribe_auto_fill', 'label': '启用', 'hint': '新增订阅时自动填充过滤规则组'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 5},
                             'content': [{'component': 'VSelect', 'props': {
                                 'model': 'subscribe_category_rules',
                                 'label': '二级分类规则映射（多选，按序匹配）',
                                 'items': [
                                     {'title': '国产剧 → 电视剧非杜比画质优先', 'value': '国产剧#电视剧非杜比画质优先'},
                                     {'title': '国产剧 → 电视剧杜比画质优先', 'value': '国产剧#电视剧杜比画质优先'},
                                     {'title': '美剧 → 电视剧非杜比画质优先', 'value': '美剧#电视剧非杜比画质优先'},
                                     {'title': '美剧 → 电视剧杜比画质优先', 'value': '美剧#电视剧杜比画质优先'},
                                     {'title': '动漫 → 电视剧非杜比画质优先', 'value': '动漫#电视剧非杜比画质优先'},
                                     {'title': '动漫 → 电视剧杜比画质优先', 'value': '动漫#电视剧杜比画质优先'},
                                     {'title': '华语电影 → 电影非杜比画质优先', 'value': '华语电影#电影非杜比画质优先'},
                                     {'title': '华语电影 → 电影含杜比画质优先', 'value': '华语电影#电影含杜比画质优先'},
                                     {'title': '外语电影 → 电影非杜比画质优先', 'value': '外语电影#电影非杜比画质优先'},
                                     {'title': '外语电影 → 电影含杜比画质优先', 'value': '外语电影#电影含杜比画质优先'},
                                 ],
                                 'multiple': True,
                                 'chips': True,
                                 'clearable': True,
                                 'closable-chips': True,
                                 'hint': '选中的映射项会应用于对应二级分类的订阅。同分类同名多项时，按选中先后顺序匹配。未匹配的分类走下方兜底规则组。',
                                 'persistent-hint': True
                             }}]},
                        ]
                    },
                    # 通用规则组兜底
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSelect', 'props': {
                                 'model': 'subscribe_tv_rule_group',
                                 'label': '电视剧兜底规则组',
                                 'items': [
                                     {'title': '电视剧非杜比画质优先', 'value': '电视剧非杜比画质优先'},
                                     {'title': '电视剧杜比画质优先', 'value': '电视剧杜比画质优先'},
                                 ],
                                 'clearable': True,
                                 'hint': '未匹配到二级分类规则时，电视剧的兜底规则组',
                                 'persistent-hint': True
                             }}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                             'content': [{'component': 'VSelect', 'props': {
                                 'model': 'subscribe_movie_rule_group',
                                 'label': '电影兜底规则组',
                                 'items': [
                                     {'title': '电影非杜比画质优先', 'value': '电影非杜比画质优先'},
                                     {'title': '电影含杜比画质优先', 'value': '电影含杜比画质优先'},
                                 ],
                                 'clearable': True,
                                 'hint': '未匹配到二级分类规则时，电影的兜底规则组',
                                 'persistent-hint': True
                             }}]},
                        ]
                    },
                    # 洗版模块（可折叠）
                    {
                        'component': 'VExpansionPanels',
                        'props': {'variant': 'accordion', 'multiple': True},
                        'content': [{
                            'component': 'VExpansionPanel',
                            'content': [
                                {'component': 'VExpansionPanelTitle', 'text': '🔄 洗版升级配置'},
                                {'component': 'VExpansionPanelText', 'content': [
                                    # 洗版开关：网盘洗版 / PT洗版
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSwitch', 'props': {
                                                 'model': 'enable_cloud_upgrade', 'label': '网盘洗版',
                                                 'hint': '115转存后自动扫本地strm，与episode_priority比对评分。发现更高分版本且层级差足够时，删除115网盘旧文件（回收站）并保留新高分文件。',
                                                 'persistent-hint': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSwitch', 'props': {
                                                 'model': 'enable_pt_upgrade', 'label': 'PT洗版',
                                                 'hint': 'PT下载后自动扫本地strm并与episode_priority比对。评分机制：匹配第1条优先级规则→100分，末条→60分，中间等差。旧文件→回收站，转存新文件。内置规则自动填充+4套预设规则组可配合使用。',
                                                 'persistent-hint': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSwitch', 'props': {
                                                 'model': 'auto_best_version', 'label': '自动开启原生洗版',
                                                 'hint': '（PT洗版子开关）打开后自动将所有电视剧订阅的best_version置为开启，无需逐个手动打开。关闭时仅已手动开启的订阅生效。',
                                                 'persistent-hint': True
                                             }}]},
                                        ]
                                    },
                                    # 独立洗版订阅选择
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{
                                                'component': 'VSelect',
                                                'props': {
                                                    'model': 'upgrade_subscribe_ids',
                                                    'label': '单独开启洗版的订阅（勾选即开启原生洗版）',
                                                    'items': UIConfig.get_subscribe_options_grouped(),
                                                    'multiple': True,
                                                    'chips': True,
                                                    'clearable': True,
                                                    'closable-chips': True,
                                                    'hint': '勾选的订阅会开启原生洗版，网盘洗版/PT洗版仅对这些订阅执行洗版操作。可点击输入框展开二级选择（电影订阅/电视剧订阅）。保存配置时自动对已选订阅的已有转存记录评分并写入episode_priority。',
                                                    'persistent-hint': True,
                                                    'no-data-text': '正在加载订阅列表...'
                                                }
                                            }]
                                        }]
                                    },
                                    # 网盘洗版目录映射
                                    # 第一行：电视剧
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'cloud_tv_local_dir', 'label': '本地strm电视剧',
                                                 'placeholder': '/media/电视剧',
                                                 'hint': '与115网盘目录层级结构保持一致可免API搜索，否则需开启下方删除开关并触发API',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'cloud_tv_remote_dir', 'label': '网盘电视剧',
                                                 'placeholder': '/视频',
                                                 'hint': '与本地strm目录层级一致可免搜索，不一致开启删除开关后增加请求可能触发风控',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                        ]
                                    },
                                    # 第二行：电影
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'cloud_movie_local_dir', 'label': '本地strm电影',
                                                 'placeholder': '/media/电影',
                                                 'hint': '与115网盘目录层级结构保持一致可免API搜索，否则需开启下方删除开关并触发API',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'cloud_movie_remote_dir', 'label': '网盘电影',
                                                 'placeholder': '/电影',
                                                 'hint': '与本地strm目录层级一致可免搜索，不一致开启删除开关后增加请求可能触发风控',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                        ]
                                    },
                                    # 洗版参数
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSelect', 'props': {
                                                 'model': 'upgrade_mode', 'label': '洗版评分模式',
                                                 'items': [
                                                     {'title': '智能洗版（体积75%+规则25% · 推荐）', 'value': 'smart'},
                                                     {'title': '简易洗版（纯体积对比）', 'value': 'simple'},
                                                 ],
                                                 'hint': '智能：体积占75%权重，规则占25%（HDR/H265/10bit等）。简易：只看文件大小，体积越大分越高。',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VSlider', 'props': {
                                                 'model': 'upgrade_threshold', 'label': '最低洗版提升分',
                                                 'min': 0, 'max': 100, 'step': 5,
                                                 'thumb-label': True,
                                                 'hint': '候选文件总分必须超过现有文件至少N分才触发洗版。25≈体积大15%或规则提升1级。越大越保守。',
                                                 'persistent-hint': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'self_heal_interval', 'label': '进度自愈间隔（分钟）', 'type': 'number',
                                                 'placeholder': '10', 'hint': '自动清理episode_priority中本地strm已不存在的记录。设为0关闭自愈。',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                        ]
                                    },
                                    # PT洗版防抖
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'upgrade_debounce_seconds', 'label': 'PT洗版防抖间隔（秒）', 'type': 'number',
                                                 'placeholder': '600',
                                                 'hint': '媒体入库事件触发的PT洗版扫描最小间隔，默认600秒（10分钟），避免逐文件触发刷屏。',
                                                 'persistent-hint': True, 'clearable': True
                                             }}]},
                                        ]
                                    },
                                    # ↓↓↓ 以下为洗版关联配置 ↓↓↓

                                    # MP过滤规则管理
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{
                                                'component': 'VAlert',
                                                'props': {
                                                    'type': 'info',
                                                    'variant': 'tonal',
                                                    'text': 'MP过滤规则管理：向MP系统注册VIVID/10BIT/60FPS三条自定义规则，'
                                                            '让订阅优先级规则组中可以正常使用 Vivid、10bit、60FPS 等规则ID。'
                                                            '同时自动应用下方选择的优先级规则组预设。'
                                                            '保存配置即自动应用，也可在插件页面手动触发。'
                                                }
                                            }]
                                        }]
                                    },
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{
                                                'component': 'VSwitch',
                                                'props': {
                                                    'model': 'auto_register_rules',
                                                    'label': '注册自定义规则+预设规则组到MP',
                                                    'hint': '开启后，插件向MP注册VIVID/10BIT/60FPS自定义规则，并自动应用所选优先级规则组预设。关闭则不动MP原有规则。默认关闭，按需开启。',
                                                    'persistent-hint': True
                                                }
                                            }]
                                        }]
                                    },
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'vivid_pattern', 'label': 'VIVID自定义规则正则',
                                                 'placeholder': r'HDR[._ ]?[Vv]ivid|菁彩影像|HDRVivid', 'clearable': True,
                                                 'hint': 'MP自定义规则ID: VIVID。匹配种子标题中含Vivid/菁彩影像的资源',
                                                 'persistent-hint': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'bit_rate_pattern', 'label': '10BIT自定义规则正则',
                                                 'placeholder': r'10bit|12bit|10-bit|12-bit', 'clearable': True,
                                                 'hint': 'MP自定义规则ID: 10BIT。匹配10bit/12bit色深的资源',
                                                 'persistent-hint': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                             'content': [{'component': 'VTextField', 'props': {
                                                 'model': 'frame_rate_pattern', 'label': '60FPS自定义规则正则',
                                                 'placeholder': r'60fps|120fps|50fps|60帧|120帧|50帧', 'clearable': True,
                                                 'hint': 'MP规则ID: 60FPS（覆盖内置规则）。匹配高帧率资源',
                                                 'persistent-hint': True
                                             }}]},
                                        ]
                                    },
                    # 预设规则组说明
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{
                                'component': 'VAlert',
                                'props': {
                                    'type': 'success',
                                    'variant': 'tonal',
                                    'text': '预设规则组：保存配置时自动创建4套优先级规则组到MP系统：'
                                            '「电视剧非杜比画质优先」「电视剧杜比画质优先」「电影含杜比画质优先」「电影非杜比画质优先」。'
                                            '可在MP的「订阅规则」页面中查看和使用。'
                                }
                            }]
                        }]
                    },
                                    # 命名规则管理
                                    {
                                        'component': 'VRow',
                                        'content': [{
                                            'component': 'VCol',
                                            'props': {'cols': 12},
                                            'content': [{
                                                'component': 'VAlert',
                                                'props': {
                                                    'type': 'info',
                                                    'variant': 'tonal',
                                                    'text': '命名规则管理：修改MP的电影/电视剧文件重命名模板。'
                                                            '开启"自动应用命名规则"后保存配置即自动写入MP系统设置。'
                                                            '模板语法：Jinja2模板，可用 title/year/tmdbid/videoFormat/edition/audioCodec/videoCodec/hdr/releaseGroup/fileExt/season/episode/season_episode/episode_title 等变量。'
                                                }
                                            }]
                                        }]
                                    },
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                                             'content': [{'component': 'VTextarea', 'props': {
                                                 'model': 'tv_rename_format', 'label': '电视剧重命名模板',
                                                 'rows': 4, 'clearable': True,
                                                 'hint': '保存即应用（需开启开关）。修改前建议备份当前模板。',
                                                 'persistent-hint': True
                                             }}]},
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6},
                                             'content': [{'component': 'VTextarea', 'props': {
                                                 'model': 'movie_rename_format', 'label': '电影重命名模板',
                                                 'rows': 4, 'clearable': True,
                                                 'hint': '保存即应用（需开启开关）。修改前建议备份当前模板。',
                                                 'persistent-hint': True
                                             }}]},
                                        ]
                                    },
                                    {
                                        'component': 'VRow',
                                        'content': [
                                            {'component': 'VCol', 'props': {'cols': 12, 'md': 3},
                                             'content': [{'component': 'VSwitch', 'props': {
                                                 'model': 'auto_apply_naming', 'label': '自动应用命名规则',
                                                 'hint': '开启后保存配置自动将上方模板写入MP系统设置（立即生效）',
                                                 'persistent-hint': True
                                             }}]},
                                        ]
                                    },
                                ]}  # end VExpansionPanelText
                            ]  # end VExpansionPanel content
                        }]  # end VExpansionPanel (洗版)
                    },  # end VExpansionPanels (洗版模块)
                ]
            }
        ]
        # ---- 评分工具按钮 ----
        form_schema.append({
            'component': 'VCard',
            'props': {'variant': 'outlined', 'class': 'mt-4'},
            'content': [{
                'component': 'VRow',
                'props': {'class': 'mt-2'},
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 6, 'class': 'text-center'},
                        'content': [{
                            'component': 'VBtn',
                            'props': {'color': 'primary', 'variant': 'outlined', 'size': 'small', 'prepend-icon': 'mdi-refresh'},
                            'text': '整理记录评分（合并）',
                            'events': {
                                'click': {
                                    'api': f'/plugin/P115StrgmSub/batch_re_score?apikey={settings.API_TOKEN}',
                                    'method': 'post'
                                }
                            }
                        }]
                    },
                    {
                        'component': 'VCol',
                        'props': {'cols': 6, 'class': 'text-center'},
                        'content': [{
                            'component': 'VBtn',
                            'props': {'color': 'warning', 'variant': 'outlined', 'size': 'small', 'prepend-icon': 'mdi-delete-restore'},
                            'text': '强制重评分（覆盖+清理脏数据）',
                            'events': {
                                'click': {
                                    'api': f'/plugin/P115StrgmSub/force_re_score?apikey={settings.API_TOKEN}',
                                    'method': 'post'
                                }
                            }
                        }]
                    }
                ]
            },
            {
                'component': 'VRow',
                'props': {'class': 'mt-2'},
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'VAlert',
                        'props': {'type': 'info', 'variant': 'tonal',
                                 'text': '「整理记录评分」基于转存记录评分并合并现有数据；「强制重评分」清空旧评分，重新扫描磁盘strm文件打分并覆盖，同时清理无效脏数据。'}
                    }]
                }]
            }]
        })

        default_config = {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "only_115": True,
            "cron": "30 2,10,18 * * *",


            "save_path": "/我的接收/MoviePilot/TV",
            "movie_save_path": "/我的接收/MoviePilot/Movie",
            "cookies": "",
            "pansou_enabled": True,
            "pansou_url": "https://so.252035.xyz/",
            "pansou_username": "",
            "pansou_password": "",
            "pansou_auth_enabled": False,
            "pansou_channels": "QukanMovie",
            "nullbr_enabled": False,
            "nullbr_appid": "",
            "nullbr_api_key": "",
            "hdhive_enabled": False,
            "hdhive_query_mode": "api",
            "hdhive_api_key": "",
            "hdhive_client_id": "",
            "hdhive_redirect_uri": "",
            "hdhive_auth_code": "",
            "hdhive_access_token": "",
            "hdhive_refresh_token": "",
            "hdhive_token_expires_at": 0,
            "hdhive_auto_unlock": False,
            "hdhive_max_unlock_points": 50,
            "hdhive_max_points_per_sub": 20,
            "hdhive_username": "",
            "hdhive_password": "",
            "hdhive_cookie": "",
            "hdhive_auto_refresh": True,
            "hdhive_refresh_before": 86400,
            "search_source_order": [],
            "subscribe_filter_mode": "exclude",
            "exclude_subscribes": [],
            "include_subscribes": [],
            "block_system_subscribe": False,
            "auto_best_version": False,
            "block_start_time": "18:00",
            "block_end_time": "23:59",
            "unblock_start_time": "00:00",
            "unblock_end_time": "17:30",
            "max_transfer_per_sync": 50,
            "batch_size": 20,
            "skip_other_season_dirs": True,
            "enable_cloud_upgrade": False,
            "enable_pt_upgrade": False,
            "upgrade_debounce_seconds": 600,
            "auto_best_version": False,
            "upgrade_subscribe_ids": [],
            "cloud_tv_local_dir": "",
            "cloud_tv_remote_dir": "",
            "cloud_movie_local_dir": "",
            "cloud_movie_remote_dir": "",
            "min_upgrade_tiers": 2,
            "upgrade_mode": "smart",
            "upgrade_threshold": 25,
            "self_heal_interval": 10,
            "frame_rate_pattern": r"60fps|120fps|50fps|60帧|120帧|50帧",
            "bit_rate_pattern": r"10bit|12bit|10-bit|12-bit",
            "vivid_pattern": r"HDR[._ ]?[Vv]ivid|菁彩影像|HDRVivid",
            "auto_register_rules": False,
            "tv_rule_group_preset": "none",
            "tv_rule_group_custom": "",
            "movie_rule_group_preset": "none",
            "movie_rule_group_custom": "",
            "tv_rename_format": "{{title}}{% if year %} ({{year}}){% endif %} {tmdbid={{tmdbid}}}/Season {{'%02d'|format(season|int)}}/{{title}}{% if year %} ({{year}}){% endif %} - {{season_episode}} - {% if episode_title %}{{episode_title}}{% else %}第 {{episode}} 集{% endif %} - {{videoFormat}}{% if edition %}.{{edition}}{% endif %}{% if hdr %}.{{hdr}}{% endif %}{% if videoCodec %}.{{videoCodec}}{% endif %}{% if audioCodec %}.{{audioCodec}}{% endif %}{% if releaseGroup %} - {{releaseGroup}}{% endif %}{{fileExt}}",
            "movie_rename_format": "{{title}}{% if year %} ({{year}}){% endif %} {tmdbid={{tmdbid}}}/{{title}}{% if year %} ({{year}}){% endif %}{% if videoFormat %} - {{videoFormat}}{% if edition %}.{{edition}}{% endif %}{% if audioCodec %}.{{audioCodec}}{% endif %}{% if videoCodec %}.{{videoCodec}}{% endif %}{% endif %}{% if releaseGroup %} - {{releaseGroup}}{% endif %}{{fileExt}}",
            "auto_apply_naming": False,
        }

        return form_schema, default_config

    @staticmethod
    def get_page(history: List[dict]) -> List[dict]:
        """
        详情页内容与 1.2.4 无强耦合，保持原样即可
        """
        # 你原有的 get_page 很长，这里不做任何改动，继续沿用你现有版本即可。
        # 如果你希望我也按 1.2.4 统一“文案/按钮标题”，你告诉我我再一起改。
        from datetime import datetime

        history = history or []
        total_count = len(history)
        success_count = len([h for h in history if h.get("status") == "成功"])
        fail_count = len([h for h in history if h.get("status") == "失败"])
        movie_count = len([h for h in history if h.get("type") == "电影"])
        tv_count = len([h for h in history if h.get("type") != "电影"])

        today = datetime.now().strftime("%Y-%m-%d")
        today_count = len([h for h in history if h.get("time", "").startswith(today)])

        success_rate = f"{(success_count / total_count * 100):.1f}%" if total_count > 0 else "0%"

        sorted_history = sorted(history, key=lambda x: x.get('time', ''), reverse=True) if history else []
        last_sync_time = sorted_history[0].get("time", "暂无") if sorted_history else "暂无"

        stats_header = {
            'component': 'VCard',
            'props': {'class': 'mb-4'},
            'content': [{
                'component': 'VCardText',
                'content': [
                    # 第一行：统计卡片（总转存数、今日转存、成功数、失败数）
                    {
                        'component': 'VRow',
                        'content': [
                            # 总转存数
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 3},
                                'content': [{
                                    'component': 'VCard',
                                    'props': {'variant': 'tonal', 'color': 'primary'},
                                    'content': [{
                                        'component': 'VCardText',
                                        'props': {'class': 'text-center pa-3'},
                                        'content': [
                                            {'component': 'VIcon', 'props': {'size': 'x-large', 'class': 'mb-2'}, 'text': 'mdi-cloud-upload'},
                                            {'component': 'div', 'props': {'class': 'text-h4 font-weight-bold'}, 'text': str(total_count)},
                                            {'component': 'div', 'props': {'class': 'text-caption'}, 'text': '总转存数'}
                                        ]
                                    }]
                                }]
                            },
                            # 今日转存
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 3},
                                'content': [{
                                    'component': 'VCard',
                                    'props': {'variant': 'tonal', 'color': 'info'},
                                    'content': [{
                                        'component': 'VCardText',
                                        'props': {'class': 'text-center pa-3'},
                                        'content': [
                                            {'component': 'VIcon', 'props': {'size': 'x-large', 'class': 'mb-2'}, 'text': 'mdi-calendar-today'},
                                            {'component': 'div', 'props': {'class': 'text-h4 font-weight-bold'}, 'text': str(today_count)},
                                            {'component': 'div', 'props': {'class': 'text-caption'}, 'text': '今日转存'}
                                        ]
                                    }]
                                }]
                            },
                            # 成功数
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 3},
                                'content': [{
                                    'component': 'VCard',
                                    'props': {'variant': 'tonal', 'color': 'success'},
                                    'content': [{
                                        'component': 'VCardText',
                                        'props': {'class': 'text-center pa-3'},
                                        'content': [
                                            {'component': 'VIcon', 'props': {'size': 'x-large', 'class': 'mb-2'}, 'text': 'mdi-check-circle'},
                                            {'component': 'div', 'props': {'class': 'text-h4 font-weight-bold'}, 'text': str(success_count)},
                                            {'component': 'div', 'props': {'class': 'text-caption'}, 'text': f'成功 ({success_rate})'}
                                        ]
                                    }]
                                }]
                            },
                            # 失败数
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 3},
                                'content': [{
                                    'component': 'VCard',
                                    'props': {'variant': 'tonal', 'color': 'error'},
                                    'content': [{
                                        'component': 'VCardText',
                                        'props': {'class': 'text-center pa-3'},
                                        'content': [
                                            {'component': 'VIcon', 'props': {'size': 'x-large', 'class': 'mb-2'}, 'text': 'mdi-close-circle'},
                                            {'component': 'div', 'props': {'class': 'text-h4 font-weight-bold'}, 'text': str(fail_count)},
                                            {'component': 'div', 'props': {'class': 'text-caption'}, 'text': '失败'}
                                        ]
                                    }]
                                }]
                            }
                        ]
                    },
                    # 第二行：媒体类型统计（电影数、剧集数）和最近同步时间
                    {
                        'component': 'VRow',
                        'props': {'class': 'mt-4'},
                        'content': [
                            # 电影数
                            {
                                'component': 'VCol',
                                'props': {'cols': 4},
                                'content': [{
                                    'component': 'div',
                                    'props': {'class': 'd-flex align-center justify-center'},
                                    'content': [
                                        {'component': 'VIcon', 'props': {'color': 'amber', 'class': 'mr-2'}, 'text': 'mdi-movie'},
                                        {'component': 'span', 'props': {'class': 'text-h6 font-weight-medium'}, 'text': str(movie_count)},
                                        {'component': 'span', 'props': {'class': 'text-caption ml-1'}, 'text': '部电影'}
                                    ]
                                }]
                            },
                            # 剧集数
                            {
                                'component': 'VCol',
                                'props': {'cols': 4},
                                'content': [{
                                    'component': 'div',
                                    'props': {'class': 'd-flex align-center justify-center'},
                                    'content': [
                                        {'component': 'VIcon', 'props': {'color': 'purple', 'class': 'mr-2'}, 'text': 'mdi-television-classic'},
                                        {'component': 'span', 'props': {'class': 'text-h6 font-weight-medium'}, 'text': str(tv_count)},
                                        {'component': 'span', 'props': {'class': 'text-caption ml-1'}, 'text': '集剧集'}
                                    ]
                                }]
                            },
                            # 最近同步时间
                            {
                                'component': 'VCol',
                                'props': {'cols': 4},
                                'content': [{
                                    'component': 'div',
                                    'props': {'class': 'd-flex align-center justify-center'},
                                    'content': [
                                        {'component': 'VIcon', 'props': {'color': 'cyan', 'class': 'mr-2'}, 'text': 'mdi-clock-outline'},
                                        {'component': 'span', 'props': {'class': 'text-caption'}, 'text': f'最近同步: {last_sync_time[:16] if len(last_sync_time) > 16 else last_sync_time}'}
                                    ]
                                }]
                            }
                        ]
                    },
                    # 操作按钮：立即搜索 + 清空历史记录
                    {
                        'component': 'VRow',
                        'props': {'class': 'mt-4'},
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'class': 'text-center'},
                                'content': [{
                                    'component': 'VBtn',
                                    'props': {'color': 'primary', 'variant': 'outlined', 'size': 'small', 'prepend-icon': 'mdi-magnify'},
                                    'text': '立即搜索',
                                    'events': {
                                        'click': {
                                            'api': f'/plugin/P115StrgmSub/sync_subscribes?apikey={settings.API_TOKEN}',
                                            'method': 'get'
                                        }
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'class': 'text-center'},
                                'content': [{
                                    'component': 'VBtn',
                                    'props': {'color': 'error', 'variant': 'outlined', 'size': 'small', 'prepend-icon': 'mdi-delete-sweep'},
                                    'text': '清空历史记录',
                                    'events': {
                                        'click': {
                                            'api': f'/plugin/P115StrgmSub/clear_history?apikey={settings.API_TOKEN}',
                                            'method': 'post'
                                        }
                                    }
                                }]
                            }
                        ]
                    }
                ]
            }]
        }

        if not sorted_history:
            empty_state = {
                'component': 'VCard',
                'props': {'variant': 'outlined', 'class': 'mt-4'},
                'content': [{
                    'component': 'VCardText',
                    'props': {'class': 'text-center py-8'},
                    'content': [
                        {'component': 'VIcon', 'props': {'size': '64', 'color': 'grey-lighten-1', 'class': 'mb-4'}, 'text': 'mdi-inbox-outline'},
                        {'component': 'div', 'props': {'class': 'text-h6 text-grey'}, 'text': '暂无转存记录'},
                        {'component': 'div', 'props': {'class': 'text-caption text-grey-lighten-1 mt-2'}, 'text': '插件运行后会在此显示转存记录'}
                    ]
                }]
            }
            return [stats_header, empty_state]

        movie_history = [h for h in sorted_history if h.get("type") == "电影"][:50]
        tv_history = [h for h in sorted_history if h.get("type") != "电影"][:50]

        def build_history_item(h: dict) -> dict:
            status = h.get("status", "")
            media_type = h.get("type", "")
            status_color = "success" if status == "成功" else "error" if status == "失败" else "warning"
            status_icon = "mdi-check-circle" if status == "成功" else "mdi-close-circle" if status == "失败" else "mdi-help-circle"
            type_icon = "mdi-movie" if media_type == "电影" else "mdi-television-classic"
            type_color = "amber" if media_type == "电影" else "purple"
            file_name = h.get("file_name", "")

            if media_type == "电影":
                title_text = f'{h.get("title", "")} ({h.get("year", "")})'
            else:
                season = h.get("season", 0) or 0
                episode = h.get("episode", 0) or 0
                title_text = f'{h.get("title", "")} S{season:02d}E{episode:02d}'

            content_items = [
                {
                    'component': 'div',
                    'props': {'class': 'd-flex justify-space-between align-center'},
                    'content': [
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center'},
                            'content': [
                                {'component': 'VIcon', 'props': {'color': type_color, 'size': 'small', 'class': 'mr-2'}, 'text': type_icon},
                                {'component': 'span', 'props': {'class': 'font-weight-bold'}, 'text': title_text}
                            ]
                        },
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center'},
                            'content': [
                                {'component': 'VIcon', 'props': {'color': status_color, 'size': 'x-small', 'class': 'mr-1'}, 'text': status_icon},
                                {'component': 'VChip', 'props': {'color': status_color, 'size': 'x-small', 'variant': 'flat'}, 'text': status}
                            ]
                        }
                    ]
                },
                {
                    'component': 'div',
                    'props': {'class': 'd-flex align-center mt-1'},
                    'content': [
                        {'component': 'VIcon', 'props': {'size': 'x-small', 'color': 'grey', 'class': 'mr-1'}, 'text': 'mdi-clock-outline'},
                        {'component': 'span', 'props': {'class': 'text-caption text-grey'}, 'text': h.get("time", "")}
                    ]
                }
            ]

            if file_name:
                content_items.append({
                    'component': 'div',
                    'props': {'class': 'd-flex align-center mt-1'},
                    'content': [
                        {'component': 'VIcon', 'props': {'size': 'x-small', 'color': 'grey', 'class': 'mr-1'}, 'text': 'mdi-file-video'},
                        {'component': 'span', 'props': {'class': 'text-caption text-grey text-truncate'}, 'text': file_name}
                    ]
                })

            border_style = f'border-left: 3px solid var(--v-theme-{status_color}) !important;'
            return {
                'component': 'VCard',
                'props': {'class': 'mb-2', 'variant': 'outlined', 'style': border_style},
                'content': [{'component': 'VCardText', 'props': {'class': 'py-2 px-3'}, 'content': content_items}]
            }

        def build_history_list(items: List[dict], empty_text: str) -> List[dict]:
            if not items:
                return [{
                    'component': 'div',
                    'props': {'class': 'text-center py-8'},
                    'content': [
                        {'component': 'VIcon', 'props': {'size': '48', 'color': 'grey-lighten-1', 'class': 'mb-2'}, 'text': 'mdi-inbox-outline'},
                        {'component': 'div', 'props': {'class': 'text-grey'}, 'text': empty_text}
                    ]
                }]
            return [build_history_item(h) for h in items]

        expansion_panels = {
            'component': 'VExpansionPanels',
            'props': {'variant': 'accordion', 'class': 'mt-4'},
            'content': [
                {
                    'component': 'VExpansionPanel',
                    'content': [
                        {
                            'component': 'VExpansionPanelTitle',
                            'content': [
                                {'component': 'VIcon', 'props': {'color': 'amber', 'class': 'mr-3'}, 'text': 'mdi-movie'},
                                {'component': 'span', 'props': {'class': 'font-weight-bold'}, 'text': f'电影 ({len(movie_history)})'}
                            ]
                        },
                        {
                            'component': 'VExpansionPanelText',
                            'content': build_history_list(movie_history, '暂无电影转存记录')
                        }
                    ]
                },
                {
                    'component': 'VExpansionPanel',
                    'content': [
                        {
                            'component': 'VExpansionPanelTitle',
                            'content': [
                                {'component': 'VIcon', 'props': {'color': 'purple', 'class': 'mr-3'}, 'text': 'mdi-television-classic'},
                                {'component': 'span', 'props': {'class': 'font-weight-bold'}, 'text': f'剧集 ({len(tv_history)})'}
                            ]
                        },
                        {
                            'component': 'VExpansionPanelText',
                            'content': build_history_list(tv_history, '暂无剧集转存记录')
                        }
                    ]
                }
            ]
        }

        return [stats_header, expansion_panels]
