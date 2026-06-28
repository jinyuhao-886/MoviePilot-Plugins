"""
httpcore_request 405/连接异常退避补丁
拦截所有 115 API 的异常并自动重试（指数退避）
"""
import time
import logging

logger = logging.getLogger(__name__)

_loaded = False


def apply():
    global _loaded
    if _loaded:
        return

    try:
        import httpcore_request
    except ImportError:
        logger.warning("httpcore_request 未安装，跳过")
        return

    _original = httpcore_request.request

    def _patched(*args, **kwargs):
        url = kwargs.get('url', args[0] if args else None)
        url_str = str(url) if url else ''
        is_115_api = '115.com' in url_str

        max_attempts = 4 if is_115_api else 1
        base_delay = 3 if is_115_api else 0

        for attempt in range(max_attempts):
            try:
                resp = _original(*args, **kwargs)
            except Exception as e:
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"⏳ 115 请求异常 {attempt+1}/{max_attempts}，{delay}s 后重试: {type(e).__name__}: {url_str[:80]}")
                    time.sleep(delay)
                    continue
                raise

            status = getattr(resp, 'status_code', None) or getattr(resp, 'status', None)
            if status == 405 and is_115_api and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"⏳ WAF 405 拦截 {attempt+1}/{max_attempts}，{delay}s 后重试: {url_str[:80]}")
                time.sleep(delay)
                continue

            return resp

        return resp

    httpcore_request.request = _patched
    _loaded = True
    logger.info("✅ httpcore_request.request 已添加 405 退避")
