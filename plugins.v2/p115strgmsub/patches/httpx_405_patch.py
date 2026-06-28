"""
httpx_request + httpx.Client.send 405/连接异常退避补丁
拦截所有 115 API 的异常并自动重试（指数退避）
"""
import time
import logging

logger = logging.getLogger(__name__)

_loaded = False


def _wrap_httpx_request():
    """拦截 httpx_request.request，异常时指数退避重试"""
    try:
        import httpx_request
    except ImportError:
        logger.warning("httpx_request 未安装，跳过")
        return

    _original = httpx_request.request

    def _patched(*args, **kwargs):
        url = kwargs.get('url', args[0] if args else None)
        is_115_api = isinstance(url, str) and '115.com' in url

        max_attempts = 4 if is_115_api else 2
        base_delay = 3 if is_115_api else 1

        for attempt in range(max_attempts):
            try:
                resp = _original(*args, **kwargs)
            except Exception as e:
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"⏳ 115 请求异常 {attempt+1}/{max_attempts}，{delay}s 后重试: {type(e).__name__}")
                    time.sleep(delay)
                    continue
                raise

            status = getattr(resp, 'status_code', None) or getattr(resp, 'status', None)
            if status == 405 and is_115_api:
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt)
                    url_str = str(url)
                    logger.warning(f"⏳ WAF 405 拦截 {attempt+1}/{max_attempts}，{delay}s 后重试: {url_str[:100]}")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"❌ WAF 405 拦截重试耗尽: {str(url)[:100]}")

            return resp

    httpx_request.request = _patched
    logger.info("✅ httpx_request.request 已添加 405 退避")


def _wrap_httpx_client_send():
    """拦截 httpx.Client.send，405 时退避重试"""
    try:
        import httpx
    except ImportError:
        return

    _original_send = httpx.Client.send

    def _patched(self, request, **kwargs):
        url = str(getattr(request, 'url', ''))
        is_115 = '115.com' in url

        max_attempts = 4 if is_115 else 1
        base_delay = 3 if is_115 else 0

        for attempt in range(max_attempts):
            try:
                resp = _original_send(self, request, **kwargs)
            except Exception as e:
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"⏳ httpx 异常 {attempt+1}/{max_attempts}，{delay}s 重试: {type(e).__name__}")
                    time.sleep(delay)
                    continue
                raise

            if resp.status_code == 405 and is_115 and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"⏳ httpx Client 405 {attempt+1}/{max_attempts}，{delay}s 重试: {url[:80]}")
                time.sleep(delay)
                continue
            return resp

        return resp

    httpx.Client.send = _patched
    logger.info("✅ httpx.Client.send 已添加 405 退避")


def apply():
    global _loaded
    if _loaded:
        return
    _loaded = True

    _wrap_httpx_request()
    _wrap_httpx_client_send()
    logger.info("✅ httpx 405 退避补丁加载完成")
