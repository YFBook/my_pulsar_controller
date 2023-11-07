#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/27 10:34
# @Author  : pangxiaolong@asinking.com
"""
desc: 记录方法的执行时间
"""


import traceback
from typing import Callable, Any


import asyncio

import time
import functools
import threading


from py_sdk.util.logger import logger
from py_sdk.util.dingding import DingDingService


def async_exec_time(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        ret = await func(*args, **kwargs)
        logger.info(f"function: {func.__name__}, 花费时间: {time.time() - start_time}")
        return ret

    return wrapper


def singleton(cls):
    """单例模式装饰器"""
    instances = {}
    lock = threading.Lock()

    def _singleton(*args, **kwargs):
        with lock:
            fullkey = str((cls.__module__, cls.__name__))
            if fullkey not in instances:
                instances[fullkey] = cls(*args, **kwargs)
        return instances[fullkey]

    return _singleton


def send_crash_msg(file_name, project_name="", dd_url=""):
    if dd_url == "":
        raise Exception("send_crash_msg方法需要配置dd_url")

    def catch_func(func):
        @functools.wraps(func)
        async def wraps(*args, **kwargs):
            try:
                await func(*args, **kwargs)
            except Exception:
                DingDingService.send_text_by_robot(
                    dd_url,
                    f"project_name：{project_name}\n崩溃脚本: {file_name}\n错误信息：{traceback.format_exc()}",
                )

        return wraps

    return catch_func


def func_exec_timeout(func_name, execute_timeout, dd_url="", project_name=""):
    if dd_url == "":
        raise Exception("send_crash_msg方法需要配置dd_url")

    def inner(func):
        @functools.wraps(func)
        async def wraps(*args, **kwargs):
            try:
                done, pending = await asyncio.wait(
                    [func(*args, **kwargs)], timeout=execute_timeout
                )
                if done:
                    for task in done:
                        result = task.result()
                        return result
                if pending:
                    for task in pending:
                        task.cancel()
                    DingDingService.send_text_by_robot(
                        dd_url,
                        f"project_name：{project_name}\n函数崩溃: {func_name}\n错误信息：执行超时，已经超过了{execute_timeout}s, 函数自动抛出异常",
                    )
                    raise Exception(f"方法执行超时,已经超过{execute_timeout}")
            except Exception as e:
                raise e

        return wraps

    return inner


# 重试装饰器，重试次数为retry_time
def retry(retry_time: int = 4, dd_url="", project_name="") -> Callable:
    def decorator(function: Callable) -> Callable:
        @functools.wraps(function)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            count = 1
            while count < retry_time:
                try:
                    return await function(*args, **kwargs)
                except Exception as e:
                    error_msg = traceback.format_exc()
                    if (
                        "数据不存在" not in error_msg
                        and function.__name__ not in "open"
                        and dd_url
                    ):
                        DingDingService.send_text_by_robot(
                            dd_url,
                            f"project_name：{project_name}\n异常/崩溃函数: {function.__name__}\n错误信息：{error_msg}",
                        )
                    logger.exception(e)
                    count += 1
                    await asyncio.sleep(count * 3)
            return await function(*args, **kwargs)

        return wrapper

    return decorator
