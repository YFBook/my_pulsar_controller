import atexit as _atexit
import sys
import traceback
from multiprocessing import current_process
from os.path import basename, splitext
from threading import current_thread
from loguru._logger import Core as _Core
from loguru._logger import Logger, start_time, context
from loguru._colorizer import Colorizer
from loguru._datetime import aware_now
from loguru._get_frame import get_frame
from loguru._recattrs import (
    RecordException,
    RecordFile,
    RecordLevel,
    RecordProcess,
    RecordThread,
)
import ujson


class CustomLogger(Logger):
    """
    重写_log方法， 调整关键字参数逻辑
    """

    def _log(
        self, level_id, static_level_no, from_decorator, options, message, args, kwargs
    ):
        core = self._core

        if not core.handlers:
            return

        (exception, depth, record, lazy, colors, raw, capture, patcher, extra) = options

        frame = get_frame(depth + 2)

        try:
            name = frame.f_globals["__name__"]
        except KeyError:
            name = None

        try:
            if not core.enabled[name]:
                return
        except KeyError:
            enabled = core.enabled
            if name is None:
                status = core.activation_none
                enabled[name] = status
                if not status:
                    return
            else:
                dotted_name = name + "."
                for dotted_module_name, status in core.activation_list:
                    if dotted_name[: len(dotted_module_name)] == dotted_module_name:
                        if status:
                            break
                        enabled[name] = False
                        return
                enabled[name] = True

        current_datetime = aware_now()

        if level_id is None:
            level_icon = " "
            level_no = static_level_no
            level_name = "Level %d" % level_no
        else:
            try:
                level_name, level_no, _, level_icon = core.levels[level_id]
            except KeyError:
                raise ValueError("Level '%s' does not exist" % level_id) from None

        if level_no < core.min_level:
            return

        code = frame.f_code
        file_path = code.co_filename
        file_name = basename(file_path)
        thread = current_thread()
        process = current_process()
        elapsed = current_datetime - start_time

        if exception:
            if isinstance(exception, BaseException):
                type_, value, traceback = (
                    type(exception),
                    exception,
                    exception.__traceback__,
                )
            elif isinstance(exception, tuple):
                type_, value, traceback = exception
            else:
                type_, value, traceback = sys.exc_info()
            exception = RecordException(type_, value, traceback)
        else:
            exception = None

        log_record = {
            "elapsed": elapsed,
            "exception": exception,
            "extra": {**core.extra, **context.get(), **extra},
            "file": RecordFile(file_name, file_path),
            "function": code.co_name,
            "level": RecordLevel(level_name, level_no, level_icon),
            "line": frame.f_lineno,
            "message": str(message),
            "module": splitext(file_name)[0],
            "name": name,
            "process": RecordProcess(process.ident, process.name),
            "thread": RecordThread(thread.ident, thread.name),
            "time": current_datetime,
        }

        if lazy:
            args = [arg() for arg in args]
            kwargs = {key: value() for key, value in kwargs.items()}

        if capture and kwargs:
            log_record["extra"].update(kwargs)

        if record:
            if "record" in kwargs:
                raise TypeError(
                    "The message can't be formatted: 'record' shall not be used as a keyword "
                    "argument while logger has been configured with '.opt(record=True)'"
                )
            kwargs.update(record=log_record)

        if colors:
            if args or kwargs:
                colored_message = Colorizer.prepare_message(message, args, kwargs)
            else:
                colored_message = Colorizer.prepare_simple_message(str(message))
            log_record["message"] = colored_message.stripped

        else:
            colored_message = None

        if core.patcher:
            core.patcher(log_record)

        if patcher:
            patcher(log_record)

        for handler in core.handlers.values():
            handler.emit(log_record, level_id, from_decorator, raw, colored_message)


def log_json_format(record):
    """
    统一输出格式为json
    :param record:
    :return:
    """
    msg_dict = {
        "levelname": record["level"].name,
        "msg": record.get("message"),
        "log_time": record["time"].strftime("%Y-%m-%d %H:%M:%S.%f"),
        "path": record["file"].path,
        "function": record["function"],
        "line": record["line"],
        "exc_info": traceback.format_exc(),
    }
    msg_dict.update(record["extra"])
    msg_json = ujson.dumps(msg_dict, ensure_ascii=False)
    record["msg_json"] = msg_json
    return "{msg_json}\n"


"""
The Loguru library provides a pre-instanced logger to facilitate dealing with logging in Python.

Just ``from loguru import logger``.
"""

__version__ = "0.5.3"

__all__ = ["logger"]

logger = CustomLogger(_Core(), None, 0, False, False, False, False, True, None, {})

logger.add(sys.stdout, format=log_json_format)

_atexit.register(logger.remove)
