"""
使用方式：底部有demo
特别注意：一个queue对象只在一个协程内执行/不要多协程使用queue
"""
import os
import sys
from typing import Callable, Any, List, Union

from pandas import DataFrame

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
)
import asyncio
from asyncio.queues import Queue

from py_sdk.util.logger import logger

from enum import Enum


class QueueRunType(Enum):
    ALL = "ALL"
    ANY = "ANY"
    MISS_ERR = "MISS_ERR"


class QueueController:
    def __init__(self, queue_max_size=10, run_type=QueueRunType.ALL.value) -> None:
        """
        run_type = ALL， 表示item在被消费时（run）发生异常后，后续的item消费都会跳过，当item消费完后抛出异常
        run_type = ANY, 表示item在被消费时（run）发生异常后，后续的item消费会继续，当item消费完后依旧会抛出异常
        run_type = MISS
            1.item在被消费时（run）发生异常后，后续的item消费会继续，当item消费完后不会抛出异常；
            2.若callback_failure执行失败，当item消费完后也会抛出异常
        """
        self.queue = Queue(maxsize=queue_max_size)
        self.queue_max_size = queue_max_size
        self.queue_items = []

        # 消费队列item的回调
        self.__callback = None
        # 消费队列item失败时的回调
        self.__callback_failure = None
        # 判断在消费时是否发生失败
        self.__is_callback_all_done = True
        self.__finished_print = ""
        self.__run_type = run_type
        self.__run_err = None
        self.name = ""
        self.__queue_done_callback = None

    @property
    def item_size(self):
        return len(self.queue_items)

    def update_run_type(self, new_type: QueueRunType):
        self.__run_type = new_type

    def set_queue_max_size(self, queue_max_size):
        self.queue_max_size = queue_max_size
        self.queue = Queue(maxsize=queue_max_size)

    def put_items(self, items: List, is_reset=True):
        """
        遍历items并将遍历值作为item传入队列中，等待回调函数消费
        需要注意已有的item是否还使用，否则需要重置（clean_items）
        """
        if is_reset is True:
            self.clean_items()
        self.queue_items.extend(items)

    def set_run_finished_print(self, content):
        """
        items消费完毕后的日志，打印次数 = queue_max_size
        """
        self.__finished_print = content

    def put_items_of_divided(
        self, items: Union[List, DataFrame], sub_item_length, is_reset=True, tag=None
    ):
        """
        将一个长迭代器切割成多个短迭代器,并放入队列中作为item用于消费;
        若传入tag，队列消费的item将是一个字典，key为tag和value,value为切割后的迭代器;
        若不传入tag，队列消费的item为切割后的迭代器
        """
        if is_reset is True:
            self.clean_items()
        for i in range(0, len(items), sub_item_length):
            sub_items = items[i : i + sub_item_length]
            if tag:
                self.queue_items.append(dict(tag=tag, value=sub_items))
            else:
                self.queue_items.append(sub_items)

    def put(self, item: Any):
        """
        添加需要处理的数据
        需要注意已有的item是否还使用，否则需要重置（clean_items）
        """
        self.queue_items.append(item)

    def clean_items(self):
        """
        清空/重置队列需要处理的数据
        """
        self.queue_items = []

    def add_item_handler(self, callback: Callable):
        """
        添加队列处理数据的逻辑函数
        callback类型：func， 入参为item
        """
        self.__callback = callback

    def add_item_failure_handler(self, callback: Callable):
        """
        添加队列处理数据失败后接收Exception的逻辑函数
        callback类型：func， 入参为error, item
        """
        self.__callback_failure = callback

    def add_run_done_callback(self, callback: Callable):
        """
        添加队列item全部消费结束后的回调
        callback类型：func， 入参为item
        """
        self.__queue_done_callback = callback

    async def queue_handler(self):
        """
        队列处理数据逻辑
        """
        while True:
            try:
                item = await self.queue.get()
                if item is None:
                    break
                elif (
                    self.__run_type == QueueRunType.ALL.value
                    and self.__is_callback_all_done is False
                ):
                    # 不能在这里break，否则队列永远无法消费完,只能跳过
                    pass
                else:
                    if self.__callback:
                        await self.__callback(item)
                    else:
                        logger.info("需要先添加回调函数，请先执行add_callback")
            except Exception as e:
                if self.__run_type in [QueueRunType.ALL.value, QueueRunType.ANY.value]:
                    self.__is_callback_all_done = False
                self.__run_err = e
                if self.__callback_failure:
                    try:
                        await self.__callback_failure(e, item)
                    except Exception as e:
                        self.__is_callback_all_done = False
                        logger.info(f"callback_failure 执行失败:{e}")
            finally:
                self.queue.task_done()

    async def run(self):
        """
        开始让队列处理数据
        """
        if self.__callback:
            logger.info(f"{self.name}队列开始消费..")
            self.__is_callback_all_done = True
            self.__run_err = None
            coro_list = [
                asyncio.create_task(self.queue_handler())
                for _ in range(self.queue_max_size)
            ]
            for param in self.queue_items:
                await self.queue.put(param)
                # 跳出循环
            for _ in range(self.queue_max_size):
                await self.queue.put(None)
            await asyncio.wait(coro_list)

            if self.__queue_done_callback:
                await self.__queue_done_callback()

            if self.__finished_print:
                logger.info(self.__finished_print)
            else:
                logger.info("queue finished..")
            if self.__is_callback_all_done is False:
                raise self.__run_err

        else:
            raise Exception("需要先添加回调函数，请先执行add_callback")


async def my_callback(num):
    if num == 5:
        raise Exception("test")
    print(f"num = {num}")
    await asyncio.sleep(num)


async def my_callback_failure(error, itme):
    print(f"item = {itme}, erro = {error}")
    raise Exception("test")


async def Demo():
    # 测试用例
    queue_controller = QueueController(
        queue_max_size=5,
        # run_type=QueueRunType.MISS_ERR.value
    )
    queue_controller.clean_items()
    queue_controller.put_items([1, 2, 3, 4, 5, 6, 7, 8, 9])
    queue_controller.add_item_handler(callback=my_callback)
    queue_controller.add_item_failure_handler(callback=my_callback_failure)
    await queue_controller.run()
    print("测试完毕")


if __name__ == "__main__":
    asyncio.run(Demo())
