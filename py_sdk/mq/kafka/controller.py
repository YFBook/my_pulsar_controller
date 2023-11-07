"""

使用方式：
0. 配置好config.kafka_consumer_info
1. 创建一个类（类A），继承KafkaConsumerControllerDelegate
2. 在类A中，实例化一个KafkaConsumerController对象（对象B）。
3. 执行对象B的add_listener函数（设置监听），一般是指向self:  self.对象B.add_listener(self)
4. 在类A实现对应的代理函数：KafkaConsumerControllerDelegate函数。
5. 执行对象A.processing_records函数即可获取kafka消息
"""


import sys
from typing import Union, List


import aiokafka
from py_sdk.util.logger import logger
import ujson
from aiokafka.structs import ConsumerRecord, TopicPartition
from aiokafka import ConsumerRebalanceListener
from py_sdk.util.dingding import DingDingService
from aiokafka.errors import KafkaError
from aiokafka.structs import RecordMetadata
import arrow
import asyncio
import traceback
import time
from enum import Enum


class KafkaMsgTimeoutType(int, Enum):
    TYPE_DEFAULT = -1
    TYPE_VALUE = 0
    TYPE_RECORD_FIELD = 1
    TYPE_MSG_CREATE_TIMESTAMP = 2


class KafkaConsumerControllerReadMsgError(KafkaError):
    errno = 101
    message = "读取/消费kafka消息异常"
    description = "消费kafka消息异常，请检查delegate实现/重写的回调函数逻辑是否正常"


class KafkaControllerTool:
    @staticmethod
    def get_value_with_record(record: Union[ConsumerRecord, List[ConsumerRecord]]):
        """
        解析kafka的消息
        """
        _value = record.value.decode()
        try:
            _value = ujson.loads(_value)
        except ujson.JSONDecodeError:
            logger.info(f"注意此条消息并不是json字符串: {record}")
        return _value

    @staticmethod
    def get_record_whthin_timeout(
        record: Union[ConsumerRecord, List[ConsumerRecord]],
        timeout: int = 0,
        timeout_type: KafkaMsgTimeoutType = KafkaMsgTimeoutType.TYPE_VALUE.value,
        timeout_field="",
    ):
        """
        判断消息是否超时
        timeout: 超时时间; 只有timeout_type=TYPE_VALUE才会生效；
        timeout_type: 判断超时的类型；TYPE_VALUE表示通过固定超时时间进行判断；TYPE_RECORD_FIELD表示根据消息自带字段值来判断
        timeout_field:
         1. 消息超时时间字段名称， 只有timeout_type=TYPE_RECORD_FIELD才会生效；
         2. 消息体内容必须是一个字典
         3. 若超时字段是多层级的子元素，则用","分割层级
         4. 值不能为空
        """
        if timeout_type == KafkaMsgTimeoutType.TYPE_DEFAULT.value:
            return record
        if isinstance(record, ConsumerRecord):
            return KafkaControllerTool.__get_record_whthin_timeout(
                record=record,
                timeout=timeout,
                timeout_type=timeout_type,
                timeout_field=timeout_field,
            )
        elif isinstance(record, List):
            list_msg = []
            for _record in record:
                _value = KafkaControllerTool.__get_record_whthin_timeout(
                    record=_record,
                    timeout=timeout,
                    timeout_type=timeout_type,
                    timeout_field=timeout_field,
                )
                if _value:
                    list_msg.append(_value)
            return list_msg
        else:
            raise Exception("未知类型，无法检查kafka超时时间")

    @staticmethod
    def __get_record_whthin_timeout(
        record: ConsumerRecord,
        timeout: int = 0,
        timeout_type: KafkaMsgTimeoutType = KafkaMsgTimeoutType.TYPE_VALUE.value,
        timeout_field="",
    ):
        now = int(time.time())
        enqueue_time = record.timestamp // 1000
        # 从消息生产到当前时间的差值
        delay_seconds = now - enqueue_time
        if timeout_type == KafkaMsgTimeoutType.TYPE_VALUE.value and timeout > 0:
            if delay_seconds < timeout:
                return record
            else:
                logger.info("消息已经超时", delay_seconds=delay_seconds, timeout=timeout)
        elif (
            timeout_type == KafkaMsgTimeoutType.TYPE_MSG_CREATE_TIMESTAMP.value
            and timeout > 0
        ):
            if enqueue_time > timeout:
                return record
            else:
                logger.info(f"消息创建时间{enqueue_time} < {timeout}; 无需消费")
        elif (
            timeout_type == KafkaMsgTimeoutType.TYPE_RECORD_FIELD.value
            and timeout_field != ""
        ):
            _value = KafkaControllerTool.get_value_with_record(record=record)
            list_field = timeout_field.split(",")
            for key in list_field:
                timeout = _value[key]
            if delay_seconds < int(timeout) and int(timeout) > 0:
                return record
            else:
                logger.info("消息已经超时", delay_seconds=delay_seconds, timeout=timeout)
        else:
            raise Exception("超时设置错误，请查看参数")


class KafkaConsumerControllerDelegate:
    # record_value是consumer_record_parse的返回值
    async def consumer_record_processing(self, record_value, tp: TopicPartition):
        """
        处理/消费 record_value 的回调函数, 通常用于实现业务逻辑
        record_value为consumer_record_parse的返回数据
        """
        raise NotImplementedError

    # record_value是ConsumerRecord对象, 消息消费成功时出发，通常是提交偏移量
    async def consumer_record_processing_success(
        self, record: Union[ConsumerRecord, List[ConsumerRecord]], tp: TopicPartition
    ):
        """
        consumer_record_processing执行成功后调用
        """
        pass

    # record是ConsumerRecord对象, kafka消息消费失败时触发，通常是直接抛出错误
    async def consumer_record_processing_failure(
        self,
        record: Union[ConsumerRecord, List[ConsumerRecord]],
        tp: TopicPartition,
        err,
    ):
        """
        consumer_record_processing执行失败/异常后调用
        """
        raise NotImplementedError

    # 解析kafka消息（应按需重写）
    async def consumer_record_parse(
        self,
        record: Union[ConsumerRecord, List[ConsumerRecord], dict, List[dict]],
    ):
        """
        如果is_read_many=False, record为ConsumerRecord
        如果is_read_many=True， record为List[ConsumerRecord]
        返回数据将会流入consumer_record_processing
        """
        if isinstance(record, ConsumerRecord):
            return KafkaControllerTool.get_value_with_record(record=record)
        elif isinstance(record, List):
            list_msg = []
            for _record in record:
                if isinstance(_record, ConsumerRecord):
                    list_msg.append(
                        KafkaControllerTool.get_value_with_record(record=_record)
                    )
                else:
                    list_msg.append(_record)
            return list_msg
        else:
            return record

    # 判断kafka消息是否是合法消息；True则表示合法，继续流入其他代理；False则跳过此条消息
    async def consumer_record_check_is_valid(
        self, record: Union[ConsumerRecord, List[ConsumerRecord]], tp: TopicPartition
    ):
        return True

    async def on_partitions_revoked(self, revoked):
        """
        rebalance操作开始之前和消费者停止获取数据之后被调用。
        如果使用的是手动提交， 可以在这里提交所有消耗的偏移量
        以避免rebalance完成后的重复信息传递
        """
        pass

    async def on_partitions_assigned(self, assigned):
        """
        在分区重新分配完成后与在消费者再次开始获取数据之前被调用。
        assigned: set of TopicPartition (源代码注释为list，实际上不是)
        """
        pass


class KafkaConsumerController(ConsumerRebalanceListener):
    """
    顶部有使用方式说明
    使用只需要关心KafkaConsumerControllerDelegate的实现/重写即可，无需关心内部实现（除非修bug或新增其他功能）
    默认批量读取、不自动提交偏移量
    """

    delegate: KafkaConsumerControllerDelegate = NotImplemented

    # kafka_consumer_info需要包含bootstrap_servers、group_id、topic（兼容AIOKafkaConsumer初始化配置字段）
    def __init__(
        self,
        is_read_many=True,
        is_auto_commit=False,
        kafka_consumer_info: dict = None,
        auto_offset_reset="earliest",
    ) -> None:
        """
        is_read_many为True时，批量消费kafka消息；默认True
        is_auto_commit控制是否可以自动提交偏移量；True为自动提交； 默认False
        get_many_max_records为批量消费时消息的最大记录数
        get_many_timeout_ms: 批量读取时，如果缓冲区没有数据时的等待时间,单位ms（毫秒）
        """
        self.__group_id = kafka_consumer_info.get("group_id", None)
        self.__topic = kafka_consumer_info.get("topic", None)
        if kafka_consumer_info is None:
            raise Exception("kafka_consumer_info不能为空")
        if self.__topic is None:
            raise Exception("kafka_consumer_info内topic不能为空")
        if self.__group_id is None:
            raise Exception("kafka_consumer_info内group_id不能为空")
        del kafka_consumer_info["topic"]
        # 无需传入topic；使用subscribe函数同样可以订阅topic
        self.kafka_consumer = aiokafka.AIOKafkaConsumer(
            **kafka_consumer_info,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=is_auto_commit,
        )

        self.is_read_many = is_read_many
        # 一轮批量消费消息的最大数量
        self.get_many_max_records = kafka_consumer_info.get("max_poll_records", 1000)
        # 读取分区的超时时间，如果设置为0，则分区没有未消费的消息时就会立即返回
        self.get_many_timeout_ms = kafka_consumer_info.get(
            "get_many_timeout_ms", 1000 * 5
        )
        # 消费 同一批/同一个 消息的累计失败次数，超过一定次数时控制器直接崩溃
        self.err_count = 0
        if isinstance(self.__topic, List):
            self.kafka_consumer.subscribe(self.__topic, listener=self)
        else:
            self.kafka_consumer.subscribe([self.__topic], listener=self)
        # kafka_consumer分配到的TopicPartition
        self.list_assigned_topic_partition = []
        # kafka_consumer是否已经执行start了
        self.is_start = False
        # 记录最后5次reblance的时间戳
        self.list_rebalance_timestamp = []

        self.is_check_timeout = False

        # rebalance时回调函数执行发生的异常
        self.__kafka_listener_error = None

        self.dd_url = ""
        self.dd_tag = ""
        self.dd_title = ""

        # 连续消费达到一定次数后可告警
        self.consume_err_warnning_count = 4

    # 设置告警钉钉url，告警关键词，告警信息标题
    def set_dd_config(self, url, tag, title):
        self.dd_tag = tag
        self.dd_url = url
        self.dd_title = title

    def add_listener(self, delegate):
        """
        设置回调监听的对象
        """
        self.delegate = delegate

    def set_timeout(
        self,
        timeout: int = 0,
        timeout_type: KafkaMsgTimeoutType = KafkaMsgTimeoutType.TYPE_VALUE.value,
        timeout_field: str = "",
    ):
        """
        设置超时判断; 当前时间与消息的创建时间进行对比，超过timeout即为超时
        timeout: 超时时间; 只有timeout_type=TYPE_VALUE才会生效；
        timeout_type: 判断超时时间的来源；TYPE_VALUE表示超时时间为timeout；TYPE_RECORD_FIELD表示根据消息自带字段值/timeout_field为timeout
        timeout_field:
         1. 消息超时时间字段名称， 只有timeout_type=TYPE_RECORD_FIELD才会生效；
         2. 消息体内容必须是一个字典
         3. 若超时字段是多层级的子元素，则用","分割层级
         4. 值不能为空
        """
        if timeout_type == KafkaMsgTimeoutType.TYPE_DEFAULT.value:
            self.is_check_timeout = False
            self.msg_timeout = 0
            self.msg_timeout_type = timeout_type
            self.msg_timeout_field = ""
        else:
            self.is_check_timeout = True
            self.msg_timeout = timeout
            self.msg_timeout_type = timeout_type
            self.msg_timeout_field = timeout_field

    async def __check_rebalance_timestamp(self):
        """
        检查每次rebalance的时间；
        如果在短时间内连续rebalance则会发出告警
        """
        self.list_rebalance_timestamp.append(arrow.now().int_timestamp)
        self.list_rebalance_timestamp.sort()
        list_len = 5
        self.list_rebalance_timestamp = self.list_rebalance_timestamp[-list_len:]
        if len(self.list_rebalance_timestamp) >= list_len:
            is_warning = True
            # 当最近5次rebalance的时间间隔都小于这个时间的时候就会告警
            warning_interval = 60 * 5
            for i in range(0, len(self.list_rebalance_timestamp) - 1, 1):
                rebalance_1 = self.list_rebalance_timestamp[i]
                rebalance_2 = self.list_rebalance_timestamp[i + 1]
                if rebalance_2 - rebalance_1 > warning_interval:
                    is_warning = False
            if is_warning is True:
                logger.exception(f"连续发生rebalance,时间段为: {self.list_rebalance_timestamp}")
                DingDingService.send_text_by_robot(
                    self.dd_url,
                    f"标题：{self.dd_title} - {self.dd_tag}\n告警信息: kafka消费组连续rebalance,可能会消费阻塞\n group_id: {self.__group_id}\n topic: {self.__topic}",
                )

    async def start(self):
        if self.is_start is False:
            await self.kafka_consumer.start()
            self.is_start = True
            self.err_count = 0

    async def close(self):
        self.err_count = 0
        self.is_start = False
        self.list_assigned_topic_partition = []
        # 没有start直接close也不会有问题
        await self.kafka_consumer.stop()

    async def commit(
        self, record: Union[ConsumerRecord, List[ConsumerRecord]], tp: TopicPartition
    ):
        """
        根据tp（partition）和record提交偏移量
        当rebalance未完成时无法提交
        """
        try:
            # 防止提交到没有被assigned到的分区(消费消息的时候发生rebalance则可能出现该情况)
            if tp in self.list_assigned_topic_partition:
                if self.is_read_many:
                    await self.kafka_consumer.commit({tp: record[-1].offset + 1})
                else:
                    await self.kafka_consumer.commit({tp: record.offset + 1})
                logger.info("offset commit success..")
            else:
                logger.warning(
                    f"group_id: {self.__group_id}\n无法提交偏移量，因为tp: {tp} ，不属于消费者: {self.list_assigned_topic_partition}"
                )
        except Exception as e:
            logger.warning(f"提交kafka偏移量失败，错误: {e}")
            DingDingService.send_text_by_robot(
                self.dd_url,
                f"标题(崩溃)：{self.dd_title} - {self.dd_tag}\ngroup_id: {self.__group_id}\nkafka偏移量提交失败: {e}\n错误详情：{traceback.format_exc()}",
            )
            self.err_count = self.consume_err_warnning_count
            raise e

    async def seek_to_committed(self, tp: TopicPartition):
        """
        将偏移量回退到上一次提交的位置
        """
        try:
            if tp in self.list_assigned_topic_partition:
                # seek_to_committed不传入partition，则会回退消费者所有assigned到的分区偏移量。
                dict_partition_offset = await self.kafka_consumer.seek_to_committed(tp)
                offset = dict_partition_offset[tp]
                # 如果该group id的consumer在当前partition中从未commit过，则offset为None,此时seek_to_committed方法无效
                if offset is None:
                    await self.kafka_consumer.seek_to_beginning(tp)
                logger.info("kafka 偏移量回退完成..")
            else:
                logger.warning(
                    f"group_id: {self.__group_id}\n无法seek_to_commited，因为tp: {tp} ，不属于消费者: {self.list_assigned_topic_partition}"
                )
        except Exception as e:
            logger.warning(f"kafka偏移量回退失败，错误: {e}")
            DingDingService.send_text_by_robot(
                self.dd_url,
                f"标题(崩溃)：{self.dd_title} - {self.dd_tag}\ngroup_id: {self.__group_id}\nkafka偏移量回退失败: {e}\n错误详情：{traceback.format_exc()}",
            )
            self.err_count = self.consume_err_warnning_count
            raise e

    async def processing_records(self):
        """
        消费kafka消息
        """
        if self.delegate is NotImplemented:
            logger.warning("没有实现delegate")
            return
        await self.start()
        while True:
            catch_err_msg = ""
            push_to_dd_err_msg = ""
            catch_err = None
            if self.is_start is False:
                logger.warning("kafka控制器已经关闭")
                break
            logger.info("-----准备读取kafka消息-----")
            if self.__kafka_listener_error:
                logger.warning(
                    f"on_partitions_revoked或on_partitions_assigned执行异常,无法继续消费:{self.__kafka_listener_error.__str__()}"
                )
                await self.close()
                raise Exception(
                    f"on_partitions_revoked或on_partitions_assigned执行异常:{self.__kafka_listener_error.__str__()}"
                )
            try:
                if self.is_read_many:
                    await self.__get_many()
                else:
                    await self.__get_one_by_one()
            except KafkaError as e:
                push_to_dd_err_msg = (
                    f"标题(崩溃)：{self.dd_title} - {self.dd_tag};kafka error\n错误详情：{e}"
                )
                catch_err = e
                DingDingService.send_text_by_robot(self.dd_url, push_to_dd_err_msg)
                sys.exit()
            except KafkaConsumerControllerReadMsgError as e:
                if self.err_count >= self.consume_err_warnning_count:
                    catch_err_msg = f"kafka消费组连续消费已达最大失败次数，错误:{e}"
                    push_to_dd_err_msg = f"标题(崩溃)：{self.dd_title} - {self.dd_tag}\nkafka消费消息连续失败次数已达上限: {self.consume_err_warnning_count}\n错误详情：{traceback.format_exc()}"
                    catch_err = e
                else:
                    push_to_dd_err_msg = f"kafka控制器-消息消费异常: {e.__str__()}"
                    catch_err_msg = push_to_dd_err_msg
            except Exception as e:
                self.err_count = self.err_count + 1
                if self.err_count >= self.consume_err_warnning_count:
                    catch_err_msg = f"kafka消费组连续消费已达最大失败次数，错误:{e}"
                    push_to_dd_err_msg = f"标题(崩溃)：{self.dd_title} - {self.dd_tag}\nkafka消费消息连续失败次数已达上限: {self.consume_err_warnning_count}\n错误详情：{traceback.format_exc()}"
                    catch_err = e
                else:
                    catch_err_msg = f"kafka控制器-消息处理失败，错误:{e}"
            finally:
                if catch_err_msg:
                    logger.exception(catch_err_msg)
                if push_to_dd_err_msg:
                    DingDingService.send_text_by_robot(self.dd_url, push_to_dd_err_msg)
                if catch_err is not None:
                    await self.close()
                    raise catch_err

    async def __get_many(self):
        """
        批量消费消息
        """
        list_records = await self.kafka_consumer.getmany(
            max_records=self.get_many_max_records,
            timeout_ms=self.get_many_timeout_ms,
        )
        for tp, records in list_records.items():
            if tp in self.list_assigned_topic_partition:
                if records:
                    if await self.__processing(record=records, tp=tp):
                        break
                else:
                    logger.info(f"kafka消息不存在: {records}")
            else:
                logger.warning(
                    f"无法消费消息，因为tp: {tp} ，不属于: {self.list_assigned_topic_partition}"
                )

    async def __get_one_by_one(self):
        """
        逐条消费消息
        """
        async for record in self.kafka_consumer:
            if record:
                tp = aiokafka.TopicPartition(record.topic, record.partition)
                if tp in self.list_assigned_topic_partition:
                    if await self.__processing(record, tp):
                        break
                else:
                    logger.warning(
                        f"无法消费消息，因为tp: {tp} ，不属于: {self.list_assigned_topic_partition}"
                    )
            else:
                logger.info(f"kafka消息不存在: {record}")

    async def __processing(
        self, record: Union[ConsumerRecord, List[ConsumerRecord]], tp: TopicPartition
    ):
        """
        将kafka消息提交给代理函数处理
        消息消费正常返回False
        异常返回True；表示跳过本次消息
        """
        error = None
        try:
            if not await self.delegate.consumer_record_check_is_valid(record, tp):
                await self.delegate.consumer_record_processing_success(record, tp)
                self.err_count = 0
                return False
            if self.is_check_timeout:
                _record = KafkaControllerTool.get_record_whthin_timeout(
                    record=record,
                    timeout=self.msg_timeout,
                    timeout_type=self.msg_timeout_type,
                    timeout_field=self.msg_timeout_field,
                )
            else:
                _record = record
            if _record:
                record_value = await self.delegate.consumer_record_parse(_record)
                await self.delegate.consumer_record_processing(record_value, tp)
            else:
                logger.info("消息超时无需处理")
            await self.delegate.consumer_record_processing_success(record, tp)
            self.err_count = 0
            return False
        except Exception as e:
            error = e
            self.err_count = self.err_count + 1
        finally:
            if self.err_count > 0:
                try:
                    if error:
                        await self.delegate.consumer_record_processing_failure(
                            record, tp, error
                        )
                except Exception as failure_err:
                    self.err_count = self.consume_err_warnning_count
                    logger.warning(
                        f"consumer_record_processing_failure执行失败--失败次数调整到最大，失败概要:{failure_err}"
                    )
                if self.err_count >= self.consume_err_warnning_count:
                    logger.exception(
                        "kafka消费异常已达最大限制--抛出异常KafkaConsumerControllerReadMsgError"
                    )
                    raise KafkaConsumerControllerReadMsgError(
                        "消费kafka消息异常，请检查delegate实现/重写的回调函数逻辑是否正常"
                    )
                return True

    # ---------------------------------ConsumerRebalanceListener----------------------
    async def on_partitions_revoked(self, revoked):
        """
        rebalance前触发（分区前触发）
        revoked: list of TopicPartition
        """
        # rebalance的时候禁止提交偏移量（会报错）
        logger.info("开始rebalance，消费者所分配的分区被清空(重新分区)")
        try:
            self.__kafka_listener_error = None
            # rebalance的时候禁止提交偏移量（会报错）
            self.list_assigned_topic_partition = []
            await self.__check_rebalance_timestamp()
            if self.delegate is not NotImplemented:
                await self.delegate.on_partitions_revoked(revoked=revoked)
            # 不进行上一批消息消费任务/协程的cancel，容易引发更多未知异常。
        except Exception as error:
            self.__kafka_listener_error = error

    async def on_partitions_assigned(self, assigned):
        """
        rebalance完成且分区已经分配完毕后触发
        assigned: set of TopicPartition (源代码注释为list，实际上不是)
        """
        try:
            self.list_assigned_topic_partition = list(assigned)
            logger.info(f"分区完毕，消费者被分配到的tp: {assigned}")
            if self.delegate is not NotImplemented:
                await self.delegate.on_partitions_assigned(assigned=assigned)
        except Exception as error:
            self.__kafka_listener_error = error


class KafkaProducerController:
    # kafka_producer_info至少包含bootstrap_servers
    def __init__(
        self,
        topic="",
        acks=1,
        kafka_producer_info: dict = None,
        **kwargs,
    ):
        """
        - topic: 推送到下游的topic/send方法的默认topic；默认值为空；
        - acks: 当producer生产的消息被不同id的消费组消费时，acks最好设置成all
        - kafka_producer_info： AIOKafkaProducer初始化配置
        """
        self.__topic = topic
        if kafka_producer_info is None:
            raise Exception("kafka_producer_info不能为None")
        self.__producer = aiokafka.AIOKafkaProducer(
            acks=acks, **kafka_producer_info, **kwargs
        )
        self.is_start = False

    def get_producer(self) -> aiokafka.AIOKafkaProducer:
        return self.__producer

    def set_topic(self, topic):
        self.__topic = topic

    def get_topic(self):
        return self.__topic

    async def start(self):
        logger.info("kafka生产者开始启动..")
        if self.is_start is True:
            logger.info("kafka生产者启动失败/kafka生产者无法重复启动")
            return
        await self.__producer.start()
        logger.info("kafka生产者启动完成..")
        self.is_start = True

    async def close(self):
        logger.info("kafka生产者开始关闭..")
        await self.__producer.stop()
        self.is_start = False
        logger.info("kafka生产者关闭完成..")

    def transaction(self):
        return self.__producer.transaction()

    async def send(
        self,
        value: Union[str, bytes, dict, list],
        key: Union[str, bytes] = None,
        topic="",
        is_print_result=False,
    ) -> RecordMetadata:
        """
        发送消息到kafka
        - value： 推送的值
        - key: 推送的分区key
        - topic: 推送消息到哪个topic；默认为空；空值时默认取初始化时的topic
        """
        if self.is_start is False:
            raise Exception("生产者没有启动")

        if not self.__topic and not topic:
            raise Exception("无法send，因为topic没有设置")

        if isinstance(value, (list, dict)):
            value = ujson.dumps(value)
        value = value.encode() if isinstance(value, str) else value
        send_topic = topic if topic else self.__topic
        result = ""
        if key:
            if isinstance(key, int):
                key = f"{key}"
            key = key.encode() if isinstance(key, str) else key
            result = await self.__producer.send_and_wait(
                topic=send_topic, key=key, value=value
            )
        else:
            result = await self.__producer.send_and_wait(topic=send_topic, value=value)
        if result and is_print_result:
            logger.info(f"推送结果: {result}")
        return result

    async def get_partition_num(self, topic=""):
        """
        根据初始化的topic，获取topic的分区数量
        """
        if self.is_start is False:
            raise Exception("生产者没有启动")

        if not self.__topic and not topic:
            raise Exception("无法send，因为topic没有设置")
        _topic = topic if topic else self.__topic
        return len(list(await self.__producer.partitions_for(_topic)))
