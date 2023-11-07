import os
import sys

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
)
from pulsar import (
    Client,
    AuthenticationToken,
    InitialPosition,
    ConsumerType,
    Consumer,
    MessageId,
    Message,
    ConsumerBatchReceivePolicy,
)

import ujson
import abc
from typing import Union, List

from enum import Enum
from py_sdk.util.logger import logger
import time


class PulsarConsumerControllerDelegate(abc.ABC):
    @abc.abstractmethod
    async def PulsarMessageReceive(self, msg: Union[Message, List[Message]]):
        pass

    @abc.abstractmethod
    async def PulsarMessageReceiveFailure(
        self, msg: Union[Message, List[Message]], err: Exception
    ):
        pass


class PulsarConsumerType(Enum):
    key_shared = ConsumerType.KeyShared
    share = ConsumerType.Shared
    exclusive = ConsumerType.Exclusive


class PulsarConsumerPositionType(Enum):
    latest = InitialPosition.Latest
    earliest = InitialPosition.Earliest


class PulsarConsumerController:
    def __init__(
        self,
        consumer_info: dict,
        postion: PulsarConsumerPositionType = PulsarConsumerPositionType.earliest,
        consumer_type: PulsarConsumerType = PulsarConsumerType.key_shared,
        is_auto_ack: bool = True,
        is_auto_nack: bool = True,
        min_publish_timestamp: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        - consumer_info：消费者订阅配置,包括如下字段
            - service_url: pulsar集群地址；必要字段
            - auth_token: 角色密钥；必要字段
            - subscription_name：消费者的group id，类似于Kafka的group id;必要字段。
            - consumer_name：消费者名称；必要字段。
            - topic：要消费的主题路径；必要字段。
            - negative_ack_redelivery_delay_ms：默认值为60000，即NACK消息（使用consumer.negative_acknowledge()）后，重新投递该消息前的等待时间；主动重新投递
            - batch_receive_max_num：批量消费时，最多可以接收的消息条数，默认值为500。
            - batch_receive_max_bytes：批量消费时，最多可以接收的数据大小，默认值为300 * 1000 ms。
            - batch_receive_timeout_ms：批量消费时，读取消息的超时时间，默认值为1000ms。
            - unacked_messages_timeout_ms： 消息取出来后没有进行ack或者nack的超时时间，超时后消息会重新投递; 需远大于negative_ack_redelivery_delay_ms；被动重新投递; 默认300000ms
            - max_retry_count: 消息最大重试次数；如果为0表示无限重试，如果大于0则表示消息在超过指定次数后会被自动ack;默认为0
            - msg_timeout: 消息过时时间；若当前时间和发布时间的间隔大于这个时间，则消息直接ack（不进行消费）；单位秒；
        - position：订阅主题时设置消费者的初始位置，默认为InitialPosition.Earliest（从最早开始）。
        -consumer_type:消费者订阅方式；默认key_shared;
            - 在 exclusive 模式下，只有一个消费者可以接收到来自该订阅的消息。如果多个消费者尝试订阅相同的主题和订阅名称，则只有第一个消费者会成功创建订阅。其他消费者将无法接收到来自该订阅的任何消息;
            - 在 shared 模式下，多个消费者可以“共享”同一个订阅，每个消费者会按照一定的规则接收到来自该订阅的部分消息。例如，Pulsar 可以使用 Round-Robin 或 Sticked Hash 等算法来分配消息给不同的消费者，并确保相同的消息只会发送到其中一个消费者;
            - 在共享模式中，消费者可以自由加入或退出消费组，而且只有处于活动状态的消费者才会接收到消息。因此，在采用这种模式时，需要考虑如何处理消费者加入或退出组的情况，以避免重复消费或消息积压等问题;
            - 在 failover 模式下，只有一个消费者可以接收来自该订阅的消息，其他消费者仅在当前消费者失败时才会成为活动消费者。当消费者组中的某个消费者发生故障或停止运行时，Pulsar 会自动选择一个备用消费者来接收未处理的消息。这种模式通常用于需要高可用性的场景，以确保即使出现故障，也不会丢失任何重要的消息;
            - 在 key_shared 模式下，相同键的消息总是被分配给同一个消费者。这种模式通常用于需要特定消费者处理特定类型消息的场景，并且可以确保所有具有相同键的消息都被发送到相同的消费者;
        - is_auto_ack：如果为True，则PulsarMessageReceive执行成功后会自动ack消息，默认为False。
        - is_auto_nack：如果为True，则PulsarMessageReceive执行失败后会自动nack消息，默认为False。
        - max_retry_count：消息重试次数，如果为0表示无限重试，如果大于0则表示消息在超过指定次数后会被自动ack
        - min_publish_timestamp: 若大于0，则所有publish time小于这个时间戳的消息都不消费（直接ack）；单位s
        """
        # 类似kafka的group id
        subscription_name = consumer_info.get("subscription_name", "")
        if subscription_name == "":
            raise Exception("consumer_info中subscription_name不能为空")

        # 消费组的名字
        consumer_name = consumer_info.get("consumer_name", "")
        if consumer_name == "":
            raise Exception("consumer_info中consumer_name不能为空")
        topic = consumer_info.get("topic", "")
        if topic == "":
            raise Exception("consumer_info中topic不能为空")

        service_url = consumer_info.get("service_url", "")
        if service_url == "":
            raise Exception("consumer_info中service_url不能为空")

        auth_token = consumer_info.get("auth_token", "")
        if auth_token == "":
            raise Exception("consumer_info中auth_token不能为空")

        self.__consumer = Client(
            service_url, authentication=AuthenticationToken(auth_token)
        ).subscribe(
            topic=topic,
            initial_position=postion.value,
            subscription_name=subscription_name,
            consumer_type=consumer_type.value,
            consumer_name=consumer_name,
            # 试无法处理的消息（使用consumer.negative_acknowledge()）之后的延迟。
            negative_ack_redelivery_delay_ms=consumer_info.get(
                "negative_ack_redelivery_delay_ms", 300 * 1000
            ),
            # 控制批量消费条数、大小、超时时间
            batch_receive_policy=ConsumerBatchReceivePolicy(
                consumer_info.get("batch_receive_max_num", 500),
                consumer_info.get("batch_receive_max_bytes", 10 * 1024 * 1024),
                consumer_info.get("batch_receive_timeout_ms", 1000),
            ),
            unacked_messages_timeout_ms=consumer_info.get(
                "unacked_messages_timeout_ms", 300 * 1000
            ),
        )

        self.__is_auto_ack = is_auto_ack
        self.__is_auto_nack = is_auto_nack
        self.__max_retry_count = consumer_info.get("max_retry_count", 0)
        self.__msg_timeout = consumer_info.get("msg_timeout", 0)
        self.__min_publish_time = min_publish_timestamp

    def close(self):
        self.__consumer.close()

    def set_message_max_retry_count(self, count: int):
        """
        设置消息的最大次数

        超过最大重试次数的消息都不进行消费，直接ack
        """
        self.__max_retry_count = count

    def set_min_publish_time(self, timestamp: int):
        """
        设置可消费消息的最小发布时间；单位s；

        所有publish time小于这个时间戳的消息都不消费（直接ack）
        """
        self.__min_publish_time = timestamp

    async def receive_message(self, delegate: PulsarConsumerControllerDelegate):
        """
        开启消费消息（单条）
        消息值必须能进行json解析，比如消息是字典或者数组投递的值
        """
        while True:
            msg = self.__consumer.receive()

            try:
                if self.check_message_is_valid(msg):
                    await delegate.PulsarMessageReceive(msg)
                    if self.__is_auto_ack:
                        self.ack_message(msg)
            except Exception as err:
                logger.exception(f"pulsar receive_message err: {err}")
                if self.__is_auto_nack:
                    self.nack_message(msg)
                await delegate.PulsarMessageReceiveFailure(msg, err)

    async def receive_batch_message(self, delegate: PulsarConsumerControllerDelegate):
        """
        开启消费消息（批量/多条）
        消息值必须能进行json解析，比如消息是字典或者数组投递的值
        """
        while True:
            msgs = self.__consumer.batch_receive()
            if not msgs:
                continue
            consume_msgs = []
            try:
                for msg in msgs:
                    if self.check_message_is_valid(msg):
                        consume_msgs.append(msg)
                if consume_msgs:
                    await delegate.PulsarMessageReceive(consume_msgs)
                if self.__is_auto_ack:
                    for msg in consume_msgs:
                        self.ack_message(msg)
            except Exception as err:
                logger.exception(f"pulsar receive_batch_message err: {err}")
                if self.__is_auto_nack:
                    for msg in consume_msgs if len(consume_msgs) else msgs:
                        self.nack_message(msg)
                if consume_msgs:
                    await delegate.PulsarMessageReceiveFailure(msgs, err)

    def check_message_is_valid(self, message: Message):
        """
        检查消息是否为可消费/可用消息；
        """

        if (
            self.__max_retry_count > 0
            and message.redelivery_count() >= self.__max_retry_count
        ):
            logger.info(
                f"msg retry count: {message.redelivery_count()}, msg retry limit: {self.__max_retry_count}, ack msg"
            )
            self.ack_message(message)
            return False
        if self.__min_publish_time > 0:
            publish_time = message.publish_timestamp() / 1000
            if publish_time < self.__min_publish_time:
                logger.info(f"expired message, message publish time: {publish_time} ")
                self.ack_message(message)
                return False

        if self.__msg_timeout > 0:
            publish_time = message.publish_timestamp() / 1000
            timestamp = time.time()
            if int(timestamp) - publish_time > self.__msg_timeout:
                logger.info(
                    f"msg timeout, msg publish time: {publish_time}; timeout: {self.__msg_timeout} "
                )
                self.ack_message(message)
                return False
        return True

    def ack_message(self, message: Union[Message, MessageId]):
        """
        Parameters
        ----------
        message : Message, _pulsar.Message, _pulsar.MessageId
            The received message or message id.
        """
        self.__consumer.acknowledge(message)

    def nack_message(self, message: Union[Message, MessageId]):
        """
        Parameters
        ----------
        message : Message, _pulsar.Message, _pulsar.MessageId
            The received message or message id.
        """
        self.__consumer.negative_acknowledge(message)

    def parse_message(self, msgs: Union[Message, List[Message]]):
        """
        默认解析方式；
        - 当msgs是列表，返回解析后的列表
        - 当msgs是message， 返回字典或列表
        """
        if isinstance(msgs, List):
            list_obj = []
            for msg in msgs:
                msg_obj = ujson.loads(msg.data())
                list_obj.append(msg_obj)
            return list_obj
        else:
            return ujson.loads(msgs.data())


class PulsarProducerController:
    def __init__(self, url, token, topic, **kwargs) -> None:
        self.__producer = Client(
            url, authentication=AuthenticationToken(token), **kwargs
        ).create_producer(topic=topic)

    def close(self):
        self.__producer.close()

    def send(self, value, partition_key=""):
        """
        推送消息是单协程;阻塞进行
        返回MessageId对象
        """
        if isinstance(value, (list, dict)):
            value = ujson.dumps(value)
        value = value.encode() if isinstance(value, str) else value
        if partition_key:
            return self.__producer.send(value, partition_key=partition_key)
        else:
            return self.__producer.send(value)
