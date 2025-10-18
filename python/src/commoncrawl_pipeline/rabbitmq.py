from abc import ABC, abstractmethod

import pika

from commoncrawl_pipeline.config import QUEUE_NAME, RABBITMQ_CONNECTION_STRING


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self.channel = rabbitmq_channel()

    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
        )


def rabbitmq_channel() -> pika.adapters.blocking_connection.BlockingChannel:
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_CONNECTION_STRING))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return channel
