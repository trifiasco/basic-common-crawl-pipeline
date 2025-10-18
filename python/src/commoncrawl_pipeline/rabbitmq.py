import logging
import time
from abc import ABC, abstractmethod

import pika
from pika.exceptions import AMQPConnectionError

from commoncrawl_pipeline.config import (
    QUEUE_NAME,
    RABBITMQ_CONNECTION_STRING,
    RABBITMQ_MAX_RETRIES,
    RABBITMQ_RETRY_DELAY,
)

logger = logging.getLogger(__name__)


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: str,
        properties: pika.BasicProperties | None = None,
    ) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self.channel = rabbitmq_channel()

    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: str,
        properties: pika.BasicProperties | None = None,
    ) -> None:
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
        )


def rabbitmq_channel(
    max_retries: int = RABBITMQ_MAX_RETRIES,
    retry_delay: float = RABBITMQ_RETRY_DELAY,
) -> pika.adapters.blocking_connection.BlockingChannel:
    """Create RabbitMQ channel with retry logic.

    Args:
        max_retries: Maximum number of connection attempts
        retry_delay: Base delay between retries (exponential backoff)

    Returns:
        BlockingChannel instance

    Raises:
        AMQPConnectionError: If connection fails after all retries
    """
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(RABBITMQ_CONNECTION_STRING)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            logger.info("Connected to RabbitMQ")
            return channel

        except AMQPConnectionError as e:
            if attempt < max_retries - 1:
                delay = retry_delay * (2**attempt)  # Exponential backoff
                logger.warning(
                    "RabbitMQ connection failed, retrying...",
                    extra={
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "retry_delay": delay,
                    },
                    exc_info=True,
                )
                time.sleep(delay)
            else:
                raise AMQPConnectionError(
                    f"Failed to connect to RabbitMQ after {max_retries} attempts"
                ) from e

    # This should never be reached, but just in case
    raise AMQPConnectionError(
        f"Failed to connect to RabbitMQ after {max_retries} attempts"
    )
