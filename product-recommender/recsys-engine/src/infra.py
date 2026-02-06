import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from src.logger_config import setup_logger
from src.models import FeedbackEvent

setup_logger()
logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap: str, registry_url: str, topic_name: str):
        self.bootstrap = bootstrap
        self.registry_url = registry_url
        self.topic_name = topic_name

        # Initialize Schema Registry Client
        self.schema_registry_conf = {
            "url": self.registry_url,
            "basic.auth.user.info": "admin:admin",
        }
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        # Initialize Avro Serializer
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            FeedbackEvent.schema(),
            lambda obj, ctx: obj.to_dict(),
        )

        # Initialize Serializing Producer
        producer_conf = {
            "bootstrap.servers": self.bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self.avro_serializer,
        }
        self.producer = SerializingProducer(producer_conf)

    def produce(self, key, value_dict):
        """
        Sends a message to the broker.
        value_dict: Dictionary containing event data.
        """
        # Convert dict to FeedbackEvent object for the serializer
        event_obj = FeedbackEvent.from_dict(value_dict)

        def on_delivery(err, msg):
            if err is not None:
                logger.error(f"Delivery failed for record {msg.key()}: {err}")

        try:
            self.producer.produce(
                topic=self.topic_name, key=key, value=event_obj, on_delivery=on_delivery
            )
            self.producer.poll(0)
        except ValueError as e:
            logger.error(f"Invalid input: {e}")
        except BufferError:
            logger.error(
                f"Local producer queue is full ({len(self.producer)} messages awaiting delivery)"
            )

    def flush(self):
        self.producer.flush()
