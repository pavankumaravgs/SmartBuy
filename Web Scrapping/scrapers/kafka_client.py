import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

class KafkaClient:

    def __init__(self, bootstrap_servers: str):
        logging.info(f"[KafkaClient] Initializing Kafka producer to {bootstrap_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=5,
            retries=1
        )

    def process_and_push_products(self, products, topic):
        import threading
        import time
        thread_name = threading.current_thread().name
        logging.info(f"[Thread {thread_name}] Starting to process and push {len(products)} products to topic '{topic}'")
        for idx, p in enumerate(products, 1):
            product_id = p.get("asin") or p.get("title")
            price_str = p.get("price")
            price = None
            if price_str:
                import re
                price_num = re.sub(r"[^0-9.]", "", price_str.replace(",", ""))
                try:
                    price = float(price_num)
                except Exception:
                    price = None
            timestamp = int(time.time() * 1000)
            message = {
                "ProductId": product_id,
                "Price": price if price is not None else 0.0,
                "Timestamp": timestamp
            }
            logging.debug(f"[Thread {thread_name}] Pushing product {idx}/{len(products)}: {message}")
            self.push(topic, message)
        self.flush_and_close()
        logging.info(f"[Thread {thread_name}] Finished pushing all products to topic '{topic}'")

    def push(self, topic: str, message: dict):
        try:
            logging.debug(f"[KafkaClient] Sending message to topic '{topic}': {message}")
            future = self.producer.send(topic, value=message)
            record_metadata = future.get(timeout=10)
            logging.info(
                f"[KafkaClient] Sent to Kafka topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}"
            )
        except KafkaError as e:
            logging.error(f"[KafkaClient] Error sending to Kafka: {e}")

    def flush_and_close(self):
        logging.info("[KafkaClient] Flushing Kafka producer...")
        self.producer.flush()
        self.producer.close()
        logging.info("[KafkaClient] Producer closed. Done.")
