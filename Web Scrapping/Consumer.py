from kafka import KafkaConsumer, TopicPartition
import json

consumer = KafkaConsumer(
    bootstrap_servers=['192.168.0.102:29092'],
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)
tp = TopicPartition('amazon', 0)  # adjust if multiple partitions
consumer.assign([tp])
consumer.seek_to_beginning(tp)
print(next(consumer).value)
consumer.close()
