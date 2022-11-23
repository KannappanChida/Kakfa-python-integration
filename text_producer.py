from confluent_kafka import Producer
from confluent_kafka import Consumer

import uuid

p = Producer({"bootstrap.servers": "DESKTOP-ONRD9NC:9092"})


def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


for data in range(1000):
    p.poll(0)
    data_str = str(data)

    p.produce("rpa_tool_reviews", data_str.encode("utf-8"), callback=delivery_report)

p.flush()
