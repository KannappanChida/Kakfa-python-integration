from confluent_kafka import Producer
from confluent_kafka import Consumer

import uuid

p = Producer({"bootstrap.servers": "DESKTOP-ONRD9NC:9092"})


def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


rpa_tools = dict()
rpa_tools["OpenRPA"] = "3 out of 5"
rpa_tools["Taiko"] = "4 out of 5"
rpa_tools["Botcity"] = "3 out of 5"
rpa_tools["Robot Framework"] = "5 out of 5"

for data in rpa_tools:
    p.poll(0)
    data_str = str(data)

    p.produce("rpa_tool_reviews", data_str.encode("utf-8"), callback=delivery_report)

p.flush()


c = Consumer(
    {
        "bootstrap.servers": "DESKTOP-ONRD9NC:9092",
        "group.id": "mygroup",
        "auto.offset.reset": "earliest",
    }
)

c.subscribe(["rpa_tool_reviews"])

while True:
    msg = c.poll(4.0)

    unique_id = uuid.uuid4()
    if msg is None:
        print(f"timeout poll txn id: {unique_id} ")
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print("Received message: {}".format(msg.value().decode("utf-8")))

c.close()
