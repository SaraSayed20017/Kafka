from confluent_kafka import Producer

me = "SaraSayed_2"
conf = {"bootstrap.servers": "34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094", 
        "client.id" : me}

producer = Producer(conf)
topic = me
producer.produce(topic, key="key", value=input('please enter a value: '))
producer.flush()
print('producer one message')