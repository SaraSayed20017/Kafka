from confluent_kafka import Consumer
import random
import requests

me = 'SaraSayed_4'
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# def msg_process(msg):
#     print(f"Recieved a New Messages: {msg.value()}")


running = True

def detect_object(id):
    return random.choice(['car', 'house', 'person'])

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        print("Subscribe To Topics", topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # msg_process(msg)
                print(detect_object(msg.value()))
                requests.put('http://127.0.0.1:5000/object/' + msg.value().decode(), json={"object": detect_object(msg.value().decode())})
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, [me])