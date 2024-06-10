from confluent_kafka import Consumer
from confluent_kafka import Producer, KafkaError,KafkaException
import sys


me = "SaraSayed"

conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': me,
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

def msg_process(msg):
    print("recieved a new message: ", msg.value())

running = True

def basic_consumer_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        print("subscribe to topics: ", topics)

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
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets
        consumer.close()

def shutdown():
    running = False

basic_consumer_loop(consumer, [me])