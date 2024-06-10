from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

two_groups=False
one_group=False

def msg_process(msg):
    print("Received a new message: ", msg.value())

def consumer_worker(conf, topics, consumer_id):
    me = "SaraSayed_2"

    if 1_group: # messages will be distributed over consumers evenly -> message will be sent to only one consumer
        conf['group.id'] = "one_group"
    elif 2_groups: # messages will be distributed over these 2 evenly, all consumers in the same group will recieve the same message
        if consumer_id==1:
            conf['group.id'] = "first_group"
        else:
            conf['group.id'] = "second_group"
    else: # different groups -> all messages will be recieved by all the consumers
        conf['group.id'] = "group_"+str(consumer_id)


    consumer = Consumer(conf)
    consumer.subscribe(topics)
    print("Consumer", me, "subscribed to topics:", topics)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

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

def main():
    conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
            'auto.offset.reset': 'smallest'}

    topics = ["SaraSayed_2"]  # Adjust the topic name here

    # Get consumer ID from command line argument
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else "1"

    consumer_worker(conf, topics, consumer_id)

if __name__ == "__main__":
    main()
