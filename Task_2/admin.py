from confluent_kafka import admin

conf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
}

ac=admin.AdminClient(conf)
topic="SaraSayed_2"
res=ac.create_topics([admin.NewTopic(topic,num_partitions=3,replication_factor=3)])
res[topic].result()