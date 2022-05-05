import json
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from retry import retry
from datetime import datetime


class KafkaOutputProducer:
    # status_topic = args.status_topic

    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

    @retry(tries=3, delay=20)
    def message_writer(self, topic, message):
        try:
            producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                     value_serializer=lambda m: json.dumps(m).encode('utf-8'))

            future = producer.send(topic, message)
            future.get(timeout=10)
            producer.flush()
        except NoBrokersAvailable as e:
            print('KAFKA ERROR :', e)
            print('Retry sending message after 20 seconds..')
        except KafkaError as e:
            print("KAFKA ERROR: Exception while sending kafka message\n", e)

        #print("DEBUG: Scraped record sent")
