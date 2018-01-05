import pika
import json
import pymongo


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='messages')
conn = pymongo.MongoClient('localhost', 27017)

try:
    def callback(ch, method, properties, body):
        test = json.loads(body)
        print(' [*] Received data from queue : {obj}'.format(obj=str(test)))
        routing_key = test['routing_key']
        dict_str = json.loads(test['data_sample'])
        conn[routing_key.split('.')[0]][routing_key.split('.')[1]].insert_one(
            dict_str
        )
        print(' [*] Queue data saved to provided MongoDB document.')

    channel.basic_consume(callback,
                          queue='messages',
                          no_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except KeyboardInterrupt:
    print(' [*] Exited. Will purge the message queue.')
    channel.queue_purge()
