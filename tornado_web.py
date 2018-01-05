import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.websocket
import pika
import json


class SentMessageHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        json_data = tornado.escape.json_decode(self.request.body)
        print(' [*] Incoming data : {obj}'.format(obj=str(json_data)))
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = conn.channel()
        channel.queue_declare(queue='messages')
        channel.basic_publish(exchange='',
                              routing_key='messages',
                              body=json.dumps(json_data))
        conn.close()
        print(' [*] Data sent to message queue.')
        self.finish()
    get = post


try:
    application = tornado.web.Application([
        (r'/send', SentMessageHandler),
    ]
    )

    application.listen(8888)
    print(' [*] Listening requests. To stop press CTRL+C')
    tornado.ioloop.IOLoop.instance().start()
except KeyboardInterrupt:
    print(' [*] Stopping server.')
