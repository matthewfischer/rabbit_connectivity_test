#!/usr/bin/env python

import argparse
import os
import pika
import socket

class CADFConsumer(object):
    EXCHANGE = 'test'
    EXCHANGE_TYPE = 'topic'
    QUEUE = ''

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url

    def connect(self):
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        print "Connection Open"
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.add_timeout(5, self.reconnect)
        print "Connection Closed"

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        print "Channel Open"
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self._connection.close()
        print "Channel Closed"

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)
        print "Exchange Connected or Created"

    def on_exchange_declareok(self, unused_frame):
        self.setup_queue(self.QUEUE)
        print "Exchange OK"

    def setup_queue(self, queue_name):
        self._channel.queue_declare(self.on_queue_declareok, queue_name)
        print "Queue Connected or Created"

    def on_queue_declareok(self, method_frame):
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE)
        print "Queue OK"

    def on_bindok(self, unused_frame):
        self.start_consuming()
        print "Bind OK"

    def start_consuming(self):
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.QUEUE)

    def add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        self.acknowledge_message(basic_deliver.delivery_tag)
        print "Got message: %s" % body

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.close_channel()

    def close_channel(self):
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()

    def close_connection(self):
        self._connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rabbit_user", default=os.environ.get('RABBIT_USER'))
    parser.add_argument("--rabbit_pass", default=os.environ.get('RABBIT_PASS'))
    parser.add_argument("--rabbit_host", default=os.environ.get('RABBIT_HOST'))
    parser.add_argument("--rabbit_port", default='5672')
    parser.add_argument("--rabbit_queue", default='rabbit_connectivity_test')
    args = parser.parse_args()

    # set the queue
    CADFConsumer.QUEUE = args.rabbit_queue

    consumer = CADFConsumer('amqp://%s:%s@%s:%s/' % (args.rabbit_user,
        args.rabbit_pass, args.rabbit_host, args.rabbit_port))
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


if __name__ == '__main__':
    main()
