import atexit
import logging
import os
from datetime import datetime
from threading import Thread

import pika

import comparator
import recon
import store
from th2 import infra_pb2

RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT'))
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY = os.getenv('RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY')
CACHE_SIZE = int(os.getenv('CACHE_SIZE'))
RECON_TIMEOUT = int(os.getenv('RECON_TIMEOUT'))
ROUTING_KEYS = [key.replace('{', '').replace('}', '').replace('"', '').replace(' ', '') for key in
                os.getenv('ROUTING_KEYS').split(',')]
TIME_INTERVAL = int(os.getenv('TIME_INTERVAL'))
EVENT_STORAGE_URI = os.getenv('EVENT_STORAGE')
COMPARATOR_URI = os.getenv('COMPARATOR_URI')
EVENT_BATCH_SIZE = os.getenv('EVENT_BATCH_SIZE')

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
params = pika.ConnectionParameters(virtual_host=RABBITMQ_VHOST, host=RABBITMQ_HOST, port=RABBITMQ_PORT,
                                   credentials=credentials)

connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange=RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY, exchange_type='direct')

queue_listeners = {
    routing_key: recon.QueueListener(routing_key, CACHE_SIZE, channel, RECON_TIMEOUT, ROUTING_KEYS.index(routing_key))
    for routing_key in ROUTING_KEYS}

for queue_listener in queue_listeners.values():
    channel.queue_bind(exchange=RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY,
                       queue=queue_listener.queue_name,
                       routing_key=queue_listener.routing_key)


def callback(ch, method, properties, body):
    message_batch = infra_pb2.MessageBatch()
    message_batch.ParseFromString(body)
    for message in message_batch.messages:
        queue_listeners[method.routing_key].blocking_queue.put(item=message, block=True)
        logging.info("Received message from %r:%r %r" % (
            method.routing_key, message.metadata.message_type, message.metadata.timestamp.seconds))


for queue_listener in queue_listeners.values():
    consumer_tag = channel.basic_consume(queue_listener.queue_name,
                                         callback,
                                         auto_ack=True)


def shutdown_hook():
    thread_recon.join()
    message_comparator.stop()
    print("Recon is stopped")


atexit.register(shutdown_hook)

event_store = store.Store(EVENT_STORAGE_URI)
event_store.send_report(event_store.report_id, 'Recon_' + str(datetime.now()))

message_comparator = comparator.Comparator(COMPARATOR_URI, event_store)
message_comparator.start()

thread_recon = Thread(target=recon.recon,
                      args=(queue_listeners, CACHE_SIZE, TIME_INTERVAL, event_store, message_comparator))
thread_recon.start()

try:
    logging.info("Waiting for messages")
    channel.start_consuming()
finally:
    channel.stop_consuming()
    shutdown_hook()
