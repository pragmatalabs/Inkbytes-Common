import json
import time

import pika
from collections import deque


class RabbitMQHandler:

    def save_local_queue_to_disk(self):
        with open('local_queue.json', 'w') as f:
            # Convert deque to list before serialization
            json.dump(list(self.local_message_queue), f)

    def load_local_queue_from_disk(self):
        try:
            with open('local_queue.json', 'r') as f:
                # Load and convert list back to deque
                self.local_message_queue = deque(json.load(f))
        except (FileNotFoundError, json.JSONDecodeError):
            # If file doesn't exist or is corrupted, start with an empty queue
            self.local_message_queue = deque()

    def __init__(self, host='kloudsix.io', port=5672, exchange_name='inkbytes@', queue_name='messor'):
        # self.local_message_queue = deque()  # Local queue for storing messages when RabbitMQ is down
        self.load_local_queue_from_disk()
        self.queue_name = queue_name
        self.connected = False
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host, port, '/', credentials))
            self.channel = self.connection.channel()
            self.declare_queue(self.queue_name, durable=True)
            self.declare_exchange(exchange_name)
            self.channel.queue_bind(exchange=exchange_name,
                                    queue=self.queue_name)
            self.connected = True
        except pika.exceptions.AMQPConnectionError:
            print("Could not connect to RabbitMQ. Messages will be queued locally.")
            self.connected = False

    def declare_exchange(self, exchange_name, exchange_type='fanout',durable=True):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type,durable=durable)

    def declare_queue(self, queue_name, passive=False, durable=True, exclusive=False, auto_delete=False):

        self.channel.queue_declare(queue=queue_name, passive=passive, exclusive=exclusive,
                                                durable=durable, auto_delete=auto_delete)

    def publish_message_to_exchange(self, exchange, routing_key, message, queue_name=None):
        if self.connected:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make the message persistent
                )
            )
            # Flush the local queue when a message is successfully sent
            self.flush_local_queue()
        else:
            # Queue the message locally if not connected to RabbitMQ
            self.local_message_queue.append((exchange, routing_key, message, queue_name))
            self.save_local_queue_to_disk()  # Save the updated queue to disk

    def publish_message_to_queue(self, exchange, routing_key, message, queue_name=None):
        if self.connected:
            if queue_name:
                self.declare_queue(queue_name)
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make the message persistent
                )
            )
            # Flush the local queue when a message is successfully sent
            self.flush_local_queue()
        else:
            # Queue the message locally if not connected to RabbitMQ
            self.local_message_queue.append((exchange, routing_key, message, queue_name))
            self.save_local_queue_to_disk()  # Save the updated queue to disk

    def consume_messages(self, queue_name, callback):
        while True:
            try:
                if self.connected:
                    #self.declare_queue(queue_name)

                    def on_message(ch, method, properties, body):
                        callback(body)
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                    self.channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)
                    print(' [*] Waiting for messages. To exit press CTRL+C')
                    self.channel.start_consuming()
                else:
                    print("Not connected to RabbitMQ. Trying to reconnect...")
                    self.try_reconnect()
            except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPConnectionError):
                print("Connection to RabbitMQ lost. Trying to reconnect...")
                self.connected = False
                self.try_reconnect()
            except KeyboardInterrupt:
                print("Program interrupted by user. Exiting gracefully.")
                break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break

            # Wait a bit before trying to reconnect or consume messages again
            time.sleep(30)  # Adjust the sleep duration as needed

    def flush_local_queue(self):
        """Flushes the local message queue to RabbitMQ."""
        while self.local_message_queue and self.connected:
            exchange, routing_key, message, queue_name = self.local_message_queue.popleft()
            self.publish_message_to_exchange(exchange, routing_key, message, queue_name)
        self.save_local_queue_to_disk()  # Save the updated queue to disk

    def close_connection(self):
        if self.connected:
            self.connection.close()

    def try_reconnect(self):
        """Try to reconnect to RabbitMQ if not connected."""
        print("\nTrying to reconnect to RabbitMQ")
        if not self.connected:
            print("\nNot connected to RabbitMQ")
            try:
                # Attempt to re-establish the connection
                credentials = pika.PlainCredentials('guest', 'guest')
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters('localhost', 5672, '/', credentials))
                self.channel = self.connection.channel()

                # If the connection is established successfully, set connected flag
                self.connected = True
                print("Reconnected to RabbitMQ.")

                # Flush any messages that were queued during the disconnection
                self.flush_local_queue()
            except pika.exceptions.AMQPConnectionError:
                # If the reconnection attempt fails, you can print an error or handle it accordingly
                print("Failed to reconnect to RabbitMQ. Will try again later.")
        else:
            print("Reconnected to RabbitMQ.")
