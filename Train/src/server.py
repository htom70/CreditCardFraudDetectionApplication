from flask import Flask, jsonify
import pika
import json

server = Flask(__name__)


def send_message(response):
    connection = pika.BlockingConnection(
        # pika.ConnectionParameters(host='localhost', port=5672)
        pika.ConnectionParameters(host='rabbitmq', port=5672))
    # pika.ConnectionParameters(host='rabbitmq', port=5672))
    channel = connection.channel()
    channel.queue_declare(queue='message.queue', durable=True)
    channel.basic_publish(exchange='', routing_key='message.queue', body=json.dumps(response))
    print(" [x] Sent 'Hello World!'")
    connection.close()


@server.route('/message')
def hello_world():
    d = dict()
    d["from"] = "Tom"
    d["entry"] = "Hello World"
    response = jsonify(d)
    send_message(d)
    return response


if __name__ == "__main__":
    server.run(host='0.0.0.0', port=5000, debug=True)
