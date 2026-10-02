import json
import pika
import os
def get_rabbitmq_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost')))
ROUTES = {'physical': 'orders.physical', 'digital': 'orders.digital', 'subscription': 'orders.subscription'}
UNROUTABLE_QUEUE = 'orders.unroutable'
def route_order(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['orderId']
    items = order.get('items')
    if not isinstance(items, list) or not items:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    n = len(items)
    conn = get_rabbitmq_connection()
    try:
        c = conn.channel()
        for q in list(ROUTES.values()) + [UNROUTABLE_QUEUE]:
            c.queue_declare(queue=q, durable=True)
        for i, item in enumerate(items):
            q = ROUTES.get(item.get('type') if isinstance(item, dict) else None, UNROUTABLE_QUEUE)
            msg = {'orderId': order_id, 'correlationId': order_id, 'itemIndex': i, 'totalItems': n, 'item': item}
            c.basic_publish(exchange='', routing_key=q, body=json.dumps(msg), properties=pika.BasicProperties(delivery_mode=2))
    finally:
        conn.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print('[Router] split', order_id, 'into', n, flush=True)
def route_order(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['orderId']
    items = order.get('items')
    if not isinstance(items, list) or not items:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    n = len(items)
    conn = get_rabbitmq_connection()
    try:
        c = conn.channel()
        for q in list(ROUTES.values()) + [UNROUTABLE_QUEUE]:
            c.queue_declare(queue=q, durable=True)
        for i, item in enumerate(items):
            q = ROUTES.get(item.get('type') if isinstance(item, dict) else None, UNROUTABLE_QUEUE)
            msg = {'orderId': order_id, 'correlationId': order_id, 'itemIndex': i, 'totalItems': n, 'item': item}
            c.basic_publish(exchange='', routing_key=q, body=json.dumps(msg), properties=pika.BasicProperties(delivery_mode=2))
    finally:
        conn.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print('[Router] split', order_id, 'into', n, flush=True)
def main():
    conn = get_rabbitmq_connection()
    ch = conn.channel()
    ch.queue_declare(queue='orders.incoming', durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue='orders.incoming', on_message_callback=route_order)
    print('[Router] Waiting for orders...', flush=True)
    ch.start_consuming()
if __name__ == '__main__':
    main()
