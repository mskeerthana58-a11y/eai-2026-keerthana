import json
import pika
import os
import threading
import time

in_flight = {}     # orderId -> {"total": int, "results": {idx: result}, "last": float}
finished = set()   # orderIds already published (guards "exactly one completion")
lock = threading.Lock()

IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))
SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()


def build_message(order_id, order, status):
    results = order["results"]
    return {
        "orderId": order_id,
        "correlationId": order_id,
        "status": status,
        "totalItems": order["total"],
        "receivedItems": len(results),
        "itemResults": [results[i] for i in sorted(results)],
        "missingItemIndexes": sorted(set(range(order["total"])) - set(results)),
    }


def aggregate_result(ch, method, properties, body):
    try:
        result = json.loads(body)
        order_id = result["orderId"]
        idx = int(result["itemIndex"])
        total = int(result["totalItems"])
        if not (0 <= idx < total):
            raise ValueError("itemIndex out of range")
    except (ValueError, KeyError, TypeError) as e:
        print(f"[Aggregator] Dropping bad message: {e!r}", flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    to_publish = None
    with lock:
        if order_id in finished:
            print(f"[Aggregator] Late/duplicate result for finished order {order_id}", flush=True)
        else:
            order = in_flight.setdefault(
                order_id, {"total": total, "results": {}, "last": time.monotonic()}
            )
            if idx not in order["results"]:
                order["last"] = time.monotonic()
            order["results"][idx] = result
            if len(order["results"]) == order["total"]:
                to_publish = build_message(order_id, order, "complete")
                del in_flight[order_id]
                finished.add(order_id)

    if to_publish:
        publish_completion(to_publish)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)
        try:
            now = time.monotonic()
            expired = []
            with lock:
                for oid, order in list(in_flight.items()):
                    if now - order["last"] > IDLE_TIMEOUT_SECONDS:
                        expired.append(build_message(oid, order, "partial"))
                        del in_flight[oid]
                        finished.add(oid)
            for msg in expired:
                publish_completion(msg)
        except Exception as e:
            print(f"[Aggregator] Sweep error: {e!r}", flush=True)


def main():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_qos(prefetch_count=1)
    threading.Thread(target=sweep_timeouts, daemon=True).start()
    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)
    print('[Aggregator] Waiting for results...', flush=True)
    channel.start_consuming()


if __name__ == '__main__':
    main()
