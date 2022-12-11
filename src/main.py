from confluent_kafka import Producer
import string, random
from datetime import datetime
import asyncio

def delivery_callback(err, msg):
    pass

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

NUM_MSG = 1000000
MSG_SIZE = int(1e4)
TOPIC = get_random_string(10)
MSG = get_random_string(MSG_SIZE)
WORKERS = 10

async def produce():
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    for i in range(0, NUM_MSG // WORKERS):
        while True:
            try:
                p.produce(TOPIC, MSG, callback=delivery_callback)
            except BufferError:
                p.poll(0)
                continue
            break
    p.flush()


if __name__ == "__main__":
    print("topic ID: ",  TOPIC)
    tasks = []
    for w in range(WORKERS):
        tasks.append(asyncio.ensure_future(produce()))

    loop = asyncio.get_event_loop()

    start = datetime.now()
    loop.run_until_complete(asyncio.wait(tasks))

    end = datetime.now()-start
    print("PRODUCER: ")
    print("%d msg/sec and %d mb/sec\n" % (NUM_MSG/int(end.total_seconds()), NUM_MSG*MSG_SIZE/1024.0/1024/end.total_seconds()))
