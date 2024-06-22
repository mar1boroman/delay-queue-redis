import redis
import time
from datetime import UTC
from datetime import datetime 
from datetime import timedelta
import json

# Connect to local redis

r = redis.Redis(decode_responses=True)
STREAM_NAME = "stream:producer"
CONSUMER_GROUP = 'myconsumergroup'
DELAY_QUEUE = "queue:delay"
RETRY_DELAY = 1

def add_to_delay_queue(message):
    retry_time = int((datetime.now(UTC) + timedelta(seconds=RETRY_DELAY)).timestamp())
    r.zadd(DELAY_QUEUE, {json.dumps(message['msg']): retry_time})
    print(f"Added to delay queue for correction: {message}")

def process_message(message):
    if 'corrupted_msg' in message['msg']:
        add_to_delay_queue(message)
    else:
        print(f"Message processed correctly through stream : {message}")

def main():
    if CONSUMER_GROUP not in [x['name'] for x in r.xinfo_groups(STREAM_NAME)]:
        r.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id='0', mkstream=True)
    while True:
        msg = r.xreadgroup(
            groupname="myconsumergroup",
            consumername="consumer1",
            streams={STREAM_NAME : '>'},
            count=1,
            block=1000
        )
        if msg:
            stream, messages = msg[0]
            for message_id, message in messages:
                process_message(message)
        


if __name__ == "__main__":
    main()
