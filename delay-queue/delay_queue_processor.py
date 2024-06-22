import redis
import time
from datetime import UTC
from datetime import datetime 
from datetime import timedelta
import json

# Connect to local redis

r = redis.Redis(decode_responses=True)
STREAM_NAME = "stream:producer"
DELAY_QUEUE = "queue:delay"
RETRY_DELAY = 1

def main():
    # Run delay queue processor every 5 seconds, correct the messages and reinsert into stream
    while True:
        print("Running Delay queue processor...")
        now = int(datetime.now(UTC).timestamp())
        messages = r.zrangebyscore(DELAY_QUEUE, 0, now)
        for msg in messages:
            message = json.loads(msg)
            corrected_message = f"corrected_msg:{message.split(':')[1]}"
            r.xadd(name=STREAM_NAME, fields={'msg' : corrected_message})
            # Remove from delay queue
            r.zrem(DELAY_QUEUE, msg)
            
        time.sleep(15)


if __name__ == "__main__":
    main()
