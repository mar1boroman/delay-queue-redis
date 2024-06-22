import redis
import time
import random

# Connect to local redis
r = redis.Redis(decode_responses=True)
r.flushdb()
STREAM_NAME = "stream:producer"


def main():
    # Add elements to a stream every 5 seconds with random corrupted messages
    i = 0
    while True:
        if random.choice([True, False]):
            message = f"msg:{i}"
        else:
            message = f"corrupted_msg:{i}"

        r.xadd(name=STREAM_NAME, fields={"msg": message})
        time.sleep(5)
        i += 1


if __name__ == "__main__":
    main()
