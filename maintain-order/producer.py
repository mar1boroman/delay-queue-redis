import redis
import time
import random

# Connect to local redis
r = redis.Redis(decode_responses=True)
r.flushdb()
STREAM_NAME = "stream:producer"


def main():
    # Add elements to a stream every 5 seconds 
    i = 0
    while True:
        message = f"msg:{i}"
        print(f"Added {message} to the stream")
        r.xadd(name=STREAM_NAME, fields={"msg": message})
        time.sleep(1)
        i += 1


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"Producer {STREAM_NAME} stopped by the user")
