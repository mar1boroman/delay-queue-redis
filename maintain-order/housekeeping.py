import redis
import time
from datetime import UTC
from datetime import datetime
from datetime import timedelta
import json

# Connect to local redis

r = redis.Redis(decode_responses=True)
STREAM_NAME = "stream:producer"
CONSUMER_GROUP = "myconsumergroup"

def housekeeping(STREAM_NAME, CONSUMER_GROUP):
    while True:
        consumers = r.xinfo_consumers(STREAM_NAME, CONSUMER_GROUP)
        safe_to_delete = [c for c in consumers if c['pending'] == 0 and c['idle'] > 10000]
        for c in safe_to_delete:
            print(f'Deleting {c} - No PEL and and idle > 10 seconds')
            r.xgroup_delconsumer(STREAM_NAME, CONSUMER_GROUP, c['name']) 

def main():
    housekeeping(STREAM_NAME=STREAM_NAME, CONSUMER_GROUP=CONSUMER_GROUP)


if __name__ == "__main__":
    main()
