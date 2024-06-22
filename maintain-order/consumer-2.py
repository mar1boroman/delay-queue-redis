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
CONSUMER_NAME = "consumer-2"
IDLE_TIME = 10000


def process_message(message):
    print(f"Message processed by {CONSUMER_NAME} : {message}")


def override_existing_consumers(STREAM_NAME, CONSUMER_GROUP):
    old_consumer = r.xinfo_consumers(STREAM_NAME, CONSUMER_GROUP)
    if old_consumer:
        old_consumer = old_consumer[0]
        print(f"Consumers active : {old_consumer}")

        if old_consumer["idle"] > IDLE_TIME:
            print(
                f"Attempting Autoclaim since consumer {old_consumer['name']} is idle for {old_consumer['idle']} ms..."
            )
            r.xautoclaim(
                name=STREAM_NAME,
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                min_idle_time=IDLE_TIME,
            )

            while True:
                pel = old_consumer["pending"]
                print(f"Pending entries : {pel}")
                if pel == 0:
                    print(f"Deleting {old_consumer} - No PEL and and idle > 10 seconds")
                    r.xgroup_delconsumer(
                        STREAM_NAME, CONSUMER_GROUP, old_consumer["name"]
                    )
                    break
                else:
                    time.sleep(1)
                    old_consumer = [
                        c
                        for c in r.xinfo_consumers(STREAM_NAME, CONSUMER_GROUP)
                        if c["name"] == old_consumer["name"]
                    ][0]
            print(
                f"Active consumers : {r.xinfo_consumers(STREAM_NAME, CONSUMER_GROUP)}"
            )
        else:
            return False

    return True


def main():

    if CONSUMER_GROUP not in [x["name"] for x in r.xinfo_groups(STREAM_NAME)]:
        r.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)

    if override_existing_consumers(STREAM_NAME, CONSUMER_GROUP):
        while True:
            msg = r.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=1,
                block=1000,
            )
            if msg:
                stream, messages = msg[0]
                for message_id, message in messages:
                    process_message(message)
    else:
        print(
            f"There are existing consumers on the consumer group {CONSUMER_GROUP} for the stream {STREAM_NAME}"
        )
        print(f"Unable to add this consumer {CONSUMER_NAME} for consumption")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"Consumer {CONSUMER_NAME} stopped by the user")
