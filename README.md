# delay-queue-redis
A demo implementation of a delay queue architecture using Redis Streams and Sorted sets
Another demo to see how Redis Streams with Consumer Groups can be used in case there are application failovers

# Project Setup

Run the below commands

```
git clone https://github.com/mar1boroman/delay-queue-redis.git && cd delay-queue-redis
```

```
python3 -m venv venv && source venv/bin/activate
```

```
pip install -r requirements.txt
```

# Demo 1

## Delay Queue Architecture

#### All the below steps should be run on different terminals

- Step 1 : Run the producer
  - The producer will send messages to a redis stream "stream:producer" every 5 seconds. Some of the messages will have a value of corrupted_msg which cannot be handled by the consumer.
  - ```
    python delay-queue/producer.py
    ```


- Step 2 : Run the consumer
  - The consumer will consume the messages from the same stream using a consumer group
  - The consumer will not be able to process the messages with value "corrupted_msg" and will send the messages to a delay queue (Sorted set)
  - ```
    python delay-queue/consumer.py
    ```


- Step 3 : Run the delay queue processor
  - The delay processor will run every 5 seconds and query the sorted set to check which messages were recieved for correction. It will correct the value from corrupted_msg and send it back to the stream for processing
  - When the message is entered in the sorted set, it will be added with a specific delay interval, in this case 1 second, so that whenever a message is entered in the sorted set, it may only be processed after 1 second
  - ```
    python delay-queue/delay_queue_processor.py
    ```

# Demo 2

## Resilient Streams (Maintain order while replacing consumers)

#### All the below steps should be run on different terminals

- Step 1 : Run the producer
  - The producer will send messages to a redis stream "stream:producer" every 1 second.
  - ```
    python maintain-order/producer.py
    ```

- Step 2 : Run Consumer 1
  - The consumer 1 will start processing the messages
  - Here, one consumer is mapped to one consumer group and vice a versa
  - The consumer will first
    - Check if there is any other consumer active, if yes, it will not engage with the stream. This is to make sure there are no multiple consumers for a single consumer group and order is maintained while consumption
    - If there is a consumer, **but it is not active** (we decide based on the idle time, or inactive time based on your use case), the consumer will 
      - First **claim** all the messages from the inactive consumer for itself
      - Make sure that the pending entries for the inactive consumer become 0
      - Delete the old consumer
      - Start processing the messages including the pending messages, to maintain order
    - If there is no consumer, it will start processing the messages straightaway
  - ```
    python maintain-order/consumer-1.py
    ```

- Step 3 : Stop Consumer 1
  - Ctrl + C to stop the consumer process

- Step 3 : Start Consumer 2
  - ```
    python maintain-order/consumer-2.py
    ```
  - You will notice that now, before starting to consume the messages where Consumer 1 left off, Consumer 2 will first follow all the above steps and make sure the messages are processed in order
  - **If you try to start Consumer 1 again, you will get a message saying that there is a existing active consumer on the stream.**


#### How PELs work

In the file consumer-1.py

- Comment out the line `r.xack(STREAM_NAME, CONSUMER_GROUP, message_id)`
- This will make sure that consumer-1 will consume the messages but will not acknowledge them, thus building up the PEL
- Now, Follow the above steps and you will find that Consumer 2 will start **consumption from the beginning since it will claim all the PELs and process them one by one.**
  