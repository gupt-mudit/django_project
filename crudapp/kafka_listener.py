import json
import time
from confluent_kafka import Consumer
from django.db import IntegrityError

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka-python-consumer-group',
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning if no offset is found
}

# Initialize Kafka Consumer
consumer = Consumer(conf)
consumer.subscribe(['user-data'])  # Subscribe to the user-data topic


def consume_and_save():
    """
    Consume Kafka messages continuously and save them to the database.
    """
    try:
        while True:  # Infinite loop to keep consuming messages
            msg = consumer.poll(5.0)  # Poll for new messages every 5 seconds
            if msg is None:
                continue  # Continue polling if no message is available

            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # Process the message
            print(f"Received message: {msg.value().decode('utf-8')}")
            data = json.loads(msg.value().decode('utf-8'))

            # Save to the database
            check_user_below_18(data)

            # Optional: Add a small delay to avoid busy-waiting
            time.sleep(1)

    except KeyboardInterrupt:
        print("Consumer interrupted by user, shutting down...")
    except Exception as e:
        print(f"Error consuming message: {e}")
    finally:
        consumer.close()


def check_user_below_18(data):
    """
    Save parsed data to the database using the User model.
    """
    try:
         if data['age'] < 18:
             print(f"User: {data['name']} age is below 18")
         else:
             print(f"User: {data['name']} age is above 18")

    except IntegrityError as e:
        print(f"Error saving to database: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    consume_and_save()
