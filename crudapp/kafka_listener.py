from confluent_kafka import Consumer
from django.db import IntegrityError
from .models.user import User
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'django-consumer-group',
    'auto.offset.reset': 'earliest',
}

# Initialize Kafka Consumer
consumer = Consumer(conf)
consumer.subscribe(['user-data'])


def consume_and_save():
    """
    Consume a single Kafka message and save it to the database.
    """
    try:
        # Poll for a message
        msg = consumer.poll(1.0)

        if msg is None:
            print("No new messages found.")
            return

        else:
            # Process the message
            print(f"Received message: {msg.value().decode('utf-8')}")
            data = json.loads(msg.value().decode('utf-8'))

            # Save to database
            save_to_db(data)

    except Exception as e:
        print(f"Error consuming message: {e}")
    finally:
        consumer.close()


def save_to_db(data):
    """
    Save parsed data to the database using the User model.
    """
    try:
        User.objects.create(
            name=data['name'],
            age=data['age'],
            email=data['email']
        )
        print("Data saved successfully!")
    except IntegrityError as e:
        print(f"Error saving to database: {e}")


if __name__ == '__main__':
    consume_and_save()