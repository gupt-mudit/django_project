from django.views.decorators.csrf import csrf_exempt
import json
from django.http import JsonResponse
from ..models.user import User
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server URL
    'client.id': 'django-producer',
}

# Initialize Kafka Producer
producer = Producer(conf)

# Kafka topic to which the message will be pushed
KAFKA_TOPIC = 'user-data'

# Kafka delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: %s' % err)
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

@csrf_exempt
def user_view(request):
    if request.method == 'GET':
        # Handle GET request
        users = list(User.objects.values())  # Retrieve all users
        return JsonResponse({'users': users}, safe=False)

    elif request.method == 'POST':
        # Handle POST request (Create a new user)
        data = json.loads(request.body)

        # Validate input data (basic example)
        if not all([data.get('name'), data.get('age'), data.get('email')]):
            return JsonResponse({'error': 'Missing required fields'}, status=400)

        # Create the user in the database
        user = User.objects.create(
            name=data['name'],
            age=data['age'],
            email=data['email']
        )

        # Send the user data to Kafka
        producer.produce(KAFKA_TOPIC, key=str(user.id), value=json.dumps({
            'id': user.id,
            'name': user.name,
            'age': user.age,
            'email': user.email,
        }), callback=delivery_report)

        # Ensure that messages are delivered
        producer.flush()

        return JsonResponse({'message': f"pushed to kafka for user with name {user.name}"}, status=201)

    elif request.method == 'PUT':
        # Handle PUT request (Update an existing user)
        data = json.loads(request.body)
        user_id = data.get('id')

        if not user_id:
            return JsonResponse({'error': 'User ID is required for update'}, status=400)

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return JsonResponse({'error': 'User not found'}, status=404)

        # Update the user's details
        user.name = data.get('name', user.name)
        user.age = data.get('age', user.age)
        user.email = data.get('email', user.email)
        user.save()

        return JsonResponse({'message': f"Updated user with name {user.name}"}, status=200)

    else:
        return JsonResponse({'error': 'Method not allowed'}, status=405)
