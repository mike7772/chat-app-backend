from flask import Flask, request
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_cors import CORS
import boto3
from boto3.dynamodb.conditions import Attr
import time
import os
import uuid
from dotenv import load_dotenv
import decimal

# Helper function to convert Decimal to standard Python types
def decimal_to_standard(data):
    if isinstance(data, list):
        return [decimal_to_standard(item) for item in data]
    elif isinstance(data, dict):
        return {key: decimal_to_standard(value) for key, value in data.items()}
    elif isinstance(data, decimal.Decimal):
        return int(data)  # Convert Decimal to int or float
    else:
        return data

# Load environment variables from .env file
load_dotenv()

# Get environment variables
aws_access_key_id = os.getenv("ACCESS_KEY")
aws_secret_access_key = os.getenv("SECRET_KEY")
region_name = os.getenv("REGION")

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize DynamoDB client
dynamodb = boto3.resource(
    'dynamodb',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
table = dynamodb.Table('Test')


@app.route('/')
def index():
    return "Flask-SocketIO Backend"


@socketio.on('connect')
def handle_connect():
    print('Client connected')


@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


@socketio.on('join_room')
def handle_join_room(data):
    room = data.get('room')
    name = data.get('name')

    if room and name:
        join_room(room)
        print(f'Client {name} joined room: {room}')
        emit('receive_message', {
            'message': f'{name} has joined the room', 'sender_id': 'server'}, room=room)

        try:
            response = table.scan(
                FilterExpression=Attr('room').eq(room)  # Filter by 'room' attribute
            )

            # & Attr("name").eq(name)
            # Manually sort the results by 'timestamp'
            items = sorted(response['Items'], key=lambda x: x['timestamp'], reverse=False)  # Change to True for descending order

            response_items = decimal_to_standard(items)


            # Loop through and emit all the messages in the room
            for item in response_items:
                emit('receive_message', {
                    'message': item['message'],
                    'sender_id': item['sender_id'],
                    'name': item.get('name', 'unknown'),
                    'id': item['id'],
                    'timestamp': item['timestamp']
                }, room=room)
        except Exception as e:
            print(f"Error querying DynamoDB: {str(e)}")
    else:
        print('Invalid join_room data:', data)



@socketio.on('send_message')
def handle_send_message(data):
    room = data.get('room')
    message = data.get('message')
    name = data.get('name')
    if room and message and name:
        sender_id = request.sid
        timestamp = int(time.time())
        # Generate a unique ID for the message
        message_id = str(uuid.uuid4())

        # Store message in DynamoDB
        try:
            table.put_item(
                Item={
                    'room': room,  
                    'timestamp': timestamp,  # Sort key
                    'id': message_id, # Partition key 
                    'message': message,
                    'sender_id': sender_id,
                    'name': name
                }
            )
            # Emit message to the room
            emit('receive_message', {
                'message': message, 'sender_id': sender_id, 'name': name, 'id': message_id}, room=room)
            print(
                f"Message sent to room {room} by {name}: {message} (ID: {message_id})")
        except Exception as e:
            print(f"Error storing message in DynamoDB: {str(e)}")
    else:
        print('Invalid send_message data:', data)


if __name__ == '__main__':
    socketio.run(app, debug=True, port=5000)
