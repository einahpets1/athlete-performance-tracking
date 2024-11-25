import boto3
import aiohttp
import asyncio
import json
from datetime import datetime
from math import sqrt

# DynamoDB setup
dynamodb = boto3.resource('dynamodb', region_name='your-region')
dynamodb_client = boto3.client('dynamodb')
player_mapping_table_name = 'playerMapping'
player_data_table = dynamodb.Table('playerDataForAll')

# Bubble API setup
bubble_api_url = "https://caritas.cobotique.ai/version-test/api/1.1/wf/updateSensorRealTimePositionAndStatus"
bubble_api_key = "a1a43126092af19653797a484b333d78"  # Replace with your API key

# Initialize variables for storing previous sensor data
last_position_x = {}
last_position_y = {}
last_record_datetime = {}

# Function to calculate vector acceleration
def calculate_vector_acceleration(ax, ay, az):
    return sqrt(ax**2 + ay**2 + az**2)

# Function to calculate distance and speed
def calculate_distance_and_speed(x1, y1, x2, y2, time_diff):
    distance = sqrt((x2 - x1)**2 + (y2 - y1)**2)
    speed = distance / time_diff if time_diff > 0 else 0
    return distance, speed

# Batch get player mappings from DynamoDB
async def get_player_mappings(sensor_ids):
    keys = [{"sensorId": {"S": sensor_id}} for sensor_id in sensor_ids]
    response = dynamodb_client.batch_get_item(
        RequestItems={player_mapping_table_name: {"Keys": keys}}
    )
    return {
        item['sensorId']['S']: {
            'playerId': item.get('playerId', {}).get('S', 'Unknown'),
            'playerName': item.get('playerName', {}).get('S', 'Unknown'),
            'teamName': item.get('teamName', {}).get('S', 'Unknown')
        }
        for item in response['Responses'][player_mapping_table_name]
    }

# Send data to Bubble API asynchronously
async def send_to_bubble_api(batch_update_data):
    headers = {"Authorization": f"Bearer {bubble_api_key}"}
    async with aiohttp.ClientSession() as session:
        async with session.post(bubble_api_url, json=batch_update_data, headers=headers) as response:
            if response.status == 200:
                print("Bubble API update successful!")
            else:
                print(f"Bubble API update failed: {await response.text()}")

# Process incoming JSON data
async def process_sensor_data(json_data):
    session_id = json_data['sessionId']
    record_datetime = json_data['recordDateTime']
    customer_id = json_data['customerId']
    data_stream_type = json_data['dataStreamType']
    sensors = json_data['sensors']

    if data_stream_type == "testing":
        return  # Skip processing for testing data streams

    sensor_ids = [sensor['sensorId'] for sensor in sensors]
    player_mappings = await get_player_mappings(sensor_ids)

    batch_update_data = []  # To store data for Bubble API
    batch_write_data = []  # To store data for DynamoDB

    for sensor in sensors:
        sensor_id = sensor['sensorId']
        sensor_status = sensor['sensorStatus']
        position_x = sensor['positionX']
        position_y = sensor['positionY']
        acceleration_x = sensor['accelerationX']
        acceleration_y = sensor['accelerationY']
        acceleration_z = sensor['accelerationZ']

        # Retrieve player details
        player_data = player_mappings.get(sensor_id, {})
        player_id = player_data.get('playerId', 'Unknown')
        player_name = player_data.get('playerName', 'Unknown')
        team_name = player_data.get('teamName', 'Unknown')

        # Calculate metrics
        vector_acceleration = calculate_vector_acceleration(acceleration_x, acceleration_y, acceleration_z)
        previous_time = last_record_datetime.get(sensor_id, 0)
        previous_x = last_position_x.get(sensor_id, 0)
        previous_y = last_position_y.get(sensor_id, 0)

        if previous_time != 0:
            time_diff = (datetime.fromisoformat(record_datetime) - datetime.fromisoformat(previous_time)).total_seconds()
            distance, speed = calculate_distance_and_speed(previous_x, previous_y, position_x, position_y, time_diff)
        else:
            distance, speed = 0, 0

        # Update previous values
        last_position_x[sensor_id] = position_x
        last_position_y[sensor_id] = position_y
        last_record_datetime[sensor_id] = record_datetime

        # Add data for DynamoDB
        batch_write_data.append({
            'customerId-sessionId-recordDateTime': f"{customer_id}-{session_id}-{record_datetime}",
            'sensorId': sensor_id,
            'sensorStatus': sensor_status,
            'playerId': player_id,
            'playerName': player_name,
            'teamName': team_name,
            'positionX': position_x,
            'positionY': position_y,
            'accelerationX': acceleration_x,
            'accelerationY': acceleration_y,
            'accelerationZ': acceleration_z,
            'vectorAcceleration': vector_acceleration,
            'recordDateTime': record_datetime,
            'distance': distance,
            'speed': speed
        })

        # Add data for Bubble API
        batch_update_data.append({
            "playerId": player_id,
            "positionX": position_x,
            "positionY": position_y,
            "sensorStatus": sensor_status
        })

    # Write to DynamoDB in batch
    with player_data_table.batch_writer() as batch:
        for item in batch_write_data:
            batch.put_item(Item=item)

    # Send data to Bubble API
    await send_to_bubble_api(batch_update_data)

# Main execution
if __name__ == "__main__":
    # Example JSON input
    incoming_json = '''
    {
        "sessionId": "10001",
        "recordDateTime": "2024-11-16T12:00:05Z",
        "customerId": "123",
        "dataStreamType": "live",
        "sensors": [
            {
                "sensorId": "sensor1",
                "sensorStatus": "Normal",
                "positionX": 0.6,
                "positionY": 0.6,
                "accelerationX": 1.2,
                "accelerationY": -0.8,
                "accelerationZ": 0.4
            },
            {
                "sensorId": "sensor2",
                "sensorStatus": "Low Battery",
                "positionX": 15.2,
                "positionY": 25.7,
                "accelerationX": 1,
                "accelerationY": -0.9,
                "accelerationZ": 0.3
            }
        ]
    }
    '''
    sensor_data = json.loads(incoming_json)
    asyncio.run(process_sensor_data(sensor_data))
