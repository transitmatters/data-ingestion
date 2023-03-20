from datetime import datetime, timedelta
import numpy as np
import boto3
from chalicelib import update_agg_tables
client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')  # Should I not create a client and a "regular" dynamo object in the same file?


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"


def write_to_traversal_table(tt_objects, line, table_name):
    batch_items = []
    if len(tt_objects) == 0:
        return

    for date, metrics in tt_objects:
        item = {'date': {}, 'value': {}, 'count': {}, 'line': {}}
        item['date']['S'] = date
        item['value']['N'] =str(metrics['median'])
        item['count']['N'] = str(metrics['count'])
        item['line']['S'] = line
        batch_items.append({'PutRequest': {'Item': item}})

    client.batch_write_item(RequestItems={table_name: batch_items})


def query_line_travel_times(params):
    # Create a DynamoDB resource
    dynamodb = boto3.resource('dynamodb')

    # Get a reference to the LineTraversalTime table
    table = dynamodb.Table('LineTraversalTime')

    # Define the query parameters
    query_params = {
        'KeyConditionExpression': '#pk = :pk and #date BETWEEN :start_date and :end_date',
        'ExpressionAttributeNames': {
            '#pk': 'line',
            '#date': 'date'
        },
        'ExpressionAttributeValues': {
            ':pk': params["line"],
            ':start_date': params["start_date"],
            ':end_date': params["end_date"]
        }
    }

    # Execute the query and return the results
    response = table.query(**query_params)
    return response['Items']



def update_speed_adherence(line, now, value):
    table = dynamodb.Table("OverviewStats")
    table.update_item(
        Key={"line": line, "stat": "SpeedAdherence"},
        UpdateExpression='SET #last_updated = :last_updated, #value = :value',
        ExpressionAttributeNames={
            '#last_updated': 'last_updated',
            '#value': 'value',
        },
        ExpressionAttributeValues={
            ':last_updated': f'{now.strftime("%Y-%m-%dT%H:%M:%S")}',
            ':value': value,
        },
    )
