import boto3
client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')  # Should I not create a client and a "regular" dynamo object in the same file?


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"


def write_to_traversal_table(tt_objects, table_name):
    table = dynamodb.Table(table_name)
    if len(tt_objects) == 0:
        return
    with table.batch_writer() as batch:
        for item in tt_objects:
            batch.put_item(Item=item)


def query_line_travel_times(params):
    table = dynamodb.Table('DailySpeed')

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

    response = table.query(**query_params)
    return response['Items']



def update_speed_adherence(line, now, value):
    table = dynamodb.Table("DailyScheduledSpeed")
    table.update_item(
        Key={"line": line, "date": now},
        UpdateExpression='SET #value = :value',
        ExpressionAttributeNames={
            '#value': 'value',
        },
        ExpressionAttributeValues={
            ':value': value,
        },
    )
