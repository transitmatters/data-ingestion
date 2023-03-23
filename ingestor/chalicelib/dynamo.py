import boto3
dynamodb = boto3.resource('dynamodb')


DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"


def dynamo_batch_write(tt_objects, table_name):
    table = dynamodb.Table(table_name)
    if len(tt_objects) == 0:
        return
    with table.batch_writer() as batch:
        for item in tt_objects:
            batch.put_item(Item=item)


def query_dynamo(params, table):
    table = dynamodb.Table(table)
    response = table.query(**params)
    return response['Items']