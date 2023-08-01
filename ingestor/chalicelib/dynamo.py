import boto3

dynamodb = boto3.resource("dynamodb")


def dynamo_batch_write(speed_objects, table_name):
    """Write objects to dynamo tables. Splitting up oversize batches is configured automatically."""
    table = dynamodb.Table(table_name)
    if len(speed_objects) == 0:
        return
    with table.batch_writer() as batch:
        for item in speed_objects:
            batch.put_item(Item=item)


def query_dynamo(params, table):
    """Send query to dynamo."""
    table = dynamodb.Table(table)
    response = table.query(**params)
    return response["Items"]
