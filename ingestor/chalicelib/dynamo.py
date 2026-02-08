import boto3

dynamodb = boto3.resource("dynamodb")


def dynamo_batch_write(speed_objects, table_name):
    """Writes a list of items to a DynamoDB table using batch_writer.

    Batch size limits are handled automatically by boto3's batch_writer.

    Args:
        speed_objects: A list of dicts to write as DynamoDB items.
        table_name: The name of the DynamoDB table.
    """
    table = dynamodb.Table(table_name)
    if len(speed_objects) == 0:
        return
    with table.batch_writer() as batch:
        for item in speed_objects:
            batch.put_item(Item=item)


def query_dynamo(params, table):
    """Queries a DynamoDB table and returns the matching items.

    Args:
        params: Query parameters passed to DynamoDB Table.query().
        table: The name of the DynamoDB table to query.

    Returns:
        A list of item dicts matching the query.
    """
    table = dynamodb.Table(table)
    response = table.query(**params)
    return response["Items"]
