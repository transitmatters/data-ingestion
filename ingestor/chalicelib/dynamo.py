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


def dynamo_update_items(items, table_name, key_names, remove_if_missing=()):
    """Writes items with update_item, so attributes not in the item are kept (put_item replaces the whole item).

    Args:
        items: A list of dicts to write. Each must contain every key in key_names.
        table_name: The name of the DynamoDB table.
        key_names: The table's key attribute names.
        remove_if_missing: Attributes to REMOVE when an item doesn't include them, so
            fields the caller owns don't keep stale values.
    """
    if not items:
        return
    table = dynamodb.Table(table_name)
    for item in items:
        attrs = {k: v for k, v in item.items() if k not in key_names}
        names = {f"#a{i}": k for i, k in enumerate(attrs)}
        values = {f":v{i}": v for i, v in enumerate(attrs.values())}
        expression = "SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(attrs))) if attrs else ""
        to_remove = [k for k in remove_if_missing if k not in item]
        if to_remove:
            names.update({f"#r{i}": k for i, k in enumerate(to_remove)})
            expression += " REMOVE " + ", ".join(f"#r{i}" for i in range(len(to_remove)))
        if not expression:
            continue
        kwargs = {
            "Key": {k: item[k] for k in key_names},
            "UpdateExpression": expression.strip(),
            "ExpressionAttributeNames": names,
        }
        if values:
            kwargs["ExpressionAttributeValues"] = values
        table.update_item(**kwargs)


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
