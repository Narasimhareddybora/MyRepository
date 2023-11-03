import boto3

# Get the service resource.
aws_access_key_id = 'AKIARAWX6HSWP2HNP7P4'
aws_secret_access_key = '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'
dynamodb = boto3.client('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName="conversation-history-store",
    KeySchema=[{"AttributeName": "SessionId", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "SessionId", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",
)

# Wait until the table exists.
table.meta.client.get_waiter("table_exists").wait(TableName="SessionTable")

# Print out some data about the table.
print(table.item_count)
