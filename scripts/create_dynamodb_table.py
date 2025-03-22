import boto3
from botocore.exceptions import ClientError


def create_kpi_table(table_name, region="eu-west-1", **kwargs):
    """Create DynamoDB table for KPI data with GSIs"""

    dynamodb = boto3.client("dynamodb", region_name=region, **kwargs)

    try:
        response = dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
                {"AttributeName": "kpi_type", "AttributeType": "S"},
                {"AttributeName": "date", "AttributeType": "S"},
                {"AttributeName": "genre", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 50, "WriteCapacityUnits": 50},
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "KpiTypeIndex",
                    "KeySchema": [
                        {"AttributeName": "kpi_type", "KeyType": "HASH"},
                        {"AttributeName": "timestamp", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 50,
                        "WriteCapacityUnits": 50,
                    },
                },
                {
                    "IndexName": "GenreDateIndex",
                    "KeySchema": [
                        {"AttributeName": "genre", "KeyType": "HASH"},
                        {"AttributeName": "date", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 50,
                        "WriteCapacityUnits": 50,
                    },
                },
            ],
        )
        print(f"Table {table_name} created successfully")
        return response

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"Table {table_name} already exists")
        else:
            raise e


if __name__ == "__main__":
    create_kpi_table("music_streaming_kpis")
