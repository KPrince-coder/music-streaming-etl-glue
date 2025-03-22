# DynamoDB Query Guide

## AWS CLI Queries

### 1. Basic Table Information

```bash
# Get table description (full details)
aws dynamodb describe-table \
    --table-name music_streaming_kpis

# Get item count and size
aws dynamodb describe-table \
    --table-name music_streaming_kpis \
    --query 'Table.[ItemCount,TableSizeBytes]'

#Get table status
aws dynamodb describe-table \
    --table-name music_streaming_kpis \
    --query 'Table.TableStatus'

# Get table creation date
aws dynamodb describe-table \
    --table-name music_streaming_kpis \
    --query 'Table.CreationDateTime'

# Return at least one item
aws dynamodb scan \
    --table-name music_streaming_kpis \
    --limit 1

# Get ARN (Amazon Resource Name)
aws dynamodb describe-table \
    --table-name music_streaming_kpis \
    --query 'Table.TableArn'
```

### 2. Query Data

#### By Primary Key

```bash
# Get specific KPI by id and timestamp
aws dynamodb get-item \
    --table-name music_streaming_kpis \
    --key '{ 
        "id": {"S": "USER_ENGAGEMENT_20240101"}, 
        "timestamp": {"S": "2024-01-01T00:00:00Z"}
    }'

# Query items by id with timestamp range
aws dynamodb query \
    --table-name music_streaming_kpis \
    --key-condition-expression "id = :id AND #ts BETWEEN :start AND :end" \
    --expression-attribute-names '{"#ts": "timestamp"}' \
    --expression-attribute-values '{
        ":id": {"S": "USER_ENGAGEMENT_20240101"},
        ":start": {"S": "2024-01-01T00:00:00Z"},
        ":end": {"S": "2024-01-01T23:59:59Z"}
    }'
```

#### Using KpiTypeIndex

```bash
# Query all metrics of specific type
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name KpiTypeIndex \
    --key-condition-expression "kpi_type = :type" \
    --expression-attribute-values '{
        ":type": {"S": "user_engagement"}
    }'

# Query metrics by type and time range
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name KpiTypeIndex \
    --key-condition-expression "kpi_type = :type AND #ts BETWEEN :start AND :end" \
    --expression-attribute-names '{"#ts": "timestamp"}' \
    --expression-attribute-values '{
        ":type": {"S": "user_engagement"},
        ":start": {"S": "2024-01-01T00:00:00Z"},
        ":end": {"S": "2024-01-31T23:59:59Z"}
    }'
```

#### Using GenreDateIndex

```bash
# Query metrics for specific genre and date
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name GenreDateIndex \
    --key-condition-expression "genre = :genre AND #date = :date" \
    --expression-attribute-names '{"#date": "date"}' \
    --expression-attribute-values '{
        ":genre": {"S": "rock"},
        ":date": {"S": "2024-01-01"}
    }'
```

### 3. Data Validation Queries

#### Check Data Structure

```bash
# Sample random items (limit 5)
aws dynamodb scan \
    --table-name music_streaming_kpis \
    --limit 5

# Check items with missing attributes
aws dynamodb scan \
    --table-name music_streaming_kpis \
    --filter-expression "attribute_not_exists(value) OR attribute_not_exists(dimensions)"

# Find malformed timestamps
aws dynamodb scan \
    --table-name music_streaming_kpis \
    --filter-expression "attribute_exists(timestamp) AND NOT begins_with(#ts, :year)" \
    --expression-attribute-names '{"#ts": "timestamp"}' \
    --expression-attribute-values '{
        ":year": {"S": "2024"}
    }'
```

## Python Script Examples

### 1. Basic Validation Script

```python
import boto3
from datetime import datetime, timedelta

def validate_kpi_data(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # Check recent data
    now = datetime.utcnow()
    yesterday = (now - timedelta(days=1)).isoformat()
    
    response = table.query(
        IndexName='KpiTypeIndex',
        KeyConditionExpression='kpi_type = :type AND #ts >= :start',
        ExpressionAttributeNames={'#ts': 'timestamp'},
        ExpressionAttributeValues={
            ':type': 'user_engagement',
            ':start': yesterday
        }
    )
    
    print(f"Found {len(response['Items'])} recent user engagement metrics")
    if response['Items']:
        print("Sample item structure:", response['Items'][0])

### 2. Data Quality Check Script
```python
def check_data_quality(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # Check for required fields
    response = table.scan(
        FilterExpression='attribute_not_exists(value) OR attribute_not_exists(kpi_type)',
        ProjectionExpression='id, timestamp'
    )
    
    if response['Items']:
        print("WARNING: Found items missing required fields:", response['Items'])
    
    # Check value ranges
    response = table.scan(
        FilterExpression='#v < :min OR #v > :max',
        ExpressionAttributeNames={'#v': 'value'},
        ExpressionAttributeValues={
            ':min': 0,
            ':max': 1000000  # adjust based on expected ranges
        }
    )
    
    if response['Items']:
        print("WARNING: Found items with suspicious values:", response['Items'])
```

## Common Data Issues to Check

1. **Missing Required Fields**
   - id
   - timestamp
   - kpi_type
   - value
   - dimensions

2. **Data Type Consistency**
   - Numeric values stored as strings
   - Inconsistent timestamp formats
   - Malformed JSON in dimensions field

3. **Value Range Validation**
   - Negative values where inappropriate
   - Unreasonably large values
   - Zero values where inappropriate

4. **Time Series Completeness**
   - Missing time periods
   - Duplicate timestamps
   - Future timestamps

## Music Streaming KPIs - DynamoDB Query Examples

### 1. User Engagement Metrics

```bash
# Query daily active users KPI
aws dynamodb query \
    --table-name music_streaming_kpis \
    --key-condition-expression "id = :id" \
    --expression-attribute-values '{
        ":id": {"S": "daily_active_users#2024-01-01"}
    }'

# Get user engagement metrics for a specific date range
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name KpiTypeIndex \
    --key-condition-expression "kpi_type = :type AND #ts BETWEEN :start AND :end" \
    --expression-attribute-names '{"#ts": "timestamp"}' \
    --expression-attribute-values '{
        ":type": {"S": "user_engagement"},
        ":start": {"S": "2024-01-01"},
        ":end": {"S": "2024-01-02"}
    }'
```

### 2. Genre-Based Metrics

```bash
# Query rock genre performance for specific date
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name GenreDateIndex \
    --key-condition-expression "genre = :genre AND #date = :date" \
    --expression-attribute-names '{"#date": "date"}' \
    --expression-attribute-values '{
        ":genre": {"S": "rock"},
        ":date": {"S": "2024-01-01"}
    }'

# Get all genres metrics for today
aws dynamodb query \
    --table-name music_streaming_kpis \
    --key-condition-expression "begins_with(id, :prefix)" \
    --expression-attribute-values '{
        ":prefix": {"S": "genre_metrics#2024-01-01"}
    }'
```

### 3. Track Performance Metrics

```bash
# Get top tracks metrics
aws dynamodb query \
    --table-name music_streaming_kpis \
    --key-condition-expression "begins_with(id, :prefix)" \
    --expression-attribute-values '{
        ":prefix": {"S": "top_tracks#2024-01"}
    }'

# Query track completion rates
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name KpiTypeIndex \
    --key-condition-expression "kpi_type = :type" \
    --expression-attribute-values '{
        ":type": {"S": "track_completion_rate"}
    }'
```

### 4. Platform Usage Metrics

```bash
# Get device type distribution
aws dynamodb query \
    --table-name music_streaming_kpis \
    --key-condition-expression "begins_with(id, :prefix)" \
    --expression-attribute-values '{
        ":prefix": {"S": "device_distribution#2024-01-01"}
    }'

# Query peak usage times
aws dynamodb query \
    --table-name music_streaming_kpis \
    --key-condition-expression "begins_with(id, :prefix)" \
    --expression-attribute-values '{
        ":prefix": {"S": "hourly_usage#2024-01-01"}
    }'
```

### 5. Data Validation Queries

```bash
# Check for recent data (last 24 hours)
aws dynamodb scan \
    --table-name music_streaming_kpis \
    --filter-expression "#ts >= :yesterday" \
    --expression-attribute-names '{"#ts": "timestamp"}' \
    --expression-attribute-values '{
        ":yesterday": {"S": "2024-01-01T00:00:00Z"}
    }'

# Find specific metric type with value range
aws dynamodb query \
    --table-name music_streaming_kpis \
    --index-name KpiTypeIndex \
    --key-condition-expression "kpi_type = :type" \
    --filter-expression "#val BETWEEN :min AND :max" \
    --expression-attribute-names '{"#val": "value"}' \
    --expression-attribute-values '{
        ":type": {"S": "daily_active_users"},
        ":min": {"N": "1000"},
        ":max": {"N": "5000"}
    }'
```

### 6. Python Example for Complex Queries

```python
import boto3
from datetime import datetime, timedelta

def query_recent_kpis():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('music_streaming_kpis')
    
    # Get yesterday's date in YYYY-MM-DD format
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%01')
    
    # Query multiple metric types
    metric_types = ['daily_active_users', 'track_completion_rate', 'genre_distribution']
    
    results = {}
    for metric_type in metric_types:
        response = table.query(
            IndexName='KpiTypeIndex',
            KeyConditionExpression='kpi_type = :type AND begins_with(#ts, :date)',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={
                ':type': metric_type,
                ':date': yesterday
            }
        )
        results[metric_type] = response['Items']
    
    return results

def query_genre_performance(genre, start_date, end_date):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('music_streaming_kpis')
    
    response = table.query(
        IndexName='GenreDateIndex',
        KeyConditionExpression='genre = :genre AND #date BETWEEN :start AND :end',
        ExpressionAttributeNames={'#date': 'date'},
        ExpressionAttributeValues={
            ':genre': genre,
            ':start': start_date,
            ':end': end_date
        }
    )
    
    return response['Items']
```

### 7. Common ID Patterns to Query

- User Engagement: `daily_active_users#YYYY-MM-DD`
- Genre Metrics: `genre_metrics#YYYY-MM-DD#genre_name`
- Track Performance: `track_metrics#YYYY-MM-DD#track_id`
- Hourly Usage: `hourly_usage#YYYY-MM-DD#HH`
- Device Distribution: `device_distribution#YYYY-MM-DD`

Remember to:

1. Replace dates with actual values
2. Adjust value ranges based on your data
3. Use appropriate index based on query pattern
4. Consider using batch operations for multiple items
