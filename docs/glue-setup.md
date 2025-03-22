# AWS Glue Setup Documentation

## 1. IAM Role Setup for Glue

### Create Base Glue Service Role

```bash
# Create the role with trust policy
aws iam create-role \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# Attach AWS managed Glue service policy
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

### Create Custom Policies

#### S3 Access Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::music-streaming-analyses-bucket",
                "arn:aws:s3:::music-streaming-analyses-bucket/*"
            ]
        }
    ]
}
```

#### DynamoDB Access Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchWriteItem",
                "dynamodb:PutItem",
                "dynamodb:DescribeTable"
            ],
            "Resource": "arn:aws:dynamodb:eu-west-1:ACCOUNT_ID:table/music_streaming_kpis"
        }
    ]
}
```

```bash
# Create and attach policies
aws iam create-policy \
    --policy-name GlueS3Access \
    --policy-document file://glue-s3-policy.json

aws iam create-policy \
    --policy-name GlueDynamoDBAccess \
    --policy-document file://glue-dynamodb-policy.json

aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::ACCOUNT_ID:policy/GlueS3Access

aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::ACCOUNT_ID:policy/GlueDynamoDBAccess
```

## 2. Glue Jobs Setup

### Upload Scripts to S3
```bash
# Upload Glue job scripts
aws s3 cp scripts/validate_data.py s3://music-streaming-analyses-bucket/scripts/
aws s3 cp scripts/compute_kpis.py s3://music-streaming-analyses-bucket/scripts/
aws s3 cp scripts/load_dynamodb.py s3://music-streaming-analyses-bucket/scripts/
```

### Create Glue Jobs

#### 1. Data Validation Job
```bash
aws glue create-job \
    --name music_streaming_validation \
    --role AWSGlueServiceRole-MusicStreaming \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://music-streaming-analyses-bucket/scripts/validate_data.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--TempDir": "s3://music-streaming-analyses-bucket/temporary/",
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable"
    }' \
    --glue-version "3.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --timeout 60 \
    --execution-property '{"MaxConcurrentRuns": 3}'
```

#### 2. KPI Computation Job
```bash
aws glue create-job \
    --name music_streaming_kpi_computation \
    --role AWSGlueServiceRole-MusicStreaming \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://music-streaming-analyses-bucket/scripts/compute_kpis.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--TempDir": "s3://music-streaming-analyses-bucket/temporary/",
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable"
    }' \
    --glue-version "3.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --timeout 60 \
    --execution-property '{"MaxConcurrentRuns": 3}'
```

#### 3. DynamoDB Load Job
```bash
aws glue create-job \
    --name music_streaming_dynamodb_load \
    --role AWSGlueServiceRole-MusicStreaming \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://music-streaming-analyses-bucket/scripts/load_dynamodb.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--TempDir": "s3://music-streaming-analyses-bucket/temporary/",
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable"
    }' \
    --glue-version "3.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --timeout 60 \
    --execution-property '{"MaxConcurrentRuns": 3}'
```

## 3. Glue Job Configuration Details

### Worker Types and Resources
- **G.1X**
  - 1 DPU (4 vCPU, 16GB memory)
  - Best for memory-intensive jobs
  - Suitable for most ETL workloads

### Timeout and Concurrency
- Timeout: 60 minutes
- Max Concurrent Runs: 3
- Job Bookmark: Disabled (handled by Airflow)

### Script Parameters
1. **Validation Job**
   - Input path: `s3://<bucket>/streams/`
   - Reference data: Songs and Users files
   - Output path: `s3://<bucket>/validated/`

2. **KPI Computation Job**
   - Input path: Validated data
   - Output path: `s3://<bucket>/kpis/`
   - Date parameters from Airflow

3. **DynamoDB Load Job**
   - Input path: Computed KPIs
   - Target table: `music_streaming_kpis`
   - KPI types: user, genre, trending

## 4. Monitoring and Logging

### CloudWatch Logs
```bash
# Enable CloudWatch logging for Glue
aws glue put-resource-policy \
    --policy-in-json '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": [
                    "arn:aws:logs:eu-west-1:ACCOUNT_ID:log-group:/aws-glue/*"
                ]
            }
        ]
    }'
```

### Metrics and Alerts
```bash
# Create CloudWatch alarm for failed Glue jobs
aws cloudwatch put-metric-alarm \
    --alarm-name GlueJobFailure \
    --alarm-description "Alarm when any Glue job fails" \
    --metric-name glue.driver.aggregate.numFailedTasks \
    --namespace AWS/Glue \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --alarm-actions arn:aws:sns:eu-west-1:ACCOUNT_ID:GlueJobAlerts
```

## 5. Troubleshooting

### Common Issues

1. **Memory Issues**
   - Increase worker type to G.2X
   - Add more workers
   - Optimize script memory usage

2. **Timeout Issues**
   - Increase timeout value
   - Optimize job performance
   - Consider partitioning data

3. **Permission Issues**
   - Verify IAM role permissions
   - Check resource policies
   - Validate cross-account access

### Debugging Tips

1. **View Logs**
```bash
aws logs get-log-events \
    --log-group-name /aws-glue/jobs/job-name \
    --log-stream-name stream-name
```

2. **Monitor Metrics**
```bash
aws cloudwatch get-metric-statistics \
    --namespace AWS/Glue \
    --metric-name glue.driver.aggregate.elapsedTime \
    --dimensions Name=JobName,Value=job-name \
    --start-time 2023-01-01T00:00:00 \
    --end-time 2023-01-02T00:00:00 \
    --period 3600 \
    --statistics Average
```

## 6. Best Practices

1. **Performance Optimization**
   - Use appropriate worker type
   - Optimize data partitioning
   - Implement efficient transformations

2. **Cost Management**
   - Monitor DPU usage
   - Use job bookmarks when appropriate
   - Implement timeout controls

3. **Security**
   - Use least privilege access
   - Encrypt sensitive data
   - Regular security audits

4. **Maintenance**
   - Regular script updates
   - Monitor job metrics
   - Update dependencies