# S3 Bucket Setup Documentation

## 1. Create S3 Bucket

### Using AWS CLI
```bash
# Create bucket in eu-west-1 region
aws s3api create-bucket \
    --bucket music-streaming-analyses-bucket \
    --region eu-west-1 \
    --create-bucket-configuration LocationConstraint=eu-west-1

# Enable versioning
aws s3api put-bucket-versioning \
    --bucket music-streaming-analyses-bucket \
    --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
    --bucket music-streaming-analyses-bucket \
    --server-side-encryption-configuration '{
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                }
            }
        ]
    }'
```

### Create Directory Structure
```bash
# Create required directories
for dir in streams validated processed archived scripts kpis; do
    aws s3api put-object \
        --bucket music-streaming-analyses-bucket \
        --key ${dir}/
done
```

## 2. IAM Roles and Policies

### Create IAM Role for Glue

```bash
# Create role
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

# Attach AWS managed policy for Glue
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

### Create Custom S3 Access Policy

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

```bash
# Create policy
aws iam create-policy \
    --policy-name MusicStreamingS3Access \
    --policy-document file://s3-policy.json

# Attach policy to role
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::ACCOUNT_ID:policy/MusicStreamingS3Access
```

## 3. Bucket Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::ACCOUNT_ID:role/AWSGlueServiceRole-MusicStreaming"
            },
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
        },
        {
            "Sid": "DenyUnencryptedTransport",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::music-streaming-analyses-bucket",
                "arn:aws:s3:::music-streaming-analyses-bucket/*"
            ],
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": "false"
                }
            }
        }
    ]
}
```

```bash
# Apply bucket policy
aws s3api put-bucket-policy \
    --bucket music-streaming-analyses-bucket \
    --policy file://bucket-policy.json
```

## 4. Lifecycle Rules

```bash
# Add lifecycle rule to move old data to Glacier
aws s3api put-bucket-lifecycle-configuration \
    --bucket music-streaming-analyses-bucket \
    --lifecycle-configuration '{
        "Rules": [
            {
                "ID": "ArchiveOldData",
                "Status": "Enabled",
                "Prefix": "archived/",
                "Transitions": [
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER"
                    }
                ]
            }
        ]
    }'
```

## 5. CORS Configuration (if needed)

```bash
# Add CORS rules
aws s3api put-bucket-cors \
    --bucket music-streaming-analyses-bucket \
    --cors-configuration '{
        "CORSRules": [
            {
                "AllowedHeaders": ["*"],
                "AllowedMethods": ["GET", "PUT", "POST"],
                "AllowedOrigins": ["*"],
                "MaxAgeSeconds": 3000
            }
        ]
    }'
```

## 6. Monitoring and Logging

### Enable Access Logging
```bash
# Create logging bucket first
aws s3api create-bucket \
    --bucket music-streaming-logs \
    --region eu-west-1 \
    --create-bucket-configuration LocationConstraint=eu-west-1

# Enable access logging
aws s3api put-bucket-logging \
    --bucket music-streaming-analyses-bucket \
    --bucket-logging-status '{
        "LoggingEnabled": {
            "TargetBucket": "music-streaming-logs",
            "TargetPrefix": "s3-access-logs/"
        }
    }'
```

## 7. Verification Steps

```bash
# Verify bucket creation
aws s3api get-bucket-location \
    --bucket music-streaming-analyses-bucket

# Check versioning status
aws s3api get-bucket-versioning \
    --bucket music-streaming-analyses-bucket

# Verify encryption
aws s3api get-bucket-encryption \
    --bucket music-streaming-analyses-bucket

# List bucket contents
aws s3 ls s3://music-streaming-analyses-bucket/

# Test permissions
aws s3api put-object \
    --bucket music-streaming-analyses-bucket \
    --key test/test.txt \
    --body test.txt
```

## 8. Security Best Practices

1. **Access Control**
   - Use least privilege principle
   - Regularly audit IAM policies
   - Use bucket policies for fine-grained control

2. **Encryption**
   - Enable default encryption
   - Use SSL/TLS for data in transit
   - Consider using KMS for sensitive data

3. **Monitoring**
   - Enable CloudTrail logging
   - Set up CloudWatch alarms
   - Regular security audits

4. **Compliance**
   - Enable versioning
   - Configure lifecycle rules
   - Implement backup strategy

## 9. Troubleshooting

### Common Issues and Solutions

1. **Access Denied Errors**
   - Verify IAM role permissions
   - Check bucket policy
   - Ensure encryption settings match

2. **Performance Issues**
   - Review S3 best practices
   - Check object naming conventions
   - Monitor request rates

3. **Cost Management**
   - Monitor storage usage
   - Review lifecycle rules
   - Check access patterns