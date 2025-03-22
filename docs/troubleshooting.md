# Troubleshooting Guide

## Common Issues and Solutions

### 1. Pipeline Initialization Failures

#### S3 Structure Setup Fails
```yaml
Symptoms:
  - Task `init_s3_structure` fails
  - S3 permission errors in logs
  - Missing directories in S3 bucket

Resolution:
  1. Verify AWS credentials and permissions
  2. Check S3 bucket exists: music-streaming-analyses-bucket
  3. Ensure IAM role has required permissions
  4. Verify required directories match REQUIRED_S3_DIRS constant
  5. Check S3 bucket versioning status
```

#### Glue Script Upload Issues
```yaml
Symptoms:
  - Task `verify_glue_scripts` fails
  - Missing scripts in S3_SCRIPTS_PREFIX
  - Upload failures in logs

Resolution:
  1. Verify local script files exist in /usr/local/airflow/scripts/
  2. Check script permissions
  3. Validate script content:
     - validate_data.py
     - compute_kpis.py
     - load_dynamodb.py
  4. Ensure sufficient S3 permissions
```

### 2. Glue Job Failures

#### Validation Job Issues
```yaml
Symptoms:
  - GLUE_JOB_VALIDATION fails
  - Data quality errors in logs
  - Incomplete validated data

Checks:
  1. Input data format in S3_RAW_PREFIX
  2. Glue job parameters:
     - Worker Type: G.1X
     - Number of Workers: 2
     - Timeout: 60 minutes
  3. Reference data availability
  4. CloudWatch logs for detailed errors
```

#### KPI Computation Errors
```yaml
Symptoms:
  - GLUE_JOB_KPI fails
  - Missing or incomplete KPIs
  - Processing timeouts

Resolution:
  1. Verify validated data exists
  2. Check reference data state
  3. Monitor memory usage
  4. Review data partitioning
  5. Adjust worker configuration if needed
```

#### DynamoDB Load Failures
```yaml
Symptoms:
  - GLUE_JOB_DYNAMODB fails
  - Throttling errors
  - Incomplete data loads

Resolution:
  1. Check DynamoDB table exists
  2. Verify table capacity
  3. Monitor write throughput
  4. Check data format consistency
  5. Review error handling in load_dynamodb.py
```

### 3. Data Quality Issues

#### Missing or Invalid Data
```yaml
Checks:
  1. Raw data completeness
  2. Reference data integrity
  3. Validation rules in validate_data.py
  4. Error logs in validated/ directory
  5. Data format consistency
```

#### KPI Accuracy Problems
```yaml
Investigation Steps:
  1. Review compute_kpis.py logic
  2. Check input data quality
  3. Verify aggregation logic
  4. Compare with historical patterns
  5. Validate reference data state
```

### 4. Performance Issues

#### Slow Pipeline Execution
```yaml
Diagnosis:
  1. Monitor task duration trends
  2. Check resource utilization:
     - Glue DPU usage
     - Worker memory
     - Network throughput
  3. Review concurrent executions
  4. Analyze data volume patterns
```

#### Resource Bottlenecks
```yaml
Resolution Steps:
  1. Identify bottleneck:
     - CPU utilization
     - Memory usage
     - I/O operations
     - Network bandwidth
  2. Adjust resources:
     - Increase GLUE_NUM_WORKERS
     - Modify GLUE_WORKER_TYPE
     - Update GLUE_TIMEOUT
  3. Optimize job configuration
```

### 5. System Integration Issues

#### AWS Connectivity
```yaml
Checks:
  1. AWS credentials validity
  2. VPC configuration
  3. Security group rules
  4. IAM role permissions
  5. Service endpoints
```

#### Airflow Configuration
```yaml
Verification:
  1. AWS connection settings
  2. DAG configuration
  3. Variable definitions
  4. Worker settings
  5. Queue configuration
```

## Recovery Procedures

### 1. Data Recovery

#### Corrupted Data Recovery
```markdown
1. Identify affected data range
2. Locate backup in S3_ARCHIVED_PREFIX
3. Restore to appropriate stage
4. Rerun validation
5. Verify recovered data
```

#### Failed Job Recovery
```markdown
1. Clear failed task state
2. Restore processing checkpoints
3. Reset job parameters
4. Restart from last successful task
5. Monitor recovery progress
```

### 2. System Recovery

#### Pipeline Reset
```markdown
1. Stop active tasks
2. Clear task instances
3. Reset processing state
4. Verify system prerequisites
5. Restart pipeline
```

#### Emergency Shutdown
```markdown
1. Pause DAG
2. Stop active Glue jobs
3. Secure incomplete data
4. Document system state
5. Plan recovery steps
```

## Preventive Measures

### 1. Regular Maintenance

#### Daily Checks
```markdown
1. Review task success rates
2. Monitor resource usage
3. Check data quality metrics
4. Verify archival process
5. Update processing logs
```

#### Weekly Tasks
```markdown
1. Analyze performance trends
2. Review error patterns
3. Update monitoring thresholds
4. Clean up temporary data
5. Verify backup integrity
```

### 2. System Updates

#### Configuration Updates
```markdown
1. Review AWS service limits
2. Update resource allocations
3. Adjust timeout values
4. Modify retry policies
5. Update alert thresholds
```

#### Code Maintenance
```markdown
1. Update Glue scripts
2. Optimize processing logic
3. Review error handling
4. Update dependencies
5. Document changes
```

## Support Resources

### 1. Documentation
- System Architecture (`docs/architecture.md`)
- Monitoring Guide (`docs/monitoring.md`)
- Setup Instructions (`docs/setup.md`)
- Data Flow Documentation (`docs/data-flow.md`)

### 2. Contact Information
```yaml
Support Levels:
  L1: data-engineering@company.com
  L2: infrastructure@company.com
  L3: system-architects@company.com
  Emergency: oncall@company.com

Slack Channels:
  - #data-pipeline-support
  - #aws-infrastructure
  - #data-engineering
```

### 3. Useful Commands
```bash
# Check Glue job status
aws glue get-job-run --job-name music_streaming_validation --run-id <run_id>

# View CloudWatch logs
aws logs get-log-events --log-group-name /aws-glue/jobs --log-stream-name <stream>

# Check DynamoDB table status
aws dynamodb describe-table --table-name music_streaming_kpis

# List S3 contents
aws s3 ls s3://music-streaming-analyses-bucket/

# Check Airflow task logs
airflow tasks test music_streaming_pipeline task_id 2024-01-01
```