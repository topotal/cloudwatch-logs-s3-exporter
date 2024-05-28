# cloudwatch-logs-s3-exporter

Lambda Function exports CloudWatch Logs to S3 without duplicates or omissions.

## Inputs

- `DestinationS3Bucket`
  - The destination S3 Bucket for the export (e.g. `my-cwlogs-archive`)
- `SourceLogGroupPrefixes`
  - The LogGroup Prefixes to be exported (Comma Separated Values) (e.g. `/aws/eks/cluster/dev,/aws/eks/cluster/prd`)
- `StateStoreType`
  - The type of store (e.g. `s3`)
- `StoreDSN`
  - The DSN of store (e.g. `s3://my-bucket/state.json`)
- `LogLevel` (Optional)
  - One of `DEBUG`, `INFO`, `WARN`, `ERROR` (default: `INFO`)

## How It Works

This behaves as follows:

- First Run
  - Exports all LogStreams matching the `SourceLogGroupPrefixies` from the LogGroups
    - Creates an export task for each LogStream
    - Sets the `from` and `to` for the export task as follows:
      - `from`: `CreationTime` of the LogStream
      - `to`: The execution time of `cloudwatch-logs-s3-exporter`
  - Saves the following information in the `state.json` in the specified S3 bucket (`StoreDSN`)
    - `LogGroupArn`
    - `LogStreamArn`
    - `ExportedAt`
- Subsequent Runs
  - Selectively exports LogStreams from LogGroups matching the `SourceLogGroupPrefixies`
    - Retrieves `state.json` and exports LogStreams based on the following conditions:
      - If LogStream's `LastIngestionTime` <= `ExportedAt`
        - Skips exporting
      - If LogStream's `LastIngestionTime` > `ExportedAt`
        - Sets `from` to `ExportedAt`, `to` to `LastIngestionTime` and executes the export

## Concurrency

LogStreams are exported one at a time. This is due to the limitation imposed by CloudWatch Logs that only one Export Task can be executed per account at a time. For more details, please refer to the following documentation:

https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html

## State Persistence

When saving the state to S3 (StateStoreType == s3), the behavior is as follows to reduce the number of GetObject and PutObject operations:

- At the start of the function, `state.json` is retrieved using GetObject and loaded into memory.
- The state in memory is updated once the ExportTask is completed.
- After all targeted LogStreams have completed their exports, PutObject is performed to save the state.

## Behavior During Timeout

To preserve the state, the function begins its termination process approximately 30 seconds before the Lambda function times out. If there are 30 seconds remaining, the export is interrupted, the state is saved using PutObject, and then the function terminates.

## Author

Ryota Yoshikawa (@rrreeeyyy)

## LICENSE

MIT
