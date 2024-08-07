# S3 Writer

## Description
Adding a new LogStreamWriter, S3 Writer, to write logs to S3 by Singer. This involves a new writer configuration as shown below.
Singer employs a buffer directory to store log messages in buffer files before uploading them to S3. The size of these buffered files is important to take into account for thresholds.
The current design employs two thresholds, a timer-based (in seconds) and file-sized threshold (in megabytes). The writer will upload the log file to S3 when either of the thresholds is met first.
For example, if 50MB of log messages is reached before 5 seconds, the writer will upload the log file to S3 because the file size threshold has been exceeded. If 5 seconds is reached before 50MB, the writer will upload the log file to S3 because the timer threshold has expired.

## Example writer configuration in Singer Log Configuration
```
...
writer.type=s3
writer.s3.maxFileSizeMB=50 ← max(50, maxFileSizeMB), default 50
writer.s3.minUploadTimeInSeconds=5 ← max(30, minUploadTimeInSeconds), default 30
writer.s3.fileNameFormat=test ← suffix right before a timestamp required
writer.s3.bucket=yourBucket ← AWS bucket name required
writer.s3.keyPrefix=yourPrefix ← AWS keyPrefix name required
writer.s3.maxRetries=3 ← retry logic (if uploads unsuccessful), default 5
writer.s3.bufferDirectory=/tmp/singer/s3 ← buffer directory, default /tmp/singer/s3
```