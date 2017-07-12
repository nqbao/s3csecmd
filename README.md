# s3csecmd

s3csecmd is a command line utility to interact with S3 with [Client-side encryption using KMS](http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingClientSideEncryption.html).
This tool allow you to quickly upload and download files on S3 encrypted with KMS. For download, it also supports both encrypted and non-encrypted files. 

## Usage

To upload from local disk to S3. 

```bash
s3csecmd --kms-id XXXX cp local_path s3://bucket/remote/path
```

To download from S3 to local disk

```bash
s3csecmd --kms-id XXXX cp s3://bucket/remote/path local_path
```

## TODO

 - [ ] Improve error handling
 - [ ] Add options to customize encryption method
