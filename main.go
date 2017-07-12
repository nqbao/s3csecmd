package main

import (
  "fmt"
  "flag"
  "os"
  "github.com/aws/aws-sdk-go/aws/credentials"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/aws"
  "strings"
)

var (
  awsAccessKey string
  awsSecretKey string
  awsRegion string
  sourcePath string
  destPath string
  kmsId string
)

func validateArgs() {
  flag.StringVar(&awsAccessKey, "access-key", "", "AWS Access Key")
  flag.StringVar(&awsSecretKey, "secret-key", "", "AWS Secret Key")
  flag.StringVar(&kmsId, "kms-id", "", "KMS Key Id")
  flag.StringVar(&awsRegion, "region", "us-east-1", "AWS Region")

  flag.Parse()

  // try to use environment variable if there is
  if val, ok := os.LookupEnv("KMS_ID"); kmsId == "" && ok {
    kmsId = val
  }

  if kmsId == "" || flag.NArg() < 3 {
    fmt.Print("Usage: s3csecmd [-a <access_key>] [-s <secret_key>] -kms-id <kms_id> [CMD] ARG1 ARG2 ... ARGN\n")
    fmt.Print("    Possible commands are:\n")
    fmt.Print("    s3csecmd cp SOURCE DEST\n")
    fmt.Print("\n")
    os.Exit(1)
  }

  cmd := flag.Arg(0)
  if cmd == "cp" {
    sourcePath = flag.Arg(1)
    destPath = flag.Arg(2)
  } else {
    fmt.Printf("Unknown command %v\n", cmd)
    os.Exit(1)
  }
}

func main() {
  validateArgs()

  var (
    creds *credentials.Credentials
  )

  if awsAccessKey != "" {
    creds = credentials.NewStaticCredentials(
      awsAccessKey, awsSecretKey, "",
    )
  }

  sess, err := session.NewSession(&aws.Config{
    Credentials: creds,
    Region: &awsRegion,
  })

  if err != nil {
    panic(err)
  }

  cli := NewClient(sess, kmsId)
  if strings.Contains(sourcePath, "s3://") {
    s3loc, err := NewS3Location(sourcePath)

    if err == nil {
      err = cli.DownloadFolder(s3loc, destPath)
    }

    if err != nil {
      panic(err)
    }
  } else if strings.Contains(destPath, "s3://") {
    s3loc, err := NewS3Location(destPath)

    if err == nil {
      err = cli.UploadFolder(sourcePath, s3loc)
    }

    if err != nil {
      panic(err)
    }
  } else {
    fmt.Print("You must specify at least one s3 location.\n")
    os.Exit(1)
  }
}
