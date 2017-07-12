package main

import (
  "fmt"
  "flag"
  "os"
  _ "github.com/aws/aws-sdk-go/aws/credentials"
  _ "github.com/aws/aws-sdk-go/aws/session"
  _ "github.com/aws/aws-sdk-go/aws"
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

  if kmsId == "" || flag.NArg() < 2 {
    fmt.Print("Usage: s3csecmd [-a <access_key>] [-s <secret_key>] -kms-id <kms_id> SOURCE DEST\n")
    os.Exit(1)
  }

  sourcePath = flag.Arg(0)
  destPath = flag.Arg(1)
}

func main() {

}
