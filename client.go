package main

import (
  "strings"
  "os"
  "io"
  "errors"
  "net/url"
  "path/filepath"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/awserr"
  "github.com/aws/aws-sdk-go/service/kms"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3/s3crypto"
  "github.com/aws/aws-sdk-go/service/s3"
  "github.com/yookoala/realpath"
  "fmt"
  "strconv"
  "sync"
)

type S3Location struct {
  Bucket string
  Key string
}

var (
  InvalidS3LocationError = errors.New("Invalid S3 Location")
  InvalidS3FolderError = errors.New("S3 Key must point to an S3 folder")
  FolderNotWritableError = errors.New("Folder is not writable")
  NotExistError = errors.New("Folder does not exist")
  workers int = 0
)

func init() {
  val, ok := os.LookupEnv("NUM_WORKERS")
  if ok {
    parsedVal, err := strconv.Atoi(val)

    if err == nil {
      workers = parsedVal
    }
  }

  if workers == 0 {
    workers = 8
  }
}

func NewS3Location(path string) (location *S3Location, err error) {
  if !strings.Contains(path, "s3://") {
    return nil, InvalidS3LocationError
  }

  u, err := url.Parse(path)

  if err != nil {
    return nil, err
  }

  location = &S3Location{
    Bucket: u.Host,
    Key: strings.Trim(u.Path, "/"),
  }

  return
}

type Client struct {
  Session *session.Session
  KmsId string
  s3 *s3.S3
  encryptionClient *s3crypto.EncryptionClient
  decryptionClient *s3crypto.DecryptionClient
}

// Make load strategy configurable
func NewClient(sess *session.Session, kmsId string) (client *Client) {
  client = &Client{
    Session: sess,
    KmsId: kmsId,
  }

  client.Init()

  return
}

func (cli *Client) Init() {
  handler := s3crypto.NewKMSKeyGenerator(kms.New(cli.Session), cli.KmsId)
  crypto := s3crypto.AESGCMContentCipherBuilder(handler)
  cli.s3 = s3.New(cli.Session)
  cli.encryptionClient = s3crypto.NewEncryptionClient(cli.Session, crypto)
  cli.decryptionClient = s3crypto.NewDecryptionClient(cli.Session)
}

func (cli *Client) UploadFile(source string, dest *S3Location) (error) {
  file, openErr := os.Open(source)

  if openErr != nil {
    return openErr
  }
  defer file.Close()

  input := &s3.PutObjectInput{
    Body:                 aws.ReadSeekCloser(file),
    Bucket:               &dest.Bucket,
    Key:                  &dest.Key,
    ACL:                  aws.String("bucket-owner-full-control"),
  }

  _, err2 := cli.encryptionClient.PutObject(input)
  return err2
}

func (cli *Client) DownloadFile(source *S3Location, dest string) (error) {
  // we want to support both encrypted and non-encrypted, so we call a HEAD to check
  head, err := cli.s3.HeadObject(&s3.HeadObjectInput{
    Bucket: &source.Bucket,
    Key: &source.Key,
  })

  if err != nil {
    return err
  }

  var output *s3.GetObjectOutput
  input := &s3.GetObjectInput{
    Bucket: &source.Bucket,
    Key: &source.Key,
  }

  // from http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/package-summary.html
  // key wrapping algorithm, x-amz-wrap-alg is always set to "kms" for CSE
  if val, ok := head.Metadata["X-Amz-Wrap-Alg"]; ok && *val == "kms" {
    output, err = cli.decryptionClient.GetObject(input)
  } else {
    output, err = cli.s3.GetObject(input)
  }

  if err != nil {
    return err
  }

  dir := filepath.Dir(dest)
  os.MkdirAll(dir, 0766)

  file, openErr := os.Create(dest)
  if openErr != nil {
    return openErr
  }
  defer file.Close()

  _, writeErr := io.Copy(file, output.Body)
  return writeErr
}

func (cli *Client) DownloadFolder(source *S3Location, dest string) (error) {
  // validate the destination
  if ok, _ := IsDirWritable(dest); !ok {
    return FolderNotWritableError
  }

  // validate the source
  if err := cli.validateFolderKey(source); err != nil {
    return err
  }

  workerErrorCh := make(chan error)
  resCh, errCh := cli.listObjects(source, workerErrorCh)

  // start upload goroutines
  wg := sync.WaitGroup{}
  wg.Add(workers)
  for i := 0; i < workers; i++ {
    go func() {
      defer wg.Done()

      startIdx := len(source.Key)
      for item := range resCh {
        destFile := strings.Trim(item.Key[startIdx:], "/")
        fmt.Printf("Downloading %v ...\n", destFile)
        err := cli.DownloadFile(item, filepath.Join(dest, destFile))

        if err != nil {
          close(resCh) // XXX
          workerErrorCh <- err
          break
        }
      }
    }()
  }

  err := <- errCh
  wg.Wait()

  if err == io.EOF {
    return nil
  }

  return err
}

func (cli *Client) UploadFolder(source string, dest *S3Location) (error) {
  realPath, err := realpath.Realpath(source)

  if err != nil {
    return err
  }

  stat, err := os.Stat(realPath)
  if err != nil {
    return err
  }

  if !stat.IsDir() {
    return NotExistError
  }

  resCh := make(chan string)
  workerErrCh := make(chan error)
  errCh := make(chan error)

  // start the workers
  wg := sync.WaitGroup{}
  wg.Add(workers)
  for i := 0; i < workers; i++ {
    go func() {
      defer wg.Done()

      for key := range resCh {
        sourceFile := filepath.Join(source, key)
        destFile := &S3Location{dest.Bucket, fmt.Sprintf("%v/%v", dest.Key, key)}
        fmt.Printf("Uploading %v\n", key)
        err := cli.UploadFile(sourceFile, destFile)

        if err != nil {
          close(resCh) // XXX
          workerErrCh <- err
          break
        }
      }
    }()
  }

  go func() {
    startIdx := len(realPath)
    upload := func(path string, f os.FileInfo, err error) error {
      // check if we have any worker error then propagate the error
      select {
      case workerErr := <- workerErrCh:
        return workerErr
      default:
      }

      if err != nil {
        return err
      }

      if !f.IsDir() {
        key := path[startIdx+1:]
        resCh <- key
      }

      return nil
    }

    err := filepath.Walk(realPath, upload)

    if err == nil {
      errCh <- io.EOF
    } else {
      errCh <- err
    }
  }()

  finalErr := <- errCh
  close(resCh)  // this is to terminate the workers
  wg.Wait()     // and wait for them to finish

  if finalErr == io.EOF {
    return nil
  }

  return finalErr
}

// Validate S3 location for download. It will make sure that the location is not pointed
// to an S3 file
func (cli *Client) validateFolderKey(source *S3Location) (error) {
  svc := s3.New(cli.Session)

  // just make sure this is not a file
  if len(source.Key) > 0 {
    _, err := svc.GetObject(&s3.GetObjectInput{
      Bucket: &source.Bucket,
      Key: &source.Key,
    })

    if err != nil {
      // it's okie that the key does not exist
      if awsErr, ok := err.(awserr.Error); ok {
        if awsErr.Code() != s3.ErrCodeNoSuchKey {
          return err
        }
      } else {
        return err
      }
    } else {
      return InvalidS3FolderError
    }
  }

  return nil
}

// List object from an S3Location and yielding the result
func (cli *Client) listObjects(location *S3Location, workerErrCh chan error) (resCh chan *S3Location, errCh chan error) {
  resCh = make(chan *S3Location)
  errCh = make(chan error)

  svc := s3.New(cli.Session)
  go func() {
    var marker string

    sendError := func(err error) {
      errCh <- err
      close(resCh)
    }

    checkWorkerError := func() bool {
      // propagate the error the stop the whole process
      select {
      case workerErr := <- workerErrCh:
        sendError(workerErr)
        return true
      default:
        return false
      }
    }

    for {
      if checkWorkerError() {
        return
      }

      output, err := svc.ListObjects(&s3.ListObjectsInput{
        Bucket: &location.Bucket,
        Prefix: &location.Key,
        Marker: &marker,
      })

      if err != nil {
        sendError(err)
        break
      }

      for _, object := range output.Contents {
        if checkWorkerError() {
          return
        }

        // skip directory and instruction file
        if !strings.HasSuffix(*object.Key, "/") &&
          !strings.HasSuffix(*object.Key, s3crypto.DefaultInstructionKeySuffix) {
          resCh <- &S3Location{
            Bucket: location.Bucket,
            Key: *object.Key,
          }
        }

        marker = *object.Key
      }

      if *output.IsTruncated {
        // if NextMarker is available, then use it
        if output.NextMarker != nil {
          marker = *output.NextMarker
        }
      } else {
        sendError(io.EOF)
        break
      }
    }
  }()

  return
}
