package main

import (
  "testing"
)

func TestValidS3Location(t *testing.T) {
  u, err := NewS3Location("s3://test/path/123")

  if err != nil {
    t.Error(err)
  }

  if u.Bucket != "test" || u.Key != "path/123" {
    t.Error("Expect bucket to be 'test' and path to be 'path/123'")
  }
}

func TestInvalidS3Location(t *testing.T) {
  u, err := NewS3Location("s3://test")

  if err != nil {
    t.Error(err)
  }

  if u.Key != "" {
    t.Error("Expect key to be emty string")
  }
}
