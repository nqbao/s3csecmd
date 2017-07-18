package main

import (
  "os"
)

// Check if the input path is a valid directory for writing
// It is consider writable if one of the condition is true:
// - it does not exist
// - it is a valid folder
// - it is a symlink to a folder
func IsDirWritable(path string) (bool, error) {
  stat, err := os.Stat(path)

  if err != nil {
    if os.IsNotExist(err) {
      return true, nil
    }

    return false, err
  }

  // this is a link then try to follow the link
  if stat.Mode() & os.ModeSymlink == os.ModeSymlink {
    link, lerr := os.Readlink(path)

    if lerr != nil {
      return false, nil
    }

    return IsDirWritable(link)
  } else {
    return stat.IsDir(), nil
  }

  return false, nil
}


func IsFile(path string) (bool, error) {
  stat, err := os.Stat(path)

  if err != nil {
    return false, err
  }

  return !stat.IsDir(), nil
}
