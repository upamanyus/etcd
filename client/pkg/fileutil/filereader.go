// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileutil

import (
	"bufio"
	"io"
	"io/fs"
	"os"
)

// FileReader is a wrapper of io.Reader. It also provides file info.
type FileReader interface {
	io.Reader
	FileInfo() (fs.FileInfo, error)
}

type fileReader struct {
	*os.File
}

func NewFileReader(f *os.File) FileReader {
	return &fileReader{f}
}

func (fr *fileReader) FileInfo() (fs.FileInfo, error) {
	return fr.Stat()
}

// FileBufReader is a wrapper of bufio.Reader. It also provides file info.
type FileBufReader struct {
	*bufio.Reader
	fi  fs.FileInfo
	err error
}

// NewFileBufReader wraps fr in a buffered reader and takes its file info. If fr.FileInfo fails
// (an fstat error), the error is kept: Err returns it, and FileInfo returns nil.
func NewFileBufReader(fr FileReader) *FileBufReader {
	bufReader := bufio.NewReader(fr)
	fi, err := fr.FileInfo()
	return &FileBufReader{bufReader, fi, err}
}

// FileInfo returns the file info taken by NewFileBufReader, nil if Err is not nil.
func (fbr *FileBufReader) FileInfo() fs.FileInfo {
	return fbr.fi
}

// Err returns the error of taking the file info in NewFileBufReader, if any.
func (fbr *FileBufReader) Err() error {
	return fbr.err
}
