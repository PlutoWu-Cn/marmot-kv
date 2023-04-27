package marmot_kv

import (
	"bufio"
	"os"
	"path/filepath"
)

const (
	FileName      = "marmot.data"
	MergeFileName = "marmot.data.merge"
)

// MarmotFile represents a marmot-kv file.
type MarmotFile struct {
	*os.File
	Offset int64
}

// newInternal opens or creates a file with the given name and returns a *MarmotFile.
func newInternal(fileName string) (*MarmotFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &MarmotFile{File: file, Offset: stat.Size()}, nil
}

// NewMarmotFile returns a new data file with the given path.
func NewMarmotFile(path string) (*MarmotFile, error) {
	fileName := filepath.Join(path, FileName)
	return newInternal(fileName)
}

// NewMergeMarmotFile returns a new merge data file with the given path.
func NewMergeMarmotFile(path string) (*MarmotFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return newInternal(fileName)
}

// Read reads an Entry starting at the given offset.
func (df *MarmotFile) Read(offset int64) (*Entry, error) {
	buf := make([]byte, entryHeaderSize)
	if _, err := df.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	e, err := Decode(buf)
	if err != nil {
		return nil, err
	}

	offset += entryHeaderSize
	if len(e.Key) > 0 {
		key := make([]byte, len(e.Key))
		if _, err = df.ReadAt(key, offset); err != nil {
			return nil, err
		}
		e.Key = key
	}

	offset += int64(len(e.Key))
	if len(e.Value) > 0 {
		value := make([]byte, len(e.Value))
		if _, err = df.ReadAt(value, offset); err != nil {
			return nil, err
		}
		e.Value = value
	}

	return e, nil
}

// Write writes the passed entry into the file.
func (df *MarmotFile) Write(e *Entry) error {
	enc, err := e.Encode()
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(df.File)
	defer func(writer *bufio.Writer) {
		flushErr := writer.Flush()
		if flushErr != nil {
			panic(flushErr)
		}
	}(writer)

	if _, err = writer.Write(enc); err != nil {
		return err
	}

	df.Offset += int64(e.GetSize())
	return nil
}

// Close closes the underlying file.
func (df *MarmotFile) Close() error {
	return df.File.Close()
}
