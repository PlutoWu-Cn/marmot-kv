package marmot_kv

import (
	"encoding/binary"
	"errors"
)

const entryHeaderSize = 10

const (
	PUT uint16 = iota
	DEL
)

// Entry represents a record written to the file
type Entry struct {
	Key   []byte
	Value []byte
	Mark  uint16
}

// NewEntry creates a new Entry
func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Mark:  mark,
	}
}

// GetSize returns the size of the encoded Entry
func (e *Entry) GetSize() int {
	keyLen := len(e.Key)
	valLen := len(e.Value)
	return entryHeaderSize + keyLen + valLen
}

// Encode encodes the Entry and returns a byte slice
func (e *Entry) Encode() ([]byte, error) {
	keySize := uint32(len(e.Key))
	valSize := uint32(len(e.Value))
	buf := make([]byte, 0, entryHeaderSize+keySize+valSize)
	buf = append(buf, make([]byte, entryHeaderSize)...)
	binary.BigEndian.PutUint32(buf[:4], keySize)
	binary.BigEndian.PutUint32(buf[4:8], valSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	buf = append(buf, e.Key...)
	buf = append(buf, e.Value...)
	return buf, nil
}

// Decode decodes the provided byte slice into an Entry
func Decode(buf []byte) (*Entry, error) {
	if len(buf) < entryHeaderSize {
		return nil, errors.New("insufficient buffer length")
	}
	keySize := binary.BigEndian.Uint32(buf[:4])
	valSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	entryLength := entryHeaderSize + keySize + valSize
	if len(buf) != int(entryLength) {
		return nil, errors.New("invalid buffer length")
	}
	return &Entry{
		Key:   buf[entryHeaderSize : entryHeaderSize+keySize],
		Value: buf[entryHeaderSize+keySize : entryLength],
		Mark:  mark,
	}, nil
}
