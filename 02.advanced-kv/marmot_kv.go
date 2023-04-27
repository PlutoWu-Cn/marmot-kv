package marmot_kv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type MarmotKV struct {
	indexes    map[string]int64 // 内存中的索引信息
	marmotFile *MarmotFile      // 数据文件
	dirPath    string           // 数据目录
	mu         sync.RWMutex
}

// Open initializes a MarmotKV instance given a directory path.
func Open(dirPath string) (*MarmotKV, error) {
	// Create the directory if it doesn't exist.
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.Mkdir(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Load the data file.
	marmotFile, err := NewMarmotFile(dirPath)
	if err != nil {
		return nil, err
	}

	// Initialize the MarmotKV instance.
	db := newMarmotKV(marmotFile, dirPath)

	// Load the indexes from file.
	db.loadIndexesFromFile()

	return db, nil
}

// newMarmotKV creates a new MarmotKV instance with default values.
func newMarmotKV(marmotFile *MarmotFile, dirPath string) *MarmotKV {
	return &MarmotKV{
		marmotFile: marmotFile,
		indexes:    make(map[string]int64),
		dirPath:    dirPath,
	}
}

// Merge merge data files
func (db *MarmotKV) Merge() error {
	// no data, ignore
	if db.marmotFile.Offset == 0 {
		return nil
	}

	// Create a channel for the valid entries.
	validEntriesCh := make(chan *Entry)

	// Read entries in parallel and filter them.
	go func() {
		var offset int64
		for {
			e, err := db.marmotFile.Read(offset)
			if err != nil {
				if err == io.EOF {
					close(validEntriesCh)
					break
				}
				// If there's an error reading an entry, send it on the channel
				// as an error value.
				validEntriesCh <- nil
				return
			}
			if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
				validEntriesCh <- e
			}
			offset += int64(e.GetSize())
		}
	}()

	// Create a new merge file.
	mergeMarmotFile, err := NewMergeMarmotFile(db.dirPath)
	if err != nil {
		return err
	}
	defer func() {
		removeErr := os.Remove(mergeMarmotFile.File.Name())
		if removeErr != nil {
			return
		}
		closeErr := mergeMarmotFile.File.Close()
		if closeErr != nil {
			return
		}
	}()

	// Batch writes to the new file.
	var batch []Entry
	for e := range validEntriesCh {
		if e == nil {
			return errors.New("error reading entry")
		}
		batch = append(batch, *e)
		if len(batch) == 1000 {
			if err := db.writeBatch(mergeMarmotFile, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Write any remaining entries.
	if len(batch) > 0 {
		if err := db.writeBatch(mergeMarmotFile, batch); err != nil {
			return err
		}
	}

	// Replace the old data file with the new one.
	db.mu.Lock()
	defer db.mu.Unlock()
	marmotFileName := db.marmotFile.File.Name()
	if err := db.marmotFile.File.Close(); err != nil {
		return err
	}
	if err := os.Rename(mergeMarmotFile.File.Name(), marmotFileName); err != nil {
		return err
	}
	db.marmotFile = mergeMarmotFile

	return nil
}

// writeBatch writes a batch of entries to the merge file and updates the indexes.
func (db *MarmotKV) writeBatch(mergeMarmotFile *MarmotFile, batch []Entry) error {
	writeOff := mergeMarmotFile.Offset
	for _, entry := range batch {
		if err := mergeMarmotFile.Write(&entry); err != nil {
			return err
		}
		db.indexes[string(entry.Key)] = writeOff
		writeOff += int64(entry.GetSize())
	}
	return nil
}

// Put data input
func (db *MarmotKV) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.marmotFile.Offset
	// Encapsulated as Entry
	entry := NewEntry(key, value, PUT)
	// Append to the data file
	err = db.marmotFile.Write(entry)

	// Write to memory
	db.indexes[string(key)] = offset
	return
}

// Get fetch data
func (db *MarmotKV) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// Retrieve index information from memory
	offset, ok := db.indexes[string(key)]
	// Key doesn't exist
	if !ok {
		err = fmt.Errorf("{key: %s doesn't exist.}", key)
		return
	}

	// Read data from disk
	var e *Entry
	e, err = db.marmotFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del delete data
func (db *MarmotKV) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// Retrieve index information from memory
	_, ok := db.indexes[string(key)]
	// Key doesn't exist, ignore
	if !ok {
		return
	}

	// Encapsulate into Entry and write
	e := NewEntry(key, nil, DEL)
	err = db.marmotFile.Write(e)
	if err != nil {
		return
	}

	// Delete key from memory
	delete(db.indexes, string(key))
	return
}

// Load index from file
func (db *MarmotKV) loadIndexesFromFile() {
	if db.marmotFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.marmotFile.Read(offset)
		if err != nil {
			// Finished reading
			if err == io.EOF {
				break
			}
			return
		}

		// Set index status
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// Delete key from memory
			delete(db.indexes, string(e.Key))
		}

		offset += int64(e.GetSize())
	}
	return
}

// Close shut down the marmot-kv instance
func (db *MarmotKV) Close() error {
	if db.marmotFile == nil {
		return errors.New("invalid marmotFile")
	}

	return db.marmotFile.File.Close()
}
