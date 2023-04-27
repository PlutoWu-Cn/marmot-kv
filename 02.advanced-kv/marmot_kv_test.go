package marmot_kv

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	// Create a temporary directory.
	dirPath, err := ioutil.TempDir("", "marmot_kv-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dirPath)

	// Open a new MarmotKV instance.
	db, err := Open(dirPath)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the MarmotKV instance was created with the correct values.
	if db.dirPath != dirPath {
		t.Errorf("db.dirPath = %v; want %v", db.dirPath, dirPath)
	}
	if db.marmotFile == nil {
		t.Error("db.marmotFile is nil")
	}
	if db.indexes == nil {
		t.Error("db.indexes is nil")
	}

	// Close the MarmotKV instance.
	if err := db.Close(); err != nil {
		t.Error(err)
	}
}

func TestPutAndGet(t *testing.T) {
	// Create a temporary directory.
	dirPath, err := ioutil.TempDir("", "marmot_kv-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dirPath)

	// Open a new MarmotKV instance.
	db, err := Open(dirPath)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a key-value pair.
	key := []byte("hello")
	value := []byte("world")
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}

	// Retrieve the value for the key.
	val, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the retrieved value matches the inserted value.
	if !bytes.Equal(val, value) {
		t.Errorf("val = %v; want %v", val, value)
	}

	// Close the MarmotKV instance.
	if err := db.Close(); err != nil {
		t.Error(err)
	}
}

func TestDel(t *testing.T) {
	// Create a temporary directory.
	dirPath, err := ioutil.TempDir("", "marmot_kv-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dirPath)

	// Open a new MarmotKV instance.
	db, err := Open(dirPath)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a key-value pair.
	key := []byte("hello")
	value := []byte("world")
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}

	// Delete the key-value pair.
	if err := db.Del(key); err != nil {
		t.Fatal(err)
	}

	// Retrieve the value for the key.
	val, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the retrieved value is nil.
	if val != nil {
		t.Errorf("val = %v; want nil", val)
	}

	// Close the MarmotKV instance.
	if err := db.Close(); err != nil {
		t.Error(err)
	}
}

func TestMerge(t *testing.T) {
	// Create a temporary directory.
	dirPath, err := ioutil.TempDir("", "marmot_kv-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dirPath)

	// Open a new MarmotKV instance.
	db, err := Open(dirPath)
	if err != nil {
		t.Fatal(err)
	}

	// Insert some key-value pairs.
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Delete some key-value pairs.
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Del(key); err != nil {
			t.Fatal(err)
		}
	}

	// Merge the data files.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	// Ensure the MarmotKV instance still works after the merge.
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		if i < 1000 && val != nil {
			t.Errorf("val = %v; want nil", val)
		} else if i >= 1000 && !bytes.Equal(val, expectedValue) {
			t.Errorf("val = %v; want %v", val, expectedValue)
		}
	} // Close the MarmotKV instance.
	if err := db.Close(); err != nil {
		t.Error(err)
	}
}
