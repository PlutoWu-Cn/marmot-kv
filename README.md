# marmot-kv

marmot-kv is a key-value database based on Go language, supporting persistent storage and high-efficiency read-write operations.

## Todo

- [x] Basic key-value example
- [x] Advanced Data Storage Structure
- [x] Read and write operations
- [x] Test case
- [ ] Persistence
- [ ] Index
- [ ] CLI Tool

## Features

- Fast: Using memory index and cache to provide efficient read-write operations.
- Persistent: Support persisting data to disk to ensure that data is not lost.
- Scalable: Supports multi-threaded concurrent read-write operations, with good scalability.
- Installation
To start using marmot-kv, install Go and run go get:

```bash
go get github.com/PlutoWu-Cn/marmot-kv
```

## Usage

Here's a simple example of how to use marmot-kv:

```go
package main

import (
"github.com/PlutoWu-Cn/marmot-kv"
)

func main() {
    db, err := kv.Open("mydb")
    if err != nil {
        // Handling errors
    }
    defer db.Close()

    // Insert data
    err = db.Put([]byte("key"), []byte("value"))
    if err != nil {
        // Handling errors
    }

    // Query data
    data, err := db.Get([]byte("key"))
    if err != nil {
        // Handling errors
    }
    fmt.Println(string(data))

    // Delete data
    err = db.Delete([]byte("key"))
    if err != nil {
        // Handling errors
    }
}
```

For more usage examples, please refer to the API documentation.

## Contributing

We welcome contributions to marmot-kv! To contribute code, please follow these steps:

- Fork the project
- Create a new branch (git checkout -b my-new-feature)
- Submit your changes (git commit -am 'Add some feature')
- Push to the branch (git push origin my-new-feature)
- Create a new pull request

## License

marmot-kv is distributed under the MIT License, see the LICENSE file for more information.
