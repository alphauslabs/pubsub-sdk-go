# (WIP) pubsub-sdk-go

A Go SDK for interacting with internal PubSub service.

## Installation

```bash
$ go get github.com/alphauslabs/pubsub-sdk-go
```

## Usage

### Initialize a client

```go
import "github.com/alphauslabs/pubsub-sdk-go"

func main() {
    client, err := pubsub.New()
    if err != nil {
        // handle error
    }
    defer client.Close()
    
    // use the client
}
```