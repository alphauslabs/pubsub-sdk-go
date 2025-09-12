
Go SDK for interacting with internal PubSub service.

## Installation

```bash
$ go get github.com/alphauslabs/pubsub-sdk-go@latest
```

### Initialize a client

```go
import "github.com/alphauslabs/pubsub-sdk-go"

func main() {
    client, err := pubsub.New()
    if err != nil {
        return
    }
    defer client.Close()
    // use the client
}
```

