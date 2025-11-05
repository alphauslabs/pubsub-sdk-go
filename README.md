
Go SDK for interacting with the internal PubSub service.

### Usage

```bash
$ go get github.com/alphauslabs/pubsub-sdk-go@latest
```

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

