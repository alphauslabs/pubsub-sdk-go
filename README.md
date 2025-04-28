
Go SDK for interacting with internal PubSub service. **(WIP)** 

## Installation

```bash
$ go get github.com/alphauslabs/pubsub-sdk-go@latest
```

## Simple usage

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
## Available functions

1. **Publish** - Publishes a message to the given topic (with retry).
2. **Subscribe** - Subscribes to a subcription and receives messages.
3. **SendAck** - Acknowledge a message.
4. **SendAckWithRetry** - Acknowledges a message (with retry).
5. **CreateTopic** - Creates a new topic.
6. **CreateSubscription** - Creates a new subscription.
7. **GetNumberOfMessages** - Gets the number of available message in all subscriptions.

8. **ExtendTimeout** - Extends the timeout of a message.