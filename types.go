package pubsub

import (
	"log"
	"sync"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

type PubSubClient struct {
	conns      map[string]*grpc.ClientConn
	clientconn *pb.PubSubServiceClient
	logger     *log.Logger
	mu         sync.Mutex
}

type PublishRequest struct {
	Topic      string
	Message    string
	Attributes map[string]string

	// RetryLimit is the max number of times to retry publishing a message. Default is 10.
	RetryLimit int
}

type MessageCallback func(ctx any, data []byte) error

type SubscribeAndAckRequest struct {
	Topic        string
	Subscription string
	Ctx          any // arbitrary data passed to callback
	Callback     MessageCallback
}

type SubscribeRequest struct {
	Topic        string
	Subscription string
	Errch        chan error
	Outch        chan []byte
}

type SubscribeResponse struct {
	Id         string
	Payload    string
	Attributes map[string]string
}

type CreateSubscriptionRequest struct {
	Topic        string
	Name         string
	NoAutoExtend bool // if true, messages will not be auto extended.
}

type GetNumberOfMessagesResponse struct {
	Subscription             string
	CurrentMessagesAvailable int32
}

type Requeuer interface {
	ShouldRequeue() bool
}

type RequeueError struct {
	error
	requeue bool
}

func (r RequeueError) ShouldRequeue() bool {
	return r.requeue
}

func NewRequeueError(err error) RequeueError {
	return RequeueError{
		error:   err,
		requeue: true,
	}
}
