package pubsub

import (
	"log"
	"sync"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

type PubSubClient struct {
	conns        map[string]*grpc.ClientConn
	clientconn   *pb.PubSubServiceClient
	logger       *log.Logger
	mu           sync.Mutex
	lastAddrUsed string
}

type PublishRequest struct {
	Topic      string
	Message    string
	Attributes map[string]string
}

type MessageCallback func(ctx any, data []byte) error

type StartRequest struct {
	Subscription string
	Ctx          any // arbitrary data passed to callback
	Callback     MessageCallback
}

type SubscribeRequest struct {
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
	Topic      string
	Name       string
	AutoExtend bool // if true, messages will be auto extended.
}

type GetNumberOfMessagesResponse struct {
	Subscription             string
	CurrentMessagesAvailable int32
}

type SubscriptionInfo struct {
	Name         string
	Topic        string
	IsAutoExtend bool
}

type TopicInfo struct {
	Name          string
	Subscriptions []string
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
