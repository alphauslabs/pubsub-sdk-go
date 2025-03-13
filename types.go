package pubsub

import (
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

type PubSubClient struct {
	conn       *grpc.ClientConn
	clientconn *pb.PubSubServiceClient
}

type PublishRequest struct {
	Topic      string
	Message    string
	Attributes map[string]string
}

type SubscribeRequest struct {
	Topic       string
	Subcription string
	Outch       chan []byte
	Errorch     chan error
}

type SubscribeResponse struct {
	Id         string
	Payload    string
	Attributes map[string]string
}
