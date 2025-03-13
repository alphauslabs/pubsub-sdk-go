package pubsub

import (
	"context"
	"encoding/json"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

type PubSubClient struct {
	conn       *grpc.ClientConn
	clientconn *pb.PubSubServiceClient
}

func (p *PubSubClient) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

func New() (*PubSubClient, error) {
	conn, err := grpc.Dial(":50051", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	clientconn := pb.NewPubSubServiceClient(conn)
	return &PubSubClient{conn: conn, clientconn: &clientconn}, nil
}

type PublishRequest struct {
	Topic      string
	Message    string
	Attributes map[string]string
}

func (p *PubSubClient) Publish(in *PublishRequest) error {
	// Create a new PublishRequest
	req := &pb.PublishRequest{
		Topic:      in.Topic,
		Payload:    in.Message,
		Attributes: in.Attributes,
	}

	_, err := (*p.clientconn).Publish(context.Background(), req)
	if err != nil {
		return err
	}

	return nil
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

func (p *PubSubClient) Subscribe(in *SubscribeRequest) {
	// Create a new SubscribeRequest
	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subcription,
	}

	stream, err := (*p.clientconn).Subscribe(context.Background(), req)
	if err != nil {
		in.Errorch <- err
		return
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			in.Errorch <- err
			return
		}
		res := &SubscribeResponse{
			Id:         msg.Id,
			Payload:    msg.Payload,
			Attributes: msg.Attributes,
		}
		b, _ := json.Marshal(res)
		in.Outch <- b
	}
}
