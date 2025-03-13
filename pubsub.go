package pubsub

import (
	"context"
	"encoding/json"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

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

func (p *PubSubClient) Subscribe(in *SubscribeRequest) {
	defer func() {
		close(in.Errorch)
		close(in.Outch)
	}()
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

func (p *PubSubClient) CreateTopic(topic string) error {
	req := &pb.CreateTopicRequest{
		Name: topic,
	}

	_, err := (*p.clientconn).CreateTopic(context.Background(), req)
	if err != nil {
		return err
	}

	return nil
}

func (p *PubSubClient) CreateSubscription(topic, subscription string) error {
	req := &pb.CreateSubscriptionRequest{
		Topic: topic,
		Name:  subscription,
	}

	_, err := (*p.clientconn).CreateSubscription(context.Background(), req)
	if err != nil {
		return err
	}

	return nil
}
