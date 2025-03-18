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
	conn, err := grpc.Dial("10.146.0.27:50051", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	clientconn := pb.NewPubSubServiceClient(conn)
	return &PubSubClient{conn: conn, clientconn: &clientconn}, nil
}

func (p *PubSubClient) Publish(ctx context.Context, in *PublishRequest) error {
	// Create a new PublishRequest
	req := &pb.PublishRequest{
		Topic:      in.Topic,
		Payload:    in.Message,
		Attributes: in.Attributes,
	}

	_, err := (*p.clientconn).Publish(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (p *PubSubClient) Subscribe(ctx context.Context, in *SubscribeRequest) {
	defer func() {
		close(in.Errorch)
		close(in.Outch)
	}()

	// Create a new SubscribeRequest
	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subcription,
	}

	stream, err := (*p.clientconn).Subscribe(ctx, req)
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

func (p *PubSubClient) SendAck(ctx context.Context, id, subscription string) error {
	_, err := (*p.clientconn).Acknowledge(ctx, &pb.AcknowledgeRequest{Id: id, Subscription: subscription})
	if err != nil {
		return err
	}

	return nil
}

func (p *PubSubClient) CreateTopic(ctx context.Context, topic string) error {
	req := &pb.CreateTopicRequest{
		Name: topic,
	}

	_, err := (*p.clientconn).CreateTopic(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (p *PubSubClient) CreateSubscription(ctx context.Context, in *CreateSubscriptionRequest) error {
	req := &pb.CreateSubscriptionRequest{
		Topic:        in.Topic,
		Name:         in.Subscription,
		NoAutoExtend: in.NoAutoExtend,
	}

	_, err := (*p.clientconn).CreateSubscription(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (p *PubSubClient) GetNumberOfMessages(ctx context.Context, topic string) ([]*GetNumberOfMessagesResponse, error) {
	res, err := (*p.clientconn).GetMessagesInQueue(ctx, &pb.GetMessagesInQueueRequest{})
	if err != nil {
		return nil, err
	}

	ret := make([]*GetNumberOfMessagesResponse, 0)
	for _, q := range res.InQueue {
		ret = append(ret, &GetNumberOfMessagesResponse{
			Subscription:             q.Subscription,
			CurrentMessagesAvailable: q.Total,
		})
	}

	return ret, nil
}
