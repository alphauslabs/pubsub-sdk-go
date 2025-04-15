package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/golang/glog"
	gaxv2 "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *PubSubClient) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

// New creates a new PubSub client.
func New() (*PubSubClient, error) {
	conn, err := grpc.NewClient("10.146.0.27:50051", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	clientconn := pb.NewPubSubServiceClient(conn)
	return &PubSubClient{conn: conn, clientconn: &clientconn}, nil
}

// Publish a message to a given topic, with retry mechanism.
func (c *PubSubClient) Publish(ctx context.Context, in *PublishRequest) error {
	req := &pb.PublishRequest{
		Topic:      in.Topic,
		Payload:    in.Message,
		Attributes: in.Attributes,
	}

	do := func(iter int) error {
		clientconn := *c.clientconn

		if iter != 0 {
			conn, err := grpc.NewClient("10.146.0.27:50051", grpc.WithInsecure())
			if err != nil {
				return err
			}
			defer conn.Close()

			clientconn = pb.NewPubSubServiceClient(conn)
		}
		_, err := clientconn.Publish(ctx, req)
		if err != nil {
			return err
		}

		return nil
	}

	bo := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}
	limit := in.RetryLimit
	if limit == 0 {
		limit = 10
	}
	var finalErr error

	for i := 0; i < limit; i++ {
		err := do(i)
		if err == nil {
			return nil
		}

		finalErr = err

		shouldRetry := false

		st, ok := status.FromError(err)
		if ok {
			glog.Info("Error: ", st.Code())
			switch st.Code() {
			case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
				shouldRetry = true
			}
		}

		if !shouldRetry || i == limit-1 {
			break
		}

		pauseTime := bo.Pause()
		glog.Errorf("Error: %v, retrying in %v, retries left: %v", err, pauseTime, limit-i-1)
		time.Sleep(pauseTime)
	}

	return finalErr
}

// Subscribes to a subscription. Data will be sent to the Outch, while errors on Errch.
func Subscribe(ctx context.Context, in *SubscribeRequest) {
	defer func() {
		close(in.Errorch)
		close(in.Outch)
	}()

	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subcription,
	}

	do := func() error {
		conn, err := grpc.NewClient("10.146.0.27:50051", grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()

		clientconn := pb.NewPubSubServiceClient(conn)
		stream, err := clientconn.Subscribe(ctx, req)
		if err != nil {
			return err
		}

		for {
			msg, err := stream.Recv()
			if err != nil {
				return err
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

	bo := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}
	limit := 30
	i := 0
	for {
		if i >= limit {
			break
		}
		err := do()
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				glog.Info("Error: ", st.Code())
				if st.Code() == codes.Unavailable {
					i++
					glog.Errorf("Error: %v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i)
					time.Sleep(bo.Pause())
					continue
				}
			}

			if err == io.EOF {
				i++
				glog.Errorf("Stream ended with EOF err=%, retrying in %v, retries left: %v", err, bo.Pause(), limit-i)
				time.Sleep(bo.Pause())
				continue
			}
			in.Errorch <- err
			break
		}
	}
}

// Sends Acknowledgement for a given message, subscriber should call this everyime a message is done processing.
func (p *PubSubClient) SendAck(ctx context.Context, id, subscription, topic string) error {
	_, err := (*p.clientconn).Acknowledge(ctx, &pb.AcknowledgeRequest{Id: id, Subscription: subscription, Topic: topic})
	if err != nil {
		return err
	}

	return nil
}

// Sends Acknowledgement for a given message, with retry mechanism.
func (p *PubSubClient) SendAckWithRetry(ctx context.Context, id, subscription, topic string) error {
	var clientconn pb.PubSubServiceClient
	do := func(iter int) error {
		clientconn = *p.clientconn
		if iter == 2 {
			conn, err := grpc.NewClient("10.146.0.27:50051", grpc.WithInsecure())
			if err != nil {
				return err
			}
			defer conn.Close()
			clientconn = pb.NewPubSubServiceClient(conn)
		}
		_, err := clientconn.Acknowledge(ctx, &pb.AcknowledgeRequest{Id: id, Subscription: subscription, Topic: topic})
		return err
	}
	bo := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}
	limit := 30
	var lastErr error
	for i := 1; i <= limit; i++ {
		err := do(i)
		lastErr = err
		if err == nil {
			return nil
		}
		if st, ok := status.FromError(err); ok {
			glog.Info("Error: ", st.Code())
			switch st.Code() {
			case codes.Unavailable:
				glog.Errorf("Error: %v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i-1)
				time.Sleep(bo.Pause())
				continue
			default:
				return err
			}
		}
		return err
	}
	return lastErr
}

// Creates a new topic with the given name.
func (p *PubSubClient) CreateTopic(ctx context.Context, name string) error {
	req := &pb.CreateTopicRequest{
		Name: name,
	}

	_, err := (*p.clientconn).CreateTopic(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

// Creates a new subscription with the given name and topic, optionally they can set NoAutoExtend to true, but this is not recommended.
// Since PubSub defaults all subscriptions to auto extend.
func (p *PubSubClient) CreateSubscription(ctx context.Context, in *CreateSubscriptionRequest) error {
	req := &pb.CreateSubscriptionRequest{
		Topic:        in.Topic,
		Name:         in.Name,
		NoAutoExtend: in.NoAutoExtend,
	}

	_, err := (*p.clientconn).CreateSubscription(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

// Gets number of messages left in queue for all subscriptions.
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

// Extends the timeout for a given message, this is useful when the subscriber needs more time to process the message.
// The message will be automatically extended if the subscription is created with NoAutoExtend set to false.
func (p *PubSubClient) ExtendTimeout(ctx context.Context, msgId, subscription, topic string) error {
	req := &pb.ExtendVisibilityTimeoutRequest{
		Id:           msgId,
		Subscription: subscription,
		Topic:        topic,
	}

	_, err := (*p.clientconn).ExtendVisibilityTimeout(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
