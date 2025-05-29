package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/dchest/uniuri"
	"github.com/golang/glog"
	gaxv2 "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (p *PubSubClient) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

// New creates a new PubSub client.
func New(addr ...string) (*PubSubClient, error) {
	aud := "35.213.109.125:50051"
	if len(addr) > 0 {
		if addr[0] != "" {
			aud = addr[0]
		}
	}
	token, err := idtoken.NewTokenSource(context.Background(), aud)
	if err != nil {
		return nil, err
	}

	tk, err := token.Token()
	if err != nil {
		return nil, err
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context,
		method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.AccessToken)
		return invoker(ctx, method, req, reply, cc, opts...)
	}))

	opts = append(opts, grpc.WithStreamInterceptor(func(ctx context.Context,
		desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.AccessToken)
		return streamer(ctx, desc, cc, method, opts...)
	}))

	conn, err := grpc.NewClient(aud, opts...)
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
			conn, err := New()
			if err != nil {
				return err
			}
			clientconn = *conn.clientconn
			defer conn.Close()
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

// Subscribes and Acknowledge the message after processing through the provided callback.
// Cancelled by ctx, and will send empty struct to done if provided.
func SubscribeAndAck(ctx context.Context, in *SubscribeAndAckRequest, done ...chan struct{}) error {
	if in.Callback == nil {
		return fmt.Errorf("Callback sould not be nil")
	}

	if in.Topic == "" {
		return fmt.Errorf("Topic should not be empty")
	}

	if in.Subcription == "" {
		return fmt.Errorf("Subscription should not be empty")
	}

	localId := uniuri.NewLen(10)
	log.Printf("Started=%v, time=%v", localId, time.Now().Format(time.RFC3339))
	defer func(start time.Time) {
		log.Printf("Stopped=%v, duration=%v", localId, time.Since(start))
		if len(done) > 0 {
			done[0] <- struct{}{}
		}
	}(time.Now())

	client, err := New()
	if err != nil {
		return err
	}
	defer client.Close()

	clientconn := *client.clientconn
	r, err := clientconn.GetSubscription(ctx, &pb.GetSubscriptionRequest{
		Name: in.Subcription,
	})
	if err != nil {
		return err
	}

	autoExtend := r.Subscription.AutoExtend

	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subcription,
	}

	do := func(addr string) error {
		conn, err := New(addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		clientconn := *conn.clientconn
		stream, err := clientconn.Subscribe(ctx, req)
		if err != nil {
			return err
		}

		// Loop for receiving messages
		for {
			msg, err := stream.Recv()
			if err != nil {
				return err
			}
			switch {
			case autoExtend:
				ack := true
				err = in.Callback(in.Ctx, []byte(msg.Payload)) // This could take some time depending on the callback.
				if err != nil {
					if r, ok := err.(Requeuer); ok {
						if r.ShouldRequeue() {
							log.Printf("Requeueing message=%v", msg.Id)
							ack = false
							// Explicitly requeue the message, since this subscription is set to autoextend.
							_, err = clientconn.RequeueMessage(ctx, &pb.RequeueMessageRequest{
								Topic:        in.Topic,
								Subscription: in.Subcription,
								Id:           msg.Id,
							})
							if err != nil {
								log.Printf("RequeueMessage failed: %v", err)
							}
						}
					} else {
						log.Printf("Callback error: %v", err)
					}
				}
				if ack {
					err = client.SendAckWithRetry(ctx, msg.Id, in.Subcription, in.Topic)
					if err != nil {
						log.Printf("Ack error: %v", err)
					} else {
						log.Printf("Acked message %s", msg.Id)
					}
				}
			default: // autoextend for this subscription is set to false, We manually extend it's timeout before timeout ends, We repeat this until the callback returns
				fdone := make(chan struct{})
				extender, cancel := context.WithCancel(ctx)
				t := time.NewTicker(20 * time.Second)
				go func() {
					defer func() {
						close(fdone)
						t.Stop()
					}()
					for {
						select {
						case <-extender.Done():
							return
						case <-t.C:
							// Reset timeout for this message
							_, err := clientconn.ExtendVisibilityTimeout(ctx, &pb.ExtendVisibilityTimeoutRequest{
								Topic:        in.Topic,
								Subscription: in.Subcription,
								Id:           msg.Id,
							})
							if err != nil { // If failed, it means the message would be requeued.
								log.Printf("ExtendVisibilityTimeout failed: %v", err)
							} else {
								log.Printf("Extended timeout for message %s", msg.Id)
							}
						}
					}
				}()
				ack := true
				err = in.Callback(in.Ctx, []byte(msg.Payload)) // This could take some time depending on the callback.
				if err != nil {
					if r, ok := err.(Requeuer); ok {
						if r.ShouldRequeue() {
							log.Printf("Requeueing message=%v", msg.Id)
							ack = false
						}
					} else {
						log.Printf("Callback error: %v", err)
					}
				}
				if ack {
					err = client.SendAckWithRetry(ctx, msg.Id, in.Subcription, in.Topic)
					if err != nil {
						log.Printf("Ack error: %v", err)
					} else {
						log.Printf("Acked message %s", msg.Id)
					}
				}

				cancel() // Signal our extender that callback is done
				<-fdone  // Wait for our extender
			}
		}
	}

	bo := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}
	limit := 30
	i := 0
	var ferr error
	var address string
	// Loop for retry
	for {
		if i >= limit {
			break
		}
		err := do(address)
		ferr = err
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				glog.Info("Error: ", st.Code())
				if st.Code() == codes.Unavailable {
					i++
					address = ""
					glog.Errorf("Error: %v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i)
					time.Sleep(bo.Pause())
					continue
				}
			}
			if strings.Contains(err.Error(), "wrongnode") {
				node := strings.Split(err.Error(), "|")[1]
				address = node
				i++
				glog.Errorf("Stream ended with wrongnode err=%v, retrying in %v, retries left: %v", err.Error(), bo.Pause(), limit-i)
				continue // retry immediately
			}
			if err == io.EOF {
				i++
				address = ""
				glog.Errorf("Stream ended with EOF err=%v, retrying in %v, retries left: %v", err.Error(), bo.Pause(), limit-i)
				time.Sleep(bo.Pause())
				continue
			}
			break
		}
	}
	return ferr
}

// Note: Better to use SubscribeAndAck instead.
// Subscribes to a subscription. Data will be sent to the Outch, while errors on Errch.
func Subscribe(ctx context.Context, in *SubscribeRequest) {
	defer func() {
		close(in.Errch)
		close(in.Outch)
	}()

	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subcription,
	}
	var clientconn pb.PubSubServiceClient
	do := func(addr string) error {
		conn, err := New(addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		clientconn = *conn.clientconn
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

	var address string
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
		err := do(address)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				glog.Info("Error: ", st.Code())
				if st.Code() == codes.Unavailable {
					i++
					address = ""
					glog.Errorf("Error: %v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i)
					time.Sleep(bo.Pause())
					continue
				}
			}
			if strings.Contains(err.Error(), "wrongnode") {
				node := strings.Split(err.Error(), "|")[1]
				address = node
				i++
				glog.Errorf("Stream ended with wrongnode err=%v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i)
				continue // retry immediately
			}
			if err == io.EOF {
				i++
				address = ""
				glog.Errorf("Stream ended with EOF err=%v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i)
				time.Sleep(bo.Pause())
				continue
			}
			in.Errch <- err
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
	do := func(addr string) error {
		conn, err := New(addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		clientconn := *conn.clientconn
		_, err = clientconn.Acknowledge(ctx, &pb.AcknowledgeRequest{Id: id, Subscription: subscription, Topic: topic})
		return err
	}
	bo := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}
	var address string
	limit := 30
	var lastErr error
	for i := 1; i <= limit; i++ {
		err := do(address)
		lastErr = err
		if err == nil {
			return nil
		}
		if st, ok := status.FromError(err); ok {
			glog.Info("Error: ", st.Code())
			if st.Code() == codes.Unavailable {
				address = ""
				glog.Errorf("Error: %v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i-1)
				time.Sleep(bo.Pause())
				continue
			}
		}
		if strings.Contains(err.Error(), "wrongnode") {
			node := strings.Split(err.Error(), "|")[1]
			address = node
			glog.Errorf("Stream ended with wrongnode err=%v, retrying in %v, retries left: %v", err, bo.Pause(), limit-i-1)
			continue // retry immediately
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
// Filter is optional, fmt: filter[0] = topic, filter[1] = subscription
func (p *PubSubClient) GetNumberOfMessages(ctx context.Context, filter ...string) ([]*GetNumberOfMessagesResponse, error) {
	var topic, subscription string
	switch len(filter) {
	case 1:
		topic = filter[0]
	case 2:
		topic = filter[0]
		subscription = filter[1]
	}
	res, err := (*p.clientconn).GetMessagesInQueue(ctx, &pb.GetMessagesInQueueRequest{
		Topic:        topic,
		Subscription: subscription,
	})
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
