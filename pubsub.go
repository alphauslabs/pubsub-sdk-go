package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/dchest/uniuri"
	gaxv2 "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (p *PubSubClient) Close() {
	for _, p := range p.conns {
		p.Close()
	}
}

type Option interface {
	Apply(*PubSubClient)
}

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(pbclient *PubSubClient) { pbclient.logger = w.l }

// WithLogger sets PubSubClient's logger object. Can be silenced by setting v to:
//
//	log.New(io.Discard, "", 0)
func WithLogger(v *log.Logger) Option { return withLogger{v} }

// New creates a new PubSub client.
func New(options ...Option) (*PubSubClient, error) {
	client := &PubSubClient{}
	addr := "35.213.109.125:50051" // default
	if len(options) > 0 {
		for _, opt := range options {
			opt.Apply(client)
		}
	}

	if client.logger == nil {
		client.logger = log.New(os.Stdout, "[pubsub-internal] ", log.Ldate|log.Ltime|log.Lshortfile)
	}

	token, err := idtoken.NewTokenSource(context.Background(), addr)
	if err != nil {
		return nil, err
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context,
		method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tk, err := token.Token()
		if err == nil {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.AccessToken)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))

	opts = append(opts, grpc.WithStreamInterceptor(func(ctx context.Context,
		desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {
		tk, err := token.Token()
		if err == nil {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.AccessToken)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}))

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.conns == nil {
		client.conns = make(map[string]*grpc.ClientConn)
	}
	client.conns[addr] = conn
	clientconn := pb.NewPubSubServiceClient(conn)
	client.clientconn = &clientconn

	return client, nil
}

// Publish a message to a given topic, with retry mechanism.
func (c *PubSubClient) Publish(ctx context.Context, in *PublishRequest) error {
	req := &pb.PublishRequest{
		Topic:      in.Topic,
		Payload:    in.Message,
		Attributes: in.Attributes,
	}

	do := func() error {
		conn, err := New()
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = (*conn.clientconn).Publish(ctx, req)
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

	for {
		err := do()
		if err == nil {
			return nil
		}

		shouldRetry := false
		st, ok := status.FromError(err)
		if ok {
			c.logger.Printf("Error: %v", st.Code())
			switch st.Code() {
			case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
				shouldRetry = true
			}
		}

		if !shouldRetry {
			return err
		}

		pauseTime := bo.Pause()
		c.logger.Printf("Error: %v, retrying in %v", err, pauseTime)
		time.Sleep(pauseTime)
	}
}

// Subscribes and Acknowledge the message after processing through the provided callback.
// Cancelled by ctx, and will send empty struct to done if provided.
func (p *PubSubClient) Start(quit context.Context, in *SubscribeAndAckRequest, done ...chan struct{}) error {
	if in.Callback == nil {
		return fmt.Errorf("callback sould not be nil")
	}

	if in.Topic == "" {
		return fmt.Errorf("topic should not be empty")
	}

	if in.Subscription == "" {
		return fmt.Errorf("subscription should not be empty")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-quit.Done()
		cancel()
	}()

	localId := uniuri.NewLen(10)
	p.logger.Printf("Started=%v, time=%v", localId, time.Now().Format(time.RFC3339))
	defer func(start time.Time) {
		p.logger.Printf("Stopped=%v, duration=%v", localId, time.Since(start))
		if len(done) > 0 {
			done[0] <- struct{}{}
		}
	}(time.Now())

	r, err := (*p.clientconn).GetSubscription(ctx, &pb.GetSubscriptionRequest{
		Name: in.Subscription,
	})
	if err != nil {
		return err
	}

	autoExtend := r.Subscription.AutoExtend
	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subscription,
	}

	do := func(addr string) error {
		pbclient, err := p.getClient(addr)
		if err != nil {
			return err
		}
		stream, err := (*pbclient.clientconn).Subscribe(ctx, req)
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
							pbclient.logger.Printf("Requeueing message=%v", msg.Id)
							ack = false
							// Explicitly requeue the message, since this subscription is set to autoextend.
							err = pbclient.RequeueMessage(ctx, msg.Id, in.Subscription, in.Topic)
							if err != nil {
								pbclient.logger.Printf("RequeueMessage failed: %v", err)
							}
						}
					} else {
						pbclient.logger.Printf("Callback error: %v", err)
					}
				}
				if ack {
					err = pbclient.SendAckWithRetry(ctx, msg.Id, in.Subscription, in.Topic)
					if err != nil {
						pbclient.logger.Printf("Ack error: %v", err)
					} else {
						pbclient.logger.Printf("Acked message %s", msg.Id)
					}
				}
			default: // autoextend for this subscription is set to false, We manually extend it's timeout before timeout ends, We repeat this until the callback returns
				fdone := make(chan struct{})
				extender, cancel := context.WithCancel(ctx)
				t := time.NewTicker(10 * time.Second)
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
							err := pbclient.ExtendMessageTimeout(ctx, msg.Id, in.Subscription, in.Topic)
							if err != nil {
								pbclient.logger.Printf("ExtendVisibilityTimeout failed: %v", err)
							} else {
								pbclient.logger.Printf("Extended timeout for message %s", msg.Id)
							}
						}
					}
				}()
				ack := true
				err = in.Callback(in.Ctx, []byte(msg.Payload)) // This could take some time depending on the callback.
				if err != nil {
					if r, ok := err.(Requeuer); ok {
						if r.ShouldRequeue() {
							pbclient.logger.Printf("Requeueing message=%v", msg.Id)
							ack = false
						}
					} else {
						pbclient.logger.Printf("Callback error: %v", err)
					}
				}
				if ack {
					err = pbclient.SendAckWithRetry(ctx, msg.Id, in.Subscription, in.Topic)
					if err != nil {
						pbclient.logger.Printf("Ack error: %v", err)
					} else {
						pbclient.logger.Printf("Acked message %s", msg.Id)
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
	var address string
	// Loop for retry
	for {
		err := do(address)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				if st.Code() == codes.Unavailable {
					address = ""
					p.logger.Printf("Error: %v, retrying in %v", err, bo.Pause())
					time.Sleep(bo.Pause())
					continue
				}

				if st.Code() == codes.Canceled {
					break
				}
			}
			if strings.Contains(err.Error(), "wrongnode") {
				node := strings.Split(err.Error(), "|")[1]
				address = node
				p.logger.Printf("Stream ended with wrongnode err=%v, retrying in %v", err.Error(), bo.Pause())
				continue // retry immediately
			}
			if err == io.EOF {
				address = ""
				p.logger.Printf("Stream ended with EOF err=%v, retrying in %v", err.Error(), bo.Pause())
				time.Sleep(bo.Pause())
				continue
			}
			return err
		}
		break
	}

	return nil
}

// Note: Better to use Start instead.
// Subscribes to a subscription. Data will be sent to the Outch, while errors on Errch.
func (pbclient *PubSubClient) Subscribe(ctx context.Context, in *SubscribeRequest) {
	defer func() {
		close(in.Errch)
		close(in.Outch)
	}()
	req := &pb.SubscribeRequest{
		Topic:        in.Topic,
		Subscription: in.Subscription,
	}
	do := func(addr string) error {
		pbclient, err := pbclient.getClient(addr)
		if err != nil {
			return err
		}

		stream, err := (*pbclient.clientconn).Subscribe(ctx, req)
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
		Max:     20 * time.Second,
	}

	for {
		err := do(address)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				if st.Code() == codes.Unavailable {
					address = ""
					pbclient.logger.Printf("Error: %v, retrying in %v", err, bo.Pause())
					time.Sleep(bo.Pause())
					continue
				}
			}
			if strings.Contains(err.Error(), "wrongnode") {
				node := strings.Split(err.Error(), "|")[1]
				address = node
				pbclient.logger.Printf("Stream ended with wrongnode err=%v, retrying in %v", err, bo.Pause())
				continue // retry immediately
			}
			if err == io.EOF {
				address = ""
				pbclient.logger.Printf("Stream ended with EOF err=%v, retrying in %v", err, bo.Pause())
				time.Sleep(bo.Pause())
				continue
			}
			in.Errch <- err
			break
		} else {
			in.Errch <- nil
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
	pbclient := p
	var address string

	bo := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     20 * time.Second,
	}

	limit := 30
	i := 0
	var err error
	for range limit {
		pbclient, err = p.getClient(address)
		if err != nil {
			return fmt.Errorf("failed to create connection: %w", err)
		}

		if pbclient.clientconn == nil {
			return fmt.Errorf("client connection is nil")
		}

		// Try to acknowledge
		_, err = (*pbclient.clientconn).Acknowledge(ctx, &pb.AcknowledgeRequest{
			Id:           id,
			Subscription: subscription,
			Topic:        topic,
		})

		if err == nil {
			return nil
		}
		i++
		// Handle errors and retry logic
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.Unavailable {
				address = ""
				p.logger.Printf("Error: %v, retrying in %v, id=%v, sub=%v, retry=%v", err, bo.Pause(), id, subscription, i)
				time.Sleep(bo.Pause())
				continue
			}
		}

		if strings.Contains(err.Error(), "wrongnode") {
			p.logger.Printf("Wrong node: %v", err)
			address = strings.Split(err.Error(), "|")[1]
			continue
		}

		return err
	}

	return err
}

// Creates a new topic with the given name, this function can be called multiple times, if the topic already exists it will just return nil.
func (p *PubSubClient) CreateTopic(ctx context.Context, name string) error {
	req := &pb.CreateTopicRequest{
		Name: name,
	}

	_, err := (*p.clientconn).CreateTopic(ctx, req)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "alreadyexists") {
			return nil
		}
		return err
	}

	return nil
}

// Creates a new subscription with the given name and topic, optionally they can set NoAutoExtend to true, but this is not recommended.
// Since PubSub defaults all subscriptions to auto extend.
// This function can be called multiple times, if the subscription already exists it will just return nil.
func (p *PubSubClient) CreateSubscription(ctx context.Context, in *CreateSubscriptionRequest) error {
	req := &pb.CreateSubscriptionRequest{
		Topic:        in.Topic,
		Name:         in.Name,
		NoAutoExtend: in.NoAutoExtend,
	}

	_, err := (*p.clientconn).CreateSubscription(ctx, req)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "alreadyexists") {
			return nil
		}
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
func (p *PubSubClient) ExtendMessageTimeout(ctx context.Context, msgId, subscription, topic string) error {
	do := func(addr string) error {
		pbclient, err := p.getClient(addr)
		if err != nil {
			return err
		}
		_, err = (*pbclient.clientconn).ExtendVisibilityTimeout(ctx, &pb.ExtendVisibilityTimeoutRequest{
			Id:           msgId,
			Subscription: subscription,
			Topic:        topic,
		})
		return err
	}

	backoff := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}

	var address string
	for {
		err := do(address)
		if err == nil {
			break
		}

		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.Unavailable {
				address = ""
				btime := backoff.Pause() // backoff time
				p.logger.Printf("Error: %v, retrying in %v", err, btime)
				time.Sleep(btime)
				continue
			}
		}

		if strings.Contains(strings.ToLower(err.Error()), "wrongnode") {
			correctNode := strings.Split(err.Error(), "|")[1]
			address = correctNode
			p.logger.Printf("Error: %v retrying..", err)
			continue
		}

		return err
	}

	return nil
}

// Attempt to requeue a message, with retry mechanism.
func (p *PubSubClient) RequeueMessage(ctx context.Context, msgId, subscription, topic string) error {
	do := func(addr string) error {
		pbclient, err := p.getClient(addr)
		if err != nil {
			return err
		}
		_, err = (*pbclient.clientconn).RequeueMessage(ctx, &pb.RequeueMessageRequest{
			Id:           msgId,
			Subscription: subscription,
			Topic:        topic,
		})
		return err
	}

	backoff := gaxv2.Backoff{
		Initial: 5 * time.Second,
		Max:     1 * time.Minute,
	}
	var address string
	for {
		err := do(address)
		if err == nil {
			break
		}

		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.Unavailable {
				address = ""
				btime := backoff.Pause() // backoff time
				p.logger.Printf("Error: %v, retrying in %v", err, btime)
				time.Sleep(btime)
				continue
			}
		}

		if strings.Contains(strings.ToLower(err.Error()), "wrongnode") {
			correctNode := strings.Split(err.Error(), "|")[1]
			address = correctNode
			p.logger.Printf("Error: %v retrying..", err)
			continue
		}
		return err
	}

	return nil
}

func (p *PubSubClient) getClient(addr string) (*PubSubClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conns == nil {
		p.conns = make(map[string]*grpc.ClientConn)
	}

	if addr == "" {
		addr = "35.213.109.125:50051" // default
	}

	if c, ok := p.conns[addr]; ok { // no need to dial again
		clientconn := pb.NewPubSubServiceClient(c)
		p.clientconn = &clientconn
		return p, nil
	}

	token, err := idtoken.NewTokenSource(context.Background(), addr)
	if err != nil {
		return nil, err
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context,
		method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tk, err := token.Token()
		if err == nil {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.AccessToken)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))

	opts = append(opts, grpc.WithStreamInterceptor(func(ctx context.Context,
		desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {
		tk, err := token.Token()
		if err == nil {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.AccessToken)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}))
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	p.conns[addr] = conn
	clientconn := pb.NewPubSubServiceClient(conn)
	p.clientconn = &clientconn
	return p, nil
}
