package subscriber

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sheffer-ido/pubsub-worker/com/logger"
	pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"
	"google.golang.org/grpc"
)

var log *logger.Logger

type MsgClass struct {
	Request        *pb.Request
	responseServer pb.StreamService_SendRequestServer
	waitgroup      sync.WaitGroup
}

func (m *MsgClass) Respond(result string, body []byte) error {
	err := m.responseServer.Send(
		&pb.Response{
			RequestId: m.Request.Id,
			Result:    result,
			Data:      body,
			Time:      time.Now().Unix(),
		},
	)
	m.waitgroup.Done()
	return err

}

type Subscriber struct {
	Ctx       context.Context
	Channel   interface{}
	MsgChan   chan (*MsgClass)
	ErrorChan chan (error)
	Grpc      *grpc.Server
}

func NewSubScriber(ctx context.Context, grpc *grpc.Server) *Subscriber {
	log = logger.NewLogger("Subscriber")
	s := &Subscriber{
		Ctx:       ctx,
		MsgChan:   make(chan *MsgClass),
		ErrorChan: make(chan error),
		Grpc:      grpc,
	}
	pb.RegisterStreamServiceServer(s.Grpc, s)

	return s
}

func (s *Subscriber) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	log.Info("start server")
	// and start...
	go func() {
		if err := s.Grpc.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
			s.ErrorChan <- err
		}
	}()
	go func() {
		select {
		case <-s.Ctx.Done():
			s.Grpc.Stop()
		}
	}()

	return nil
}

func (s *Subscriber) SendRequest(r *pb.Request, srv pb.StreamService_SendRequestServer) error {
	m := MsgClass{
		Request:        r,
		responseServer: srv,
	}
	m.waitgroup.Add(1)
	s.MsgChan <- &m
	go func() {
		select {
		case <-s.Ctx.Done():
			m.waitgroup.Done()
		}
	}()
	m.waitgroup.Wait()

	//handle keep alive
	//parse keepalive
	//if new worker =>add to map worker
	//if old worker =>update map
	return nil
}
