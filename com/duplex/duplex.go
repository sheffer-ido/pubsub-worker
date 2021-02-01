package duplex

import (
	"context"
	"io"
	"net"
	"sync"

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

func (m *MsgClass) Respond(result *pb.Response) error {
	err := m.responseServer.Send(result)
	m.waitgroup.Done()
	return err

}

type Duplex struct {
	Ctx       context.Context
	Channel   interface{}
	MsgChan   chan (*MsgClass)
	ErrorChan chan (error)
	Grpc      *grpc.Server
}

func NewDuplex(ctx context.Context, grpc *grpc.Server) *Duplex {
	log = logger.NewLogger("duplex")
	s := &Duplex{
		Ctx:       ctx,
		MsgChan:   make(chan *MsgClass),
		ErrorChan: make(chan error),
		Grpc:      grpc,
	}

	pb.RegisterDuplexServer(grpc, s)

	return s
}

func (d *Duplex) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}

	log.Info("start server")
	// and start...
	go func() {
		if err := d.Grpc.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
			d.ErrorChan <- err
		}
	}()
	go func() {
		select {
		case <-d.Ctx.Done():
			d.Grpc.Stop()
		}
	}()

	return nil
}

func (d *Duplex) StreamRequest(srv pb.Duplex_StreamRequestServer) error {

	ctx := srv.Context()

	for {

		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// receive data from stream
		req, err := srv.Recv()
		if err == io.EOF {
			// return will close stream from server side

			return nil
		}
		if err != nil {
			log.Printf("receive error %v", err)
			continue
		}
		m := MsgClass{
			Request:        req,
			responseServer: srv,
		}
		m.waitgroup.Add(1)
		d.MsgChan <- &m

		// continue if number received from stream
		// less than max
		// if req.Id <= max {
		// 	continue
		// }

		// update max and send it to stream
		//max = req.Id

		// resp := pb.Response{RequestId: req.Id}
		// if err := srv.Send(&resp); err != nil {
		// 	log.Printf("send error %v", err)
		// }
		// log.Printf("send new max=%d", max)
	}
}
