package health

import (
	"context"
	"net"

	logger "github.com/sheffer-ido/pubsub-worker/com/logger"
	pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"
	"google.golang.org/grpc"
)

var log *logger.Logger

type Health struct {
	client pb.AckNakServiceClient
	grpc   *grpc.ClientConn
}

func NewClient(port string) (*Health, error) {
	log = logger.NewLogger("health")
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
		return nil, err
	}
	c := pb.NewAckNakServiceClient(conn)
	return &Health{
		client: c,
	}, nil
}

func (h *Health) CloseClient() {
	h.grpc.Close()
}

func NewServer(port string) (*Health, error) {
	log = logger.NewLogger("health")
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return nil, err
	}

	s := &Health{}

	grpcServer := grpc.NewServer()

	pb.RegisterAckNakServiceServer(grpcServer, s)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
		return nil, err
	}
	return s, nil

}
func (h *Health) Healthy(ctx context.Context, in *pb.AckNak) (*pb.AckNak, error) {
	//log.Debugf("Receive message body from client: %s", in.Body)
	return &pb.AckNak{Body: "Hello From the Server!"}, nil
}

func (h *Health) Ack() (*pb.AckNak, error) {

	response, err := h.client.Healthy(context.Background(), &pb.AckNak{Body: "hi"})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	log.Printf("Response from server: %s", response.Body)
	return response, nil
}
