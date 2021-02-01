package server

import pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"

type Server struct {
}

func (b *Server) SendRequest(request *pb.Request, srv pb.StreamService_GetResponseServer) error {
	//get status of the task
	//remove from stack

}
