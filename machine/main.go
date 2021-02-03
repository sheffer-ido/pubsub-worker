package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/xid"
	logger "github.com/sheffer-ido/pubsub-worker/com/logger"
	safe "github.com/sheffer-ido/pubsub-worker/com/safe"
	pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"
	"google.golang.org/grpc"
)

var (
	port string = "5005"
	id   string = "myserver"
	log  *logger.Logger
)

func main() {

	log = logger.NewLogger("machine")

	//fill env params

	envName := []string{"ID", "PORT"}
	envVars := []*string{&id, &port}
	//fill local parameters with environment values
	for index := 0; index < len(envVars); index++ {
		if len(os.Getenv(envName[index])) == 0 {
			log.Errorf("Did not find environment %s setting to default of %s", envName[index], *envVars[index])
			continue
		}
		*envVars[index] = os.Getenv(envName[index])
		log.Infof("Found environment %s setting %s\n", envName[index], *envVars[index])
	}

	//set unique id
	id = fmt.Sprintf("id %s", xid.New())
	log.Infof("hi i m machine s%", id)

	//stop signal
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)

	//General safe bollean status for container health refrance
	grpcStatus := safe.SafeHealth{}
	grpcStatus.Set(false)

	//srv container health route (depending on grpcStatus)
	go func(*safe.SafeHealth) {
		http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			if grpcStatus.GetStatus() == false {
				http.Error(w, "not connected to server", http.StatusInternalServerError)
			}
			fmt.Fprint(w, "i feel good")
		},
		)
		http.ListenAndServe(":9001", nil)
	}(&grpcStatus)

	//context for rpc bidi loop functions
	ctx := context.Background()

	//synced array of processing request with no replay from server
	requests := &safe.SafeMap{}

	//Open bidi client stream
	openStream(ctx, port, &grpcStatus, requests)

	log.Errorf("finsih")

}

// open Stream -- when failed recursion itself
func openStream(ctx context.Context, port string, health *safe.SafeHealth, requests *safe.SafeMap) error {

	//if the dial fails the port is taken and it will return error
	conn, err := grpc.Dial(":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("can not connect with server %v", err)
		return err
	}

	// create stream
	client := pb.NewDuplexClient(conn)

	//connect to server will trey 3 times  with 3 second interval and exit if fails will issue a Stream again with new Dial
	stream, err := streamRequest(ctx, client, health, 0)
	if err != nil {
		time.Sleep(time.Second * 5)
		return openStream(ctx, port, health, requests)
	}

	//when connected will start send requests
	stremOperation(stream, health, requests)
	log.Errorf("finsih")

	//when stream fails a reconnect will occur
	return openStream(ctx, port, health, requests)

}

func stremOperation(stream pb.Duplex_StreamRequestClient, health *safe.SafeHealth, requests *safe.SafeMap) {
	//stream context will be done when connection fails
	ctx := stream.Context()

	done := make(chan bool)

	//health requests to the server to report status of machine
	//health response from the server will be handled by the response loop
	healthCounter := 0
	go func() {
		for {
			healthCounter++
			in := &pb.Request{
				Id:   int32(healthCounter),
				Meta: "health",
				Time: time.Now().Unix(),
			}

			if err := stream.Send(in); err != nil {
				log.Fatalf("can not send %v", err)
				health.Set(false)
				ctx.Done()
				return
			}
			log.Printf("%v sent", in)
			select {
			default:
				time.Sleep(time.Second * 1)
			}
		}
	}()

	//Re send the processed requests had no status from server
	for _, r := range requests.GetAll() {
		requests.Add(r.Id, r)
		if err := stream.Send(r); err != nil {
			log.Fatalf("can not send %v", err)
			continue
		}
		requests.Remove(r.Id)
	}

	///fake loop to generate messages
	go func() {
		counter := 0
		for {
			counter++
			in := generateRequest(counter)
			// generate random nummber and send it to stream
			requests.Add(in.Id, in)
			if err := stream.Send(in); err != nil {
				log.Fatalf("can not send %v", err)
				ctx.Done()
				return
			}
			log.Printf("%v sent", in)
			select {
			default:
				time.Sleep(time.Second * 5)
			}
		}
	}()

	//responses loop to handle server responses
	go func() {
		for {

			resp, err := stream.Recv()
			if err == io.EOF {
				close(done)
				return
			}
			if err != nil {
				log.Fatalf("can not receive %v", err)
				health.Set(false)
				ctx.Done()
				return
			}
			if resp.Meta == "health" {
				health.Set(true)
				continue
			}

			requests.Remove(resp.RequestId)

			log.Debugf("received type %s, id: %d, duration:%d sec", resp.Meta, resp.RequestId, (resp.Time - resp.RequestTime))
		}
	}()

	// third goroutine closes done channel
	// if context is done
	go func() {
		<-ctx.Done()
		if err := ctx.Err(); err != nil {
			log.Error(err.Error())
		}
		close(done)
	}()

	<-done
}

func streamRequest(ctx context.Context, client pb.DuplexClient, grpcStatus *safe.SafeHealth, counter int) (pb.Duplex_StreamRequestClient, error) {
	stream, err := client.StreamRequest(ctx)
	if err != nil {
		log.Fatalf("open stream error %v", err)
		grpcStatus.Set(false)
		counter++
		if counter < 3 {
			time.Sleep(time.Second * 3)
			return streamRequest(ctx, client, grpcStatus, counter)
		} else {
			return nil, fmt.Errorf("counter has reached 3 times")
		}
	}
	return stream, nil
}

func generateRequest(i int) *pb.Request {

	b, _ := json.Marshal(struct {
		Name string `json:"name"`
	}{Name: "ido"})

	return &pb.Request{
		Id:   int32(i),
		Data: b,
		Meta: "name",
		Time: time.Now().Unix(),
	}

}
