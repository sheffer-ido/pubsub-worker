package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/xid"
	logger "github.com/sheffer-ido/pubsub-worker/com/logger"
	pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"
	"google.golang.org/grpc"
)

var (
	port string = "5005"
	id   string = "myserver"
)

func main() {

	log := logger.NewLogger("machine")

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

	//check keepalive
	connected := false

	//set pod keepalive
	go func(bool) {
		http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			if connected == false {
				http.Error(w, "not connected to server", http.StatusInternalServerError)
			}
			fmt.Fprint(w, "i feel good")
		},
		)
		http.ListenAndServe(":9001", nil)
	}(connected)

	conn, err := ReDial(port, 0)
	if err != nil {
		log.Fatalf("%s", err.Error())
		return
	}

	// create stream
	client := pb.NewDuplexClient(conn)
	stream, err := client.StreamRequest(context.Background())
	if err != nil {
		log.Fatalf("openn stream error %v", err)
		return
	}

	ctx := stream.Context()
	done := make(chan bool)

	healthCounter := 0
	go func() {
		for {
			healthCounter++
			in := &pb.Request{
				Id:   int32(healthCounter),
				Meta: "health",
				Time: time.Now().Unix(),
			}
			// generate random nummber and send it to stream

			if err := stream.Send(in); err != nil {
				log.Fatalf("can not send %v", err)
				/*
						conn, err := ReDial(port, 0)
					if err != nil {
						log.Fatalf("%s", err.Error())
						return
					}
					client = pb.NewStreamServiceClient(conn)
				*/
			}
			log.Printf("%v sent", in)
			select {
			case <-gracefulShutdown:
				log.Fatal("closing")
				return
			default:
				time.Sleep(time.Second * 1)
			}
		}
	}()
	// first goroutine sends random increasing numbers to stream
	// and closes int after 10 iterations
	go func() {
		counter := 0
		for {
			counter++
			in := generateRequest(counter)
			// generate random nummber and send it to stream

			if err := stream.Send(in); err != nil {
				log.Fatalf("can not send %v", err)
			}
			log.Printf("%v sent", in)
			select {
			case <-gracefulShutdown:
				log.Fatal("closing")
				return
			default:
				time.Sleep(time.Second * 5)
			}
		}
	}()

	// second goroutine receives data from stream
	// and saves result in max variable
	//
	// if stream is finished it closes done channel
	go func() {
		for {

			resp, err := stream.Recv()
			if err == io.EOF {
				close(done)
				return
			}
			if err != nil {
				log.Fatalf("can not receive %v", err)
				connected = false
				continue
			}
			if resp.Meta == "health" {
				connected = true
			}

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

func ReDial(port string, counter int) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("can not connect with server %v", err)
		if counter < 3 {
			counter++
			time.Sleep(time.Second * 1)
			ReDial(port, counter)
		} else {
			return nil, fmt.Errorf("counter has reached 3 times")
		}
	}
	return conn, nil

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
