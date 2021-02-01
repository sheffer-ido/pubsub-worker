package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/common/log"
	"github.com/rs/xid"
	dup "github.com/sheffer-ido/pubsub-worker/com/duplex"
	logger "github.com/sheffer-ido/pubsub-worker/com/logger"
	pb "github.com/sheffer-ido/pubsub-worker/com/src/proto"
	"google.golang.org/grpc"
)

var (
	port string = "5005"
	id   string = "my"
)

func main() {

	log := logger.NewLogger("main")

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

	id = fmt.Sprintf("%s_%s", id, xid.New())

	log.Infof("hi i m BE worker s%", id)

	//general connection status
	connected := false
	//serv container health
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

	// create grpc StreamService server
	s := grpc.NewServer()
	d := dup.NewDuplex(context.Background(), s)
	// and start...
	err := d.Start(port)

	if err != nil {
		log.Fatalf("could not creat server %s", err.Error())
		return
	}
	//handle grpc server errors
	// go func() {
	// 	for err := range h.ErrorChan {
	// 		log.Errorf("error on subscriber %s", err.Error())
	// 	}
	// }()

	//handle messages from machines
	go func() {
		for m := range d.MsgChan {
			log.Debugf("received msg %s", m.Request.Meta)

			r, err := workClassification(m.Request)
			if err != nil {
				m.Respond(&pb.Response{
					RequestId:   m.Request.Id,
					Meta:        m.Request.Meta,
					Result:      err.Error(),
					Data:        []byte{},
					Time:        time.Now().Unix(),
					RequestTime: m.Request.Time,
				})
			} else {
				m.Respond(&pb.Response{
					RequestId:   m.Request.Id,
					Meta:        m.Request.Meta,
					Result:      "ok",
					Data:        r,
					Time:        time.Now().Unix(),
					RequestTime: m.Request.Time,
				})
			}

		}
	}()

	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	//done make break
	<-gracefulShutdown

}

func workClassification(req *pb.Request) ([]byte, error) {
	var res []byte
	switch req.Meta {
	case "health":
		return []byte{}, nil
	case "name":
		name := struct {
			Name string `json:"name"`
		}{}
		err := json.Unmarshal(req.Data, &name)
		if err != nil {
			return nil, err
		}
		log.Debugf("name v%", name)
		name.Name += " ok"
		res, err = json.Marshal(name)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("could not find type %d", req.Meta)
	}

	sleep := rand.Intn(20-1) + 1
	if sleep == 1 {
		time.Sleep(time.Second * 7)
	} else {
		d := 4 - (sleep / 10)
		time.Sleep(time.Second * time.Duration(d))
	}
	return res, nil

}
