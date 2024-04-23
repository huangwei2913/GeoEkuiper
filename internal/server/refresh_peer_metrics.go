package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	uuid "github.com/satori/go.uuid"
)

func RefreshMetricsFromPeer() {

	nc, err := nats.Connect("nats://10.66.66.1:4222")
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	query := make(chan *nats.Msg, 8*1024)
	_, err1 := nc.ChanSubscribe(QUERYRESULT, query)

	if err1 != nil {
		panic(err1)
	}

	go func() {
		for {
			select {
			case msg := <-query:
				var rm ruleMetrics
				if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&rm); err != nil {
					log.Fatal(err)
				}
				// 处理接收到的 ruleMetrics 对象
				fmt.Printf("Received ruleMetrics: %+v\n", rm)
				rulestaticcache.Set(string(uuid.NewV4().String()), rm, 0)
			}

		}
	}()

}
