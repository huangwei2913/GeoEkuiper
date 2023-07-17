package server

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

var TRANSFER = "TRANSFER" //transfer rule to peer

func TransferedRuleToPeer(rulejson chan string) {

	for rule_json := range rulejson {
		//we firestly checked if it's a valid json
		validatejson := json.Valid([]byte(rule_json))
		if validatejson == false {
			continue
		}
		//
		bytes, _ := json.Marshal(rule_json)
		nc, err5 := nats.Connect("nats://10.66.66.1:4222")
		if err5 != nil {
			panic(err5)
		}
		defer nc.Close()
		nc.Publish(TRANSFER, bytes)
	}

}
