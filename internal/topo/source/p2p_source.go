package source

import (
	"encoding/json"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/nats-io/nats.go"
)

//const BooststrapnodeId = "/ip4/175.178.60.176/tcp/7654/p2p/12D3KooWRkUCFAL1YddcXmpi7Cg66exfAHBmCnjcbq8v422TFRBb"
// we are going on to leverage  to ingested distributed tasks
type P2pSource struct {
	topic string
	nc    *nats.Conn
}

type Message struct {
	Messageid string `json:"messageid"`
	Topic     string `json:"topic"`
	Data      string `json:"data"`
}

//add the
func (myp2p *P2pSource) Close(ctx api.StreamContext) error {
	myp2p.nc.Close()
	return nil
}

func (myp2p *P2pSource) Configure(datasource string, props map[string]interface{}) error {
	myp2p.topic = datasource
	nc, err := nats.Connect("nats://10.66.66.1:4222")
	if err != nil {
		panic(err)
	}
	myp2p.nc = nc
	return nil
}

func (myp2p *P2pSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	err := myp2p.subscribe(ctx, consumer, errCh)
	if err != nil {
		errCh <- err
	}
}

func (myp2p *P2pSource) subscribe(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) error {

	lines := make(chan *nats.Msg, 8*1024)
	// ChanSubscribe will put all messages it received into the channnel
	_, err := myp2p.nc.ChanSubscribe(myp2p.topic, lines)

	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-lines:
			the_rulejson := string(msg.Data)
			//first to see whether it's a validatedjson
			validatejson := json.Valid([]byte(the_rulejson))
			if validatejson == false {
				continue
			}
			var bird Message
			if err := json.Unmarshal([]byte(the_rulejson), &bird); err != nil {
				continue
			}
			data := bird.Data
			length := len(data)
			meta := map[string]interface{}{
				"messageid": bird.Messageid,
				"topic":     bird.Topic,
				"length":    length,
			}
			result, _ := ctx.Decode(([]byte)(data))
			select {
			case consumer <- api.NewDefaultSourceTuple(result, meta):
			case <-ctx.Done():
				return nil
			}

		case <-ctx.Done():
			return nil
		}
	}

}
