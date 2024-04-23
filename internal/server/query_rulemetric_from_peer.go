package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

//periodically, accpet the rule metrics query, and return the timespedent on the peer
//typically
var QUERYPEER = "QUERY" //the following is the topi used by nats.go
var QUERYRESULT = "QUERYRESULT"

func AccpetPeerQuery() {

	nc, err := nats.Connect("nats://10.66.66.1:4222")
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	query := make(chan *nats.Msg, 8*1024)
	_, err1 := nc.ChanSubscribe(QUERYPEER, query)

	if err1 != nil {
		panic(err1)
	}
	go func() {
		for {
			select {
			case sql := <-query:
				fmt.Println("we get the peer metric query....", sql)
				the_rulesql := string(sql.Data)
				var result *ruleMetrics
				for _, item := range rulestaticcache.Items() {
					rm := item.Object.(*ruleMetrics)
					if rm.Isremote == true && rm.RuleSql == the_rulesql {
						rmSamplingTime, err := time.Parse(time.RFC3339Nano, rm.LastCompletiontime)
						if err != nil {
							fmt.Printf("解析SamplingTime出错：%v\n", err)
							continue
						}
						if result == nil {
							result = rm
						} else {
							resultSamplingTime, err := time.Parse(time.RFC3339Nano, result.LastCompletiontime)
							if err != nil {
								fmt.Printf("解析SamplingTime出错：%v\n", err)
								continue
							}
							if rmSamplingTime.After(resultSamplingTime) {
								result = rm
							}
						}
					}
				}

				if result != nil {
					fmt.Printf("找到了一个匹配的元素：%+v\n", result)
					var buf bytes.Buffer
					if err := gob.NewEncoder(&buf).Encode(result); err != nil {
						log.Fatal(err)
					}
					// 发送数据到指定的主题
					if err := nc.Publish(QUERYRESULT, buf.Bytes()); err != nil {
						log.Fatal(err)
					}

					if err := nc.Flush(); err != nil {
						log.Fatal(err)
					}

				}

			}

		}
	}()
}
