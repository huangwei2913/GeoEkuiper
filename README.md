# GeoEkuiper
GeoEkuiper: a Cloud-Cooperated Geospatial Edge Stream Processing Engine for Resource-Constrained Devices with Higher Throughput

The code can be requested by contacting Dr. Wei Huang (huangwei2913@gmail.com)

We have some testing scripts to help reproduce the results of our work.

Regarding the spatial joining test, you can first run:

./kuiper  create stream src1 '() WITH (FORMAT="JSON", DATASOURCE="src1/#")'
./kuiper  create stream src2 '() WITH (FORMAT="JSON", DATASOURCE="src2/#")'

then run the following go program to send polygons to you preconfigured EMQX cluster:

package main

import (
    "encoding/json"
    "fmt"
    "math/rand"
    "time"

    mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Message struct {
    Geom string `json:"geom"`
}

func main() {
    broker := "tcp://10.66.66.2:1883" // 替换为您的MQTT代理地址
    topics := make([]string, 10)
    for i := 0; i < 10; i++ {
        topics[i] = fmt.Sprintf("src%d/test", i+1)
    }

    client := connect("publisher", broker)
    defer client.Disconnect(0)

    rand.Seed(time.Now().UnixNano())

    // 每100ms随机向一个MQTT主题发布一个封装好的Message结构体数据
    ticker := time.NewTicker(100 * time.Millisecond)
    go func() {
        for range ticker.C {
            topic := topics[rand.Intn(len(topics))]
            polygon := generatePolygon(100)
            message := Message{
                Geom: polygonToString(polygon),
            }
            messageJSON, _ := json.Marshal(message)
            token := client.Publish(topic, 0, false, messageJSON)
            token.Wait()
        }
    }()

    select {}
}

func connect(clientID string, uri string) mqtt.Client {
    opts := mqtt.NewClientOptions()
    opts.AddBroker(uri)
    opts.SetClientID(clientID)

    client := mqtt.NewClient(opts)
    token := client.Connect()
    token.Wait()

    return client
}

func generatePolygon(size int) [][]int {
    polygon := make([][]int, size*4)
    for i := 0; i < size; i++ {
        polygon[i] = []int{i, 0}
        polygon[size+i] = []int{size - 1, i}
        polygon[size*2+i] = []int{size - 1 - i, size - 1}
        polygon[size*3+i] = []int{0, size - 1 - i}
    }
    return polygon
}

func polygonToString(polygon [][]int) string {
    str := "POLYGON (("
    for i, point := range polygon {
        str += fmt.Sprintf("%d %d", point[0], point[1])
        if i != len(polygon)-1 {
            str += ", "
        }
    }
    str += "))"
    return str
}

the metrics can be obtained by using the following commands:
./kuiper describe  IncomingMessageCount 60 
./kuiper describe ProcessedMessageCount 60 
./kuiper describe  RuleCompletionCount 60
./kuiper describe  RuleAverageTimeSpend 60

notes: these metrics represent the unit time that the incoming message count, the processed message count, and the rule competion count, and average time spend per rule respectivey.


