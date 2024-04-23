// Copyright 2021-2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build rpc || !core
// +build rpc !core

package server

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/internal/topo/sink"
	"github.com/lf-edge/ekuiper/pkg/infra"
	mycache "github.com/patrickmn/go-cache"
)

const QueryRuleId = "internal-ekuiper_query_rule"

func init() {
	servers["rpc"] = rpcComp{}
}

type rpcComp struct {
	s *http.Server
}

func (r rpcComp) register() {}

func (r rpcComp) serve() {
	// Start rpc service
	server := new(Server)
	portRpc := conf.Config.Basic.Port
	ipRpc := conf.Config.Basic.Ip
	rpcSrv := rpc.NewServer()
	err := rpcSrv.Register(server)
	if err != nil {
		logger.Fatal("Format of service Server isn'restHttpType correct. ", err)
	}
	srvRpc := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", ipRpc, portRpc),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      rpcSrv,
	}
	r.s = srvRpc
	go func() {
		if err = srvRpc.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rpc service:", err)
		}
	}()
	initQuery()
}

func (r rpcComp) close() {
	if r.s != nil {
		if err := r.s.Shutdown(context.TODO()); err != nil {
			logger.Errorf("rpc server shutdown error: %v", err)
		}
		logger.Info("rpc server shutdown.")
	}
}

type Server int

func (t *Server) CreateQuery(sql string, reply *string) error {
	if _, ok := registry.Load(QueryRuleId); ok {
		stopQuery()
	}
	tp, err := ruleProcessor.ExecQuery(QueryRuleId, sql)
	if err != nil {
		return err
	} else {
		rs := &rule.RuleState{RuleId: QueryRuleId, Topology: tp}
		registry.Store(QueryRuleId, rs)
		msg := fmt.Sprintf("Query was submit successfully.")
		logger.Println(msg)
		*reply = fmt.Sprintf(msg)
	}
	return nil
}

func stopQuery() {
	if rs, ok := registry.Load(QueryRuleId); ok {
		logger.Printf("stop the query.")
		(*rs.Topology).Cancel()
		registry.Delete(QueryRuleId)
	}
}

/**
 * qid is not currently used.
 */
func (t *Server) GetQueryResult(_ string, reply *string) error {
	if rs, ok := registry.Load(QueryRuleId); ok {
		c := (*rs.Topology).GetContext()
		if c != nil && c.Err() != nil {
			return c.Err()
		}
	}

	sink.QR.LastFetch = time.Now()
	sink.QR.Mux.Lock()
	if len(sink.QR.Results) > 0 {
		*reply = strings.Join(sink.QR.Results, "\n")
		sink.QR.Results = make([]string, 0, 10)
	} else {
		*reply = ""
	}
	sink.QR.Mux.Unlock()
	return nil
}

func (t *Server) Stream(stream string, reply *string) error {
	content, err := streamProcessor.ExecStmt(stream)
	if err != nil {
		return fmt.Errorf("Stream command error: %s", err)
	} else {
		for _, c := range content {
			*reply = *reply + fmt.Sprintln(c)
		}
	}
	return nil
}

func (t *Server) CreateRule(rule *model.RPCArgDesc, reply *string) error {
	id, err := createRule(rule.Name, rule.Json)
	if err != nil {
		return fmt.Errorf("Create rule %s error : %s.", id, err)
	} else {
		*reply = fmt.Sprintf("Rule %s was created successfully, please use 'bin/kuiper getstatus rule %s' command to get rule status.", rule.Name, rule.Name)
	}
	return nil
}

func (t *Server) GetStatusRule(name string, reply *string) error {
	if r, err := getRuleStatus(name); err != nil {
		return err
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) GetTopoRule(name string, reply *string) error {
	if r, err := getRuleTopo(name); err != nil {
		return err
	} else {
		dst := &bytes.Buffer{}
		if err = json.Indent(dst, []byte(r), "", "  "); err != nil {
			*reply = r
		} else {
			*reply = dst.String()
		}
	}
	return nil
}

func (t *Server) StartRule(name string, reply *string) error {
	if err := startRule(name); err != nil {
		return err
	} else {
		*reply = fmt.Sprintf("Rule %s was started", name)
	}
	return nil
}

func (t *Server) StopRule(name string, reply *string) error {
	*reply = stopRule(name)
	return nil
}

func (t *Server) RestartRule(name string, reply *string) error {
	err := restartRule(name)
	if err != nil {
		return err
	}
	*reply = fmt.Sprintf("Rule %s was restarted.", name)
	return nil
}

func (t *Server) DescRule(name string, reply *string) error {
	r, err := ruleProcessor.ExecDesc(name)
	if err != nil {
		return fmt.Errorf("Desc rule error : %s.", err)
	} else {
		*reply = r
	}
	return nil
}

func countProcessedMessage(cache *mycache.Cache, ruleid string, duration time.Duration) (int, error) {

	allcount := 0
	allcount = sumProcessedMessagesByRuleID(cache, ruleid, duration)
	return allcount, nil
}

func sumInputMessagesByRuleID(cache *mycache.Cache, ruleID string, duration time.Duration) int {
	sum := 0
	for _, item := range cache.Items() {
		rule, ok := item.Object.(*ruleMetrics)
		if !ok {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < duration && rule.Ruleid == ruleID {
			sum += rule.InputMessageCount
		}
	}
	return sum
}

func countIncomingMessage(cache *mycache.Cache, ruleid string, duration time.Duration) (int, error) {
	count := 0
	count = sumInputMessagesByRuleID(cache, ruleid, duration)
	return count, nil
}

func sumProcessedMessagesByRuleID(cache *mycache.Cache, ruleID string, duration time.Duration) int {
	sum := 0
	for _, item := range cache.Items() {
		rule, ok := item.Object.(*ruleMetrics)
		if !ok {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < duration && rule.Ruleid == ruleID {
			sum += rule.ProcessedMessageCount
		}
	}
	return sum
}

//get the last hours performance, every rules perfromance
func (t *Server) ProcessedMessageCount(name string, reply *string) error {
	//processedMessagecount := 0
	i, err := strconv.Atoi(name)
	if err != nil {
		log.Fatal(err)
	}

	allresuls := make(map[string]int, 0)
	ks := globalcache.Items()
	for k, _ := range ks {
		tmpRuleState, found := globalcache.Get(k)
		if found {
			rssatus, _ := tmpRuleState.(*rule.RuleState)
			//result, _ := rssatus.GetState()
			//if result == "Running" {
			count, err := countProcessedMessage(rulestaticcache, rssatus.RuleId, time.Duration(i)*time.Second)
			if err != nil {
				continue
			}
			allresuls[rssatus.RuleId] = count
			//}
		}
	}

	file, err := os.Create("ProcessedMessageCount.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题行
	err = writer.Write([]string{"key", "value"})
	if err != nil {
		log.Fatal(err)
	}

	// 写入数据行
	for k, v := range allresuls {
		err := writer.Write([]string{k, strconv.Itoa(v)})
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

//get the last hours performance, every rules perfromance
func (t *Server) IncomingMessageCount(name string, reply *string) error {
	//processedMessagecount := 0
	i, err := strconv.Atoi(name)
	if err != nil {
		log.Fatal(err)
	}

	allresuls := make(map[string]int, 0)
	ks := globalcache.Items()
	for k, _ := range ks {
		tmpRuleState, found := globalcache.Get(k)
		if found {
			rssatus, _ := tmpRuleState.(*rule.RuleState)
			//result, _ := rssatus.GetState()
			//if result == "Running" {
			count, err := countIncomingMessage(rulestaticcache, rssatus.RuleId, time.Duration(i)*time.Second)
			if err != nil {
				continue
			}
			allresuls[rssatus.RuleId] = count
			//}
		}
	}

	file, err := os.Create("IncomingMessageCount.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题行
	err = writer.Write([]string{"key", "value"})
	if err != nil {
		log.Fatal(err)
	}

	// 写入数据行
	for k, v := range allresuls {
		err := writer.Write([]string{k, strconv.Itoa(v)})
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

func getSinkItems(db *memdb.MemDB, ruleid string, duration time.Duration) ([]*node.SinkTuple, error) {
	txn := db.Txn(false)
	defer txn.Abort()

	// 创建一个时间范围，从当前时间向前推duration
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	// 获取索引
	ruleIndex, err := txn.Get("sinktuple", "ruleid", ruleid)
	if err != nil {
		return nil, err
	}
	var items []*node.SinkTuple
	for item := ruleIndex.Next(); item != nil; item = ruleIndex.Next() {
		// 获取whichtime字段的值并将其解析为time.Time类型
		zz := item.(*node.SinkTuple)
		whichTimeStr := item.(*node.SinkTuple).GetSinkTime()
		whichTime, err := time.Parse(time.RFC3339Nano, whichTimeStr)
		if err != nil {
			return nil, err
		}

		// 检查whichtime是否在指定的时间范围内
		if whichTime.After(startTime) && whichTime.Before(endTime) {
			fmt.Printf(".........................vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv%v\n", zz)
			items = append(items, item.(*node.SinkTuple))
		}
	}
	return items, nil
}

func countDistinctRuleIDsInCache(cache *mycache.Cache, duration time.Duration) int {
	ruleIDs := make(map[string]struct{})
	for _, item := range cache.Items() {
		rule, ok := item.Object.(*ruleMetrics)
		if !ok {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < duration {
			ruleIDs[rule.Ruleid] = struct{}{}
		}
	}
	return len(ruleIDs)
}

func (t *Server) CompletedRuleCount(name string, reply *string) error {
	//processedMessagecount := 0
	i, err := strconv.Atoi(name)
	if err != nil {
		log.Fatal(err)
	}
	completedrulecounts := countDistinctRuleIDsInCache(rulestaticcache, time.Duration(i)*time.Second)
	file, err := os.Create("CompletedRuleCount.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	fmt.Println("UUUUUUUUUUUUUUUUUUUUUUUUUUUU.....", completedrulecounts)
	_, err = fmt.Fprintf(file, "%d\n", completedrulecounts)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func countRuleIDByLastCompletionTime(cache *mycache.Cache, ruleID string, duration time.Duration) int {
	count := 0
	lastCompletionTimes := make(map[string]struct{})
	for _, item := range cache.Items() {
		rule, ok := item.Object.(*ruleMetrics)
		if !ok {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < duration && rule.Ruleid == ruleID {
			if _, ok := lastCompletionTimes[rule.LastCompletiontime]; !ok {
				count++
				lastCompletionTimes[rule.LastCompletiontime] = struct{}{}
			}
		}
	}
	return count
}

// func countMetricsInLastNSeconds(db *memdb.MemDB, n int) int {

// 	txn := db.Txn(false)
// 	defer txn.Abort()
// 	count := 0
// 	it, err := txn.Get("metrics", "id")
// 	if err != nil {
// 		panic(err)
// 	}

// 	for obj := it.Next(); obj != nil; obj = it.Next() {
// 		metric := obj.(*conf.RuleMetric)
// 		t, err := time.Parse(time.RFC3339Nano, metric.LastCompletiontime)
// 		if err != nil {
// 			panic(err)
// 		}
// 		if time.Since(t) <= time.Duration(n)*time.Second {
// 			count++
// 		}
// 	}
// 	return count
// }

//every rule complete the whole chaining how many time
func (t *Server) RuleCompletionCount(name string, reply *string) error {
	//processedMessagecount := 0
	i, err := strconv.Atoi(name)
	if err != nil {
		log.Fatal(err)
	}
	rulesuccessfulruns := make(map[string]int, 0)
	ks := globalcache.Items()

	for k, _ := range ks {
		tmpRuleState, _ := globalcache.Get(k)
		//if found {
		rssatus, _ := tmpRuleState.(*rule.RuleState)
		//based on the sink to recursively obtain whether there has a correpsonding input message
		sucesfuns := 0
		sucesfuns = countRuleIDByLastCompletionTime(rulestaticcache, rssatus.RuleId, time.Duration(i)*time.Second)
		fmt.Println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV", sucesfuns)
		// if sucesfuns <= 0 {
		// 	continue
		// }
		rulesuccessfulruns[rssatus.RuleId] = sucesfuns
		//}
	}

	file, err := os.Create("RuleCompletionCount.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题行
	err = writer.Write([]string{"key", "value"})
	if err != nil {
		log.Fatal(err)
	}

	// 写入数据行
	for k, v := range rulesuccessfulruns {
		err := writer.Write([]string{k, strconv.Itoa(v)})
		if err != nil {
			log.Fatal(err)
		}
	}

	//write the result out

	return nil
}

func averageOperatorCostByRuleID(cache *mycache.Cache, ruleID string, duration time.Duration) float64 {
	sum := 0.0
	count := 0
	for _, item := range cache.Items() {
		rule, ok := item.Object.(*ruleMetrics)
		if !ok {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < duration && rule.Ruleid == ruleID {
			sum += rule.OperatorCost
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

func (t *Server) RuleAverageTimeSpend(name string, reply *string) error {
	//processedMessagecount := 0
	i, err := strconv.Atoi(name)
	if err != nil {
		log.Fatal(err)
	}
	rulesuccessfulruns := make(map[string]float64, 0)
	ks := globalcache.Items()
	for k, _ := range ks {
		tmpRuleState, _ := globalcache.Get(k)
		rssatus, _ := tmpRuleState.(*rule.RuleState)
		opcost := averageOperatorCostByRuleID(rulestaticcache, rssatus.RuleId, time.Duration(i)*time.Second)
		rulesuccessfulruns[rssatus.RuleId] = opcost
	}

	file, err := os.Create("RuleAverageTimeSpend.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题行
	err = writer.Write([]string{"key", "value"})
	if err != nil {
		log.Fatal(err)
	}

	// 写入数据行
	for k, v := range rulesuccessfulruns {
		err := writer.Write([]string{k, strconv.FormatFloat(v, 'f', -1, 64)})
		if err != nil {
			log.Fatal(err)
		}
	}

	//write the result out

	return nil
}

func (t *Server) ShowRules(_ int, reply *string) error {
	r, err := getAllRulesWithStatus()
	if err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	}
	if len(r) == 0 {
		*reply = "No rule definitions are found."
	} else {
		result, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("Show rule error : %s.", err)
		}
		dst := &bytes.Buffer{}
		if err := json.Indent(dst, result, "", "  "); err != nil {
			return fmt.Errorf("Show rule error : %s.", err)
		}
		*reply = dst.String()
	}
	return nil
}

func (t *Server) DropRule(name string, reply *string) error {
	deleteRule(name)
	r, err := ruleProcessor.ExecDrop(name)
	if err != nil {
		return fmt.Errorf("Drop rule error : %s.", err)
	} else {
		err := t.StopRule(name, reply)
		if err != nil {
			return err
		}
	}
	*reply = r
	return nil
}

func (t *Server) Import(file string, reply *string) error {
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("fail to read file %s: %v", file, err)
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)
	if err != nil {
		return fmt.Errorf("fail to convert file %s: %v", file, err)
	}
	content := buf.Bytes()
	rules, counts, err := rulesetProcessor.Import(content)
	if err != nil {
		return fmt.Errorf("import ruleset error: %v", err)
	}
	infra.SafeRun(func() error {
		for _, name := range rules {
			rul, ee := ruleProcessor.GetRuleById(name)
			if ee != nil {
				logger.Error(ee)
				continue
			}
			reply := recoverRule(rul)
			if reply != "" {
				logger.Error(reply)
			}
		}
		return nil
	})
	*reply = fmt.Sprintf("imported %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
	return nil
}

func (t *Server) Export(file string, reply *string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	exported, counts, err := rulesetProcessor.Export()
	if err != nil {
		return err
	}
	_, err = io.Copy(f, exported)
	if err != nil {
		return fmt.Errorf("fail to save to file %s:%v", file, err)
	}
	*reply = fmt.Sprintf("exported %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
	return nil
}

func (t *Server) ImportConfiguration(arg *model.ImportDataDesc, reply *string) error {
	file := arg.FileName
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("fail to read file %s: %v", file, err)
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)
	if err != nil {
		return fmt.Errorf("fail to convert file %s: %v", file, err)
	}
	content := buf.Bytes()

	configurationReset()
	err = configurationImport(content, arg.Stop)
	if err != nil {
		return fmt.Errorf("import configuration error: %v", err)
	}
	*reply = fmt.Sprintf("import configuration success")
	return nil
}

func (t *Server) GetStatusImport(_ int, reply *string) error {
	jsonRsp := configurationStatusExport()
	result, err := json.Marshal(jsonRsp)
	if err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, result, "", "  "); err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	}
	*reply = dst.String()

	return nil
}

func (t *Server) ExportConfiguration(file string, reply *string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	jsonBytes, err := configurationExport()
	_, err = io.Copy(f, bytes.NewReader(jsonBytes))
	if err != nil {
		return fmt.Errorf("fail to save to file %s:%v", file, err)
	}
	*reply = fmt.Sprintf("export configuration success")
	return nil
}

func marshalDesc(m interface{}) (string, error) {
	s, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("invalid json %v", m)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, s, "", "  "); err != nil {
		return "", fmt.Errorf("indent json error %v", err)
	}
	return dst.String(), nil
}

func initQuery() {
	ticker := time.NewTicker(time.Second * 5)
	go infra.SafeRun(func() error {
		for {
			//fmt.Println("initial query....")

			<-ticker.C
			if registry != nil {
				if _, ok := registry.Load(QueryRuleId); !ok {
					continue
				}
				fmt.Println("registry...", QueryRuleId)

				n := time.Now()
				w := 10 * time.Second
				if v := n.Sub(sink.QR.LastFetch); v >= w {
					logger.Printf("The client seems no longer fetch the query result, stop the query now.")
					stopQuery()
				}
			}
		}
	})
}
