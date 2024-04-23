// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package server

/*

#include <geos_c.h>
#include <stdlib.h>

#cgo LDFLAGS: -L/usr/local/lib -lgeos_c
#cgo CFLAGS: -I/usr/local/include
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/binder/meta"
	"github.com/lf-edge/ekuiper/internal/conf"
	meta2 "github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/topo/connection/factory"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/nats-io/nats.go"
	mycache "github.com/patrickmn/go-cache"
)

var globalcache = mycache.New(5*time.Minute, 100*time.Minute)

var rulesopeationscache = mycache.New(5*time.Minute, 10*time.Minute)

var rulestaticcache = mycache.New(5*time.Minute, 10*time.Minute)

var rulestateid2rulejson = mycache.New(5*time.Minute, 10*time.Minute)

// we will predict the time before we getting transfered rule

const trasnferedTopics = "transfered" //subscribe to

type ruleMetrics struct {
	Ruleid                string  `json:"ruleid"`                //规则ID
	Timespent             float64 `json:"timespent"`             //规则执行一次拓扑所需要时间
	Isremote              bool    `json:"isremote"`              //是主机的还是远程的规则
	RuleSql               string  `json:"rulesql"`               //规则所对应的拓扑结构，也就是sql查询语句
	LastCompletiontime    string  `json:"lastshowntime"`         //上次最晚的sink接收时间
	SamplingTime          string  `json:"samplingtime"`          //何时对规则进行采样
	InputMessageCount     int     `json:"inputmessagecount"`     //输入到规则的消息个数
	ProcessedMessageCount int     `json:"processedmessagecount"` //规则的消息个数
	OperatorCost          float64 `json:"operationcost"`         //规则的拓扑中操作器的平均处理时间
}

var the_role = 0

var (
	logger           = conf.Log
	startTimeStamp   int64
	version          = ""
	ruleProcessor    *processor.RuleProcessor
	streamProcessor  *processor.StreamProcessor
	rulesetProcessor *processor.RulesetProcessor
)

var transfered = make(chan string) //listensing for incoming rules from peers
var demo = make(chan string)       //listensing for incoming rules from peers

// Create path if mount an empty dir. For edgeX, all the folders must be created priorly
func createPaths() {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	dirs := []string{"uploads", "sources", "sinks", "functions", "services", "services/schemas", "connections"}

	for _, v := range dirs {
		// Create dir if not exist
		realDir := filepath.Join(dataDir, v)
		if _, err := os.Stat(realDir); os.IsNotExist(err) {
			if err := os.MkdirAll(realDir, os.ModePerm); err != nil {
				fmt.Printf("Failed to create dir %s: %v", realDir, err)
			}
		}
	}

	files := []string{"connections/connection.yaml"}
	for _, v := range files {
		// Create dir if not exist
		realFile := filepath.Join(dataDir, v)
		if _, err := os.Stat(realFile); os.IsNotExist(err) {
			if _, err := os.Create(realFile); err != nil {
				fmt.Printf("Failed to create file %s: %v", realFile, err)
			}
		}
	}

}

// https://play.golang.org/p/Qg_uv_inCek
// contains checks if a string is present in a slice
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func StartUp(Version, LoadFileType string) {

	version = Version
	conf.LoadFileType = LoadFileType
	startTimeStamp = time.Now().Unix()
	createPaths()
	conf.InitConf()
	factory.InitClientsFactory()

	err := store.SetupWithKuiperConfig(conf.Config)
	if err != nil {
		panic(err)
	}

	meta2.InitYamlConfigManager()
	ruleProcessor = processor.NewRuleProcessor()
	streamProcessor = processor.NewStreamProcessor()
	rulesetProcessor = processor.NewRulesetProcessor(ruleProcessor, streamProcessor)
	initRuleset()

	// register all extensions
	for k, v := range components {
		logger.Infof("register component %s", k)
		//fmt.Printf("register component %s... \n", k)
		v.register()
	}

	// Bind the source, function, sink
	sort.Sort(entries)
	err = function.Initialize(entries)
	if err != nil {
		panic(err)
	}
	err = io.Initialize(entries)
	if err != nil {
		panic(err)
	}
	meta.Bind()

	registry = &RuleRegistry{internal: make(map[string]*rule.RuleState)}
	//Start lookup tables, we are not care about it first
	streamProcessor.RecoverLookupTable()
	//Start rules
	if rules, err := ruleProcessor.GetAllRules(); err != nil {
		logger.Infof("Start rules error: %s", err)
	} else {
		logger.Info("Starting rules")
		var reply string
		for _, name := range rules {
			fmt.Println("the rule being looked is ...", name)
			rule, err := ruleProcessor.GetRuleById(name)
			if err != nil {
				logger.Error(err)
				continue
			}

			fmt.Println("the rule id is..", name, ".. and beign to manage the rule state........", rule.Sql)
			//we can extrate the demo, and analysis the streamsouce
			//for the specified rule, we going on to update the
			stmt, err := xsql.GetStatementFromSql(rule.Sql)
			streamsFromStmt := xsql.GetStreams(stmt)
			fmt.Printf("streamsFromStmt...................: %v\n", streamsFromStmt)
			if len(streamsFromStmt) <= 0 {
				continue
			}
			isfrompeerrule := false
			for _, vsource := range streamsFromStmt {
				//we get the stream information
				if vsource == "" {
					continue
				}
				allstreamsinfo, err := streamProcessor.GetAll()
				if err != nil {
					panic(err)
				}
				for _, v := range allstreamsinfo {
					zzstreaminfo := v[vsource]
					if zzstreaminfo == "" {
						continue
					}
					//fmt.Println("m....m.m.m.................", zzstreaminfo)
					if strings.Contains(zzstreaminfo, "TYPE=\"p2p\"") == true {
						isfrompeerrule = true
						break
					}
				}

			}

			if isfrompeerrule == true {
				fmt.Println("we are going to tackle the peer transfered rrule......................")
				reply = recoverRuleForPeer(rule)
				if 0 != len(reply) {
					logger.Info(reply)
				}
			} else {
				reply = recoverRule(rule)
				if 0 != len(reply) {
					logger.Info(reply)
				}
			}

		}
	}

	//for simulations the sceduler,we can randmoly stop and restart our rules
	//Start rest service
	srvRest := createRestServer(conf.Config.Basic.RestIp, conf.Config.Basic.RestPort, conf.Config.Basic.Authentication)
	go func() {
		var err error
		if conf.Config.Basic.RestTls == nil {
			err = srvRest.ListenAndServe()
		} else {
			err = srvRest.ListenAndServeTLS(conf.Config.Basic.RestTls.Certfile, conf.Config.Basic.RestTls.Keyfile)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Errorf("Error serving rest service: %s", err)
		}
	}()

	// Start extend services
	for k, v := range servers {
		fmt.Println("start extend serives ....", k)
		logger.Infof("start service %s", k)
		v.serve()
	}

	//Startup message
	restHttpType := "http"
	if conf.Config.Basic.RestTls != nil {
		restHttpType = "https"
	}
	msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d, and restful api on %s://%s:%d. \n", Version, conf.Config.Basic.Port, restHttpType, conf.Config.Basic.RestIp, conf.Config.Basic.RestPort)
	logger.Info(msg)
	fmt.Printf(msg)

	fmt.Println("waiting exisits message..................................")

	getrulemetrics()
	//GetOpetationMetric()
	AccpetPeerQuery()
	RefreshMetricsFromPeer()
	accepetTransferedrule()

	//Stop the services
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
	<-sigint

	if err = srvRest.Shutdown(context.TODO()); err != nil {
		logger.Errorf("rest server shutdown error: %v", err)
	}
	logger.Info("rest server successfully shutdown.")

	// close extend services
	for k, v := range servers {
		logger.Infof("close service %s", k)
		v.close()
	}

	os.Exit(0)
}

func initRuleset() error {
	loc, err := conf.GetDataLoc()
	if err != nil {
		return err
	}
	msg1 := fmt.Sprintf("initRuleset....................... %s \n", loc)
	fmt.Printf(msg1)
	signalFile := filepath.Join(loc, "initialized")
	if _, err := os.Stat(signalFile); errors.Is(err, os.ErrNotExist) {
		fmt.Println("does this called ................................")

		defer os.Create(signalFile)
		content, err := os.ReadFile(filepath.Join(loc, "init.json"))
		if err != nil {
			conf.Log.Infof("fail to read init file: %v", err)
			return nil
		}
		conf.Log.Infof("start to initialize ruleset")
		_, counts, err := rulesetProcessor.Import(content)
		conf.Log.Infof("initialzie %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
		msg12 := fmt.Sprintf("initialzie %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
		fmt.Printf(msg12)
	}
	return nil
}

func resetAllRules() error {
	rules, err := ruleProcessor.GetAllRules()
	if err != nil {
		return err
	}
	for _, name := range rules {
		_ = deleteRule(name)
		_, err := ruleProcessor.ExecDrop(name)
		if err != nil {
			logger.Warnf("delete rule: %s with error %v", name, err)
			continue
		}
	}
	return nil
}

func resetAllStreams() error {
	allStreams, err := streamProcessor.GetAll()
	if err != nil {
		return err
	}
	Streams := allStreams["streams"]
	Tables := allStreams["tables"]

	for name, _ := range Streams {
		_, err2 := streamProcessor.DropStream(name, ast.TypeStream)
		if err2 != nil {
			logger.Warnf("streamProcessor DropStream %s error: %v", name, err2)
			continue
		}
	}
	for name, _ := range Tables {
		_, err2 := streamProcessor.DropStream(name, ast.TypeTable)
		if err2 != nil {
			logger.Warnf("streamProcessor DropTable %s error: %v", name, err2)
			continue
		}
	}
	return nil
}

func accepetTransferedrule() {

	nc, err1 := nats.Connect("nats://10.66.66.1:4222")
	if err1 != nil {
		panic(err1)
	}

	lines := make(chan *nats.Msg, 8*1024)
	// ChanSubscribe will put all messages it received into the channnel
	_, err := nc.ChanSubscribe(TRANSFER, lines)
	if err != nil {
		panic(err)
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

			sec := map[string]interface{}{}
			if err := json.Unmarshal([]byte(the_rulejson), &sec); err != nil {
				continue
			}
			fmt.Println(sec)

			keys := make([]string, 0, len(sec))
			for k := range sec {
				keys = append(keys, k)
			}

			if contains(keys, "sql") == false {
				continue
			}
			//juedge whetehr the rulejosn is in ruleprocessor
			isinruledb := ruleProcessor.JudgeWhetherRuleJsonInDb(the_rulejson)
			if isinruledb == true {
				continue
			}

			//create statment for letting p2p source ready
			stmt, _ := xsql.GetStatementFromSql(sec["sql"].(string))
			streamsFromStmt := xsql.GetStreams(stmt)
			fmt.Printf("streamsFromStmt: %v\n", streamsFromStmt)
			if len(streamsFromStmt) <= 0 {
				continue
			}
			createflag := true
			for _, vss := range streamsFromStmt {

				isbinary := strings.Contains(sec["sql"].(string), "(self)") // whehether the stream offers json or binary data
				streamcreationstatement := "create stream " + " " + vss + " ()" + " WITH (DATASOURCE="
				//sensor1 () WITH (DATASOURCE=\"home/+/sensor1\", FORMAT=\"JSON\", TYPE=\"memory\")"
				if isbinary == true {
					streamcreationstatement = streamcreationstatement + "\"" + vss + "\"" + "," + "FORMAT=\"BINARY\"," + "TYPE=\"p2p\")"
				} else {
					streamcreationstatement = streamcreationstatement + "\"" + vss + "\"" + "," + "FORMAT=\"JSON\"," + "TYPE=\"p2p\")"
				}

				if _, err := streamProcessor.ExecStmt(streamcreationstatement); err != nil {
					createflag = false
					fmt.Println("creation stream faild.....", err)
					break
				}

			}

			if createflag == false {
				continue
			}
			rule, err := ruleProcessor.GetRuleForPeer(the_rulejson)
			if err != nil {
				panic(err)
			}
			reply := createRuleForPeer(rule)
			if 0 != len(reply) {
				logger.Info(reply)
			}

		}
	}
}
