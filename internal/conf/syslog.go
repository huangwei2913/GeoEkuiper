// Copyright 2021 EMQ Technologies Co., Ltd.
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

//go:build !windows
// +build !windows

package conf

import (
	"log/syslog"
	"os"

	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

type Ruleid2sql struct { //the struct is used for storing source tuples for distributed executing
	UUID string // the uuid of the ruleid
	SQL  string // the name of the source node
	PEER string // whether the rule is for peer
}

//whether a rule is done periodcally, to remove uncesaary storage
// type RuleDoneFlag struct {
// 	UUID     string // uuid reprsent ruleid
// 	DONE     string // every time there are only pssobile two value, one is false , the other is true, to repsent a task is done
// 	DONETIME string //completion time
// }

type RuleMetric struct {
	UUID                    string
	Timespent               string
	LastCompletiontime      string
	OperatorCost            string
	ProcessedOperationcount string
	InputMessageCount       string
}

func initSyslog() {
	if "true" == os.Getenv(KuiperSyslogKey) {
		if hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_INFO, ""); err != nil {
			Log.Error("Unable to connect to local syslog daemon")
		} else {
			Log.AddHook(hook)
		}
	}
}
