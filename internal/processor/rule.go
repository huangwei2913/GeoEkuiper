// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package processor

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"github.com/patrickmn/go-cache"
	uuid "github.com/satori/go.uuid"
)

type RuleProcessor struct {
	Db                kv.KeyValue
	Ruleid2rulejsonDb cache.Cache
}

func NewRuleProcessor() *RuleProcessor {
	err, db := store.GetKV("rule")
	if err != nil {
		panic(fmt.Sprintf("Can not initialize store for the rule processor at path 'rule': %v", err))
	}
	Name2RuleidDb := cache.New(500*time.Minute, 10000*time.Minute)
	processor := &RuleProcessor{
		Db:                db,
		Ruleid2rulejsonDb: *Name2RuleidDb,
	}
	return processor
}

func (p *RuleProcessor) ExecCreateWithValidation(name, ruleJson string) (*api.Rule, error) {
	rule, err := p.GetRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}
	err = p.Db.Setnx(rule.Id, ruleJson)
	if err != nil {
		return nil, err
	} else {
		log.Infof("Rule %s is created.", rule.Id)
	}
	return rule, nil
}

func (p *RuleProcessor) ExecCreate(name, ruleJson string) error {
	err := p.Db.Setnx(name, ruleJson)
	if err != nil {
		return err
	} else {
		log.Infof("Rule %s is created.", name)
	}
	return nil
}

func (p *RuleProcessor) ExecUpdate(name, ruleJson string) (*api.Rule, error) {
	rule, err := p.GetRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}
	err = p.Db.Set(rule.Id, ruleJson)
	if err != nil {
		return nil, err
	} else {
		log.Infof("Rule %s is update.", rule.Id)
	}
	return rule, nil
}

func (p *RuleProcessor) ExecReplaceRuleState(name string, triggered bool) (err error) {
	rule, err := p.GetRuleById(name)
	if err != nil {
		return err
	}

	rule.Triggered = triggered
	ruleJson, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("Marshal rule %s error : %s.", name, err)
	}
	err = p.Db.Set(name, string(ruleJson))
	if err != nil {
		return err
	} else {
		log.Infof("Rule %s is replaced.", name)
	}
	return err
}

func (p *RuleProcessor) GetRuleJson(id string) (string, error) {

	tmpRulejson, found := p.Ruleid2rulejsonDb.Get(id)
	if found {
		return tmpRulejson.(string), nil
	}

	return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", id))

	// var s1 string
	// f, _ := p.Db.Get(id, &s1)
	// if !f {
	// 	return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", id))
	// }
	// return s1, nil
}

func (p *RuleProcessor) JudgeWhetherRuleJsonInDb(rulejson string) bool {

	ks := p.Ruleid2rulejsonDb.Items()
	for k, _ := range ks {
		tmpRulejson, found := p.Ruleid2rulejsonDb.Get(k)
		if found {
			the_rulejosn := tmpRulejson.(string)
			hash1 := md5.Sum([]byte(the_rulejosn))
			hash2 := md5.Sum([]byte(rulejson))
			if hash1 == hash2 || the_rulejosn == rulejson {
				return true
			}
		}
	}
	return false
}

func (p *RuleProcessor) GetRuleById(id string) (*api.Rule, error) {
	var s1 string
	f, _ := p.Db.Get(id, &s1)
	if !f {
		return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", id))
	}
	return p.GetRuleByJsonValidated(s1)
}

func (p *RuleProcessor) getDefaultRule(name, sql string) *api.Rule {
	return &api.Rule{
		Id:  name,
		Sql: sql,
		Options: &api.RuleOption{
			IsEventTime:        false,
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                api.AtMostOnce,
			CheckpointInterval: 300000,
			Restart: &api.RestartStrategy{
				Attempts:     0,
				Delay:        1000,
				Multiplier:   2,
				MaxDelay:     30000,
				JitterFactor: 0.1,
			},
		},
	}
}

// GetRuleByJsonValidated called when the json is getting from trusted source like db
func (p *RuleProcessor) GetRuleByJsonValidated(ruleJson string) (*api.Rule, error) {
	opt := conf.Config.Rule
	//set default rule options
	rule := &api.Rule{
		Triggered: true,
		Options:   clone(opt),
	}
	fmt.Println("parse rule from defined rule json file, and store into api.rule struct ...... ")
	fmt.Println("the rule json is.....", ruleJson)
	if err := json.Unmarshal([]byte(ruleJson), &rule); err != nil {
		return nil, fmt.Errorf("Parse rule %s error : %s.", ruleJson, err)
	}
	if rule.Id == "" {
		u2 := uuid.NewV4()
		rule.Id = u2.String()
	} else {

		fmt.Println("the rule.Id is not an uuid.........................................", rule.Id)
		_, err := uuid.FromString(rule.Id)
		if err != nil {
			fmt.Println("error rulejosn file.......................")
			return nil, fmt.Errorf("Parse rule %s error not uuid: %s.", ruleJson, err)
		}
	}
	if rule.Options == nil {
		rule.Options = &opt
	}
	p.Ruleid2rulejsonDb.Add(rule.Id, ruleJson, cache.NoExpiration)
	return rule, nil
}

//this function would bring about confusion about what is the rule object
//实际上这个函数是返回api.Rule对象，而不是rulejson, this id is in fact not used
func (p *RuleProcessor) GetRuleByJson(id, ruleJson string) (*api.Rule, error) {
	rule, err := p.GetRuleByJsonValidated(ruleJson)
	if err != nil {
		return rule, err
	}
	fmt.Println("the rule id  and id are............ in this place...", rule.Id, id)
	//validation
	if rule.Id == "" && id == "" {
		return nil, fmt.Errorf("Missing rule id.")
	}
	// if id != "" && rule.Id != "" && id != rule.Id {
	// 	return nil, fmt.Errorf("RuleId is not consistent with rule id.")
	// }
	// if rule.Id == "" {
	// 	rule.Id = id
	// }
	if rule.Sql != "" {
		if rule.Graph != nil {
			return nil, fmt.Errorf("Rule %s has both sql and graph.", rule.Id)
		}
		if _, err := xsql.GetStatementFromSql(rule.Sql); err != nil {
			return nil, err
		}
		if rule.Actions == nil || len(rule.Actions) == 0 {
			return nil, fmt.Errorf("Missing rule actions.")
		}
	} else {
		if rule.Graph == nil {
			return nil, fmt.Errorf("Rule %s has neither sql nor graph.", rule.Id)
		}
	}
	err = conf.ValidateRuleOption(rule.Options)
	if err != nil {
		return nil, fmt.Errorf("Rule %s has invalid options: %s.", rule.Id, err)
	}
	return rule, nil
}

func clone(opt api.RuleOption) *api.RuleOption {
	return &api.RuleOption{
		IsEventTime:        opt.IsEventTime,
		LateTol:            opt.LateTol,
		Concurrency:        opt.Concurrency,
		BufferLength:       opt.BufferLength,
		SendMetaToSink:     opt.SendMetaToSink,
		SendError:          opt.SendError,
		Qos:                opt.Qos,
		CheckpointInterval: opt.CheckpointInterval,
		Restart: &api.RestartStrategy{
			Attempts:     opt.Restart.Attempts,
			Delay:        opt.Restart.Delay,
			Multiplier:   opt.Restart.Multiplier,
			MaxDelay:     opt.Restart.MaxDelay,
			JitterFactor: opt.Restart.JitterFactor,
		},
	}
}

func (p *RuleProcessor) ExecDesc(name string) (string, error) {
	var s1 string
	f, _ := p.Db.Get(name, &s1)
	if !f {
		return "", fmt.Errorf("Rule %s is not found.", name)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, []byte(s1), "", "  "); err != nil {
		return "", err
	}

	return fmt.Sprintln(dst.String()), nil
}

func (p *RuleProcessor) GetAllRules() ([]string, error) {
	return p.Db.Keys()
}

func (p *RuleProcessor) GetAllRulesJson() (map[string]string, error) {
	return p.Db.All()
}

func (p *RuleProcessor) ExecDrop(name string) (string, error) {
	result := fmt.Sprintf("Rule %s is dropped.", name)
	var ruleJson string
	if ok, _ := p.Db.Get(name, &ruleJson); ok {
		if err := cleanSinkCache(name); err != nil {
			result = fmt.Sprintf("%s. Clean sink cache faile: %s.", result, err)
		}
		if err := cleanCheckpoint(name); err != nil {
			result = fmt.Sprintf("%s. Clean checkpoint cache faile: %s.", result, err)
		}

	}
	err := p.Db.Delete(name)
	if err != nil {
		return "", err
	} else {
		//p.Myruledb.Delete(name)
		return result, nil
	}
}

func cleanCheckpoint(name string) error {
	err := store.DropTS(name)
	if err != nil {
		return err
	}
	return nil
}

func cleanSinkCache(name string) error {
	err := store.DropCacheKVForRule(name)
	if err != nil {
		return err
	}
	return nil
}

func (p *RuleProcessor) GetRuleForPeer(ruleJson string) (*api.Rule, error) {

	isexisted := p.JudgeWhetherRuleJsonInDb(ruleJson)
	if isexisted == true {
		return nil, errorx.NewWithCode(errorx.GENERAL_ERR, fmt.Sprintf("Rule %s is already  existsed.", ruleJson))
	}
	return p.GetRuleByJsonValidated(ruleJson)

}
