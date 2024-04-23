package server

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"io"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	uuid "github.com/satori/go.uuid"
)

func GetALLMetricsFromDB(db *memdb.MemDB) ([]*conf.RuleMetric, error) {
	txn := db.Txn(false)
	defer txn.Abort()
	it, err := txn.Get("metrics", "LastCompletiontime")
	if err != nil {
		return nil, err
	}
	var results []*conf.RuleMetric
	for obj := it.Next(); obj != nil; obj = it.Next() {
		rm := obj.(*conf.RuleMetric)
		results = append(results, rm)
	}
	return results, nil
}

func GetMetricsInLastTenMinutes(db *memdb.MemDB) ([]*conf.RuleMetric, error) {
	past := time.Now().Add(-10 * time.Minute)
	txn := db.Txn(false)
	defer txn.Abort()
	it, err := txn.Get("metrics", "LastCompletiontime")
	if err != nil {
		return nil, err
	}
	var results []*conf.RuleMetric
	for obj := it.Next(); obj != nil; obj = it.Next() {
		rm := obj.(*conf.RuleMetric)
		t, err := time.Parse(time.RFC3339Nano, rm.LastCompletiontime)
		if err != nil {
			return nil, err
		}
		if t.After(past) {
			results = append(results, rm)
		}
	}
	return results, nil
}

func getAllRuleMetricsFromRuleStaticCache() []ruleMetrics {
	var rules []ruleMetrics
	for _, item := range rulestaticcache.Items() {
		rule, ok := item.Object.(*ruleMetrics)
		if ok {
			rules = append(rules, *rule)
		}
	}
	return rules
}

func groupByRuleIDFromRuleMtrics(metrics []ruleMetrics) map[string]int {
	result := make(map[string]int)
	for _, metric := range metrics {
		result[metric.Ruleid]++
	}
	return result
}

// func findLeastUsedRule(ruleMetricsSlice []ruleMetrics, ruleIds []string) ruleMetrics {
// 	rand.Shuffle(len(ruleMetricsSlice), func(i, j int) {
// 		ruleMetricsSlice[i], ruleMetricsSlice[j] = ruleMetricsSlice[j], ruleMetricsSlice[i]
// 	})
// 	ruleIdCount := make(map[string]int)
// 	for _, ruleId := range ruleIds {
// 		ruleIdCount[ruleId]++
// 	}
// 	var leastUsedRule ruleMetrics
// 	minCount := math.MaxInt32
// 	for _, rule := range ruleMetricsSlice {
// 		if count, ok := ruleIdCount[rule.Ruleid]; ok && count < minCount {
// 			leastUsedRule = rule
// 			minCount = count
// 		}
// 	}
// 	return leastUsedRule
// }

func findLeastUsedRule(ruleMetricsSlice []ruleMetrics, ruleIds []string) ruleMetrics {
	rand.Shuffle(len(ruleMetricsSlice), func(i, j int) {
		ruleMetricsSlice[i], ruleMetricsSlice[j] = ruleMetricsSlice[j], ruleMetricsSlice[i]
	})
	ruleIdCount := make(map[string]int)
	for _, ruleId := range ruleIds {
		ruleIdCount[ruleId]++
	}
	var leastUsedRule ruleMetrics
	minCount := math.MaxInt32
	for _, rule := range ruleMetricsSlice {
		if count, ok := ruleIdCount[rule.Ruleid]; ok && count < minCount {
			leastUsedRule = rule
			minCount = count
		}
	}
	if minCount == math.MaxInt32 && len(ruleIds) > 0 {
		rand.Shuffle(len(ruleIds), func(i, j int) {
			ruleIds[i], ruleIds[j] = ruleIds[j], ruleIds[i]
		})
		leastUsedRule = ruleMetrics{Ruleid: ruleIds[0]}
	}
	return leastUsedRule
}

func GetAllRuleIds() []string {
	allruledis := make([]string, 0)
	ks := globalcache.Items()
	for k, _ := range ks {
		tmpRuleState, found := globalcache.Get(k)
		if found {
			rssatus, _ := tmpRuleState.(*rule.RuleState)
			allruledis = append(allruledis, rssatus.Rule.Id)
		}
	}
	return allruledis
}

func getRuleStateByRuleid(ruleid string) (string, error) {
	ks := globalcache.Items()
	for k, _ := range ks {
		tmpRuleState, found := globalcache.Get(k)
		if found {
			rssatus, _ := tmpRuleState.(*rule.RuleState)
			if rssatus.RuleId != ruleid {
				continue
			}
			return rssatus.GetState()
		}
	}
	return "", nil
}

func restartruleworker(queue chan string) {
	for ruleid := range queue {
		delay := time.Duration(rand.Intn(10)+1) * time.Second
		time.Sleep(delay)
		ks := globalcache.Items()
		for k, _ := range ks {
			tmpRuleState, found := globalcache.Get(k)
			if found {
				rssatus, _ := tmpRuleState.(*rule.RuleState)
				result, _ := rssatus.GetState()
				if result != "Running" {
					if rssatus.RuleId == ruleid {
						if rssatus.Transfered == false {
							rssatus.Start()
						} else {
							rssatus.StartForPeer()
						}
					}
				}
			}
		}

	}
}

func stopRuleByRuleid(ruleid string) {
	ks := globalcache.Items()
	for k, _ := range ks {
		tmpRuleState, found := globalcache.Get(k)
		if found {
			rssatus, _ := tmpRuleState.(*rule.RuleState)
			result, _ := rssatus.GetState()
			if result == "Running" {
				if rssatus.RuleId == ruleid {
					//we should first collect the metrics
					allmetricss, _ := GetALLMetricsFromDB(rssatus.Topology.Db)
					for _, rmetric := range allmetricss {
						f, err := strconv.ParseFloat(rmetric.Timespent, 64)
						if err != nil {
							continue
						}

						processedcount, err := strconv.Atoi(rmetric.ProcessedOperationcount)
						if err != nil {
							panic(err)
						}

						inputmessagecount, er1 := strconv.Atoi(rmetric.InputMessageCount)
						if er1 != nil {
							panic(er1)
						}

						myoperationcost, err2 := decompressString(rmetric.OperatorCost)
						if err2 != nil {
							panic(err2)
						}
						total := sumValues(myoperationcost)

						rulestaticcache.Set(string(uuid.NewV4().String()),
							&ruleMetrics{Ruleid: rssatus.RuleId, Timespent: f,
								Isremote:              rssatus.Transfered,
								RuleSql:               rssatus.Rule.Sql,
								LastCompletiontime:    rmetric.LastCompletiontime,
								SamplingTime:          time.Now().Format(time.RFC3339Nano),
								InputMessageCount:     inputmessagecount,
								ProcessedMessageCount: processedcount,
								OperatorCost:          total,
							}, 0)
					}
					rssatus.Stop()
				}
			}
		}
	}
}

func decompressString(s string) (map[string]float64, error) {
	b := []byte(s)
	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	var result map[string]float64
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		return nil, err
	}
	return result, nil
}

func sumValues(m map[string]float64) float64 {
	var sum float64
	for _, v := range m {
		sum += v
	}
	return sum
}

//这个函数接受一个 ruleMetrics 类型的切片作为参数，并返回过去十分钟内 InputMessageCount 最多的 ruleMetrics 元素。
func getMostInputMessagesRule(rules []ruleMetrics) ruleMetrics {
	var maxRule ruleMetrics
	maxInputMessages := 0
	for _, rule := range rules {
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < 10*time.Minute && rule.InputMessageCount > maxInputMessages {
			maxInputMessages = rule.InputMessageCount
			maxRule = rule
		}
	}
	return maxRule
}

func getLeastInputMessagesRule(rules []ruleMetrics) ruleMetrics {
	var minRule ruleMetrics
	minInputMessages := math.MaxInt64
	for _, rule := range rules {
		t, err := time.Parse(time.RFC3339Nano, rule.LastCompletiontime)
		if err != nil {
			continue
		}
		if time.Since(t) < 10*time.Minute && rule.InputMessageCount < minInputMessages {
			minInputMessages = rule.InputMessageCount
			minRule = rule
		}
	}
	return minRule
}

//要增加缓冲区数量，增加并发数量
func AddResourceToRule(queue chan string) {

	for ruleid := range queue {
		ks := globalcache.Items()
		for k, _ := range ks {
			tmpRuleState, found := globalcache.Get(k)
			if found {
				rssatus, _ := tmpRuleState.(*rule.RuleState)
				//result, _ := rssatus.GetState()
				if rssatus.RuleId == ruleid {
					if rssatus.Transfered == false {
						the_ruleoption := rssatus.Rule.Options
						concurrency := the_ruleoption.Concurrency
						bufferlength := the_ruleoption.BufferLength
						concurrency = concurrency + 1

						if concurrency > 5 {
							concurrency = 5
						}
						totobuflength := bufferlength + int(0.1*float64(bufferlength))
						if totobuflength > 3000 {
							totobuflength = 3000
						}
						rssatus.SetRuleOption(concurrency, totobuflength)
						stopRuleByRuleid(rssatus.RuleId)
						stopRule(rssatus.RuleId)
						time.Sleep(5 * time.Millisecond)
						rssatus.Start()
					} //
				} //helping with each other, not to stop

			}
		}

	}
}

//要增加缓冲区数量，增加并发数量
func DecreaseResourceFromRule(queue chan string) {

	for ruleid := range queue {
		ks := globalcache.Items()
		for k, _ := range ks {
			tmpRuleState, found := globalcache.Get(k)
			if found {
				rssatus, _ := tmpRuleState.(*rule.RuleState)
				if rssatus.RuleId == ruleid {
					if rssatus.Transfered == false {
						the_ruleoption := rssatus.Rule.Options
						concurrency := the_ruleoption.Concurrency
						bufferlength := the_ruleoption.BufferLength
						concurrency = concurrency - 1
						if concurrency <= 0 {
							concurrency = 1
						}
						totobuflength := bufferlength - int(0.1*float64(bufferlength))
						if totobuflength <= 0 {
							totobuflength = 10
						}
						rssatus.SetRuleOption(concurrency, totobuflength)
						stopRuleByRuleid(rssatus.RuleId)
						rssatus.Stop()
						time.Sleep(5 * time.Millisecond)
						rssatus.Start()
					} //
				} //helping with each other, not to stop

			}
		}

	}
}

//periodcally get rules
func getrulemetrics() {

	collectRuleMetrics := time.NewTicker(5000 * time.Millisecond)
	myscheulder := time.NewTicker(10000 * time.Millisecond)
	restartqueue := make(chan string)
	addresourcequeue := make(chan string)
	decreaseresourcequeue := make(chan string)

	go restartruleworker(restartqueue)
	go AddResourceToRule(addresourcequeue)
	go DecreaseResourceFromRule(decreaseresourcequeue)

	go func() {
		for {
			select {

			case <-myscheulder.C:
				allrulesmetrics := getAllRuleMetricsFromRuleStaticCache() // 获取所有 ruleMetrics
				groupedMetrics := groupByRuleIDFromRuleMtrics(allrulesmetrics)
				numUniqueRuleIDs := len(groupedMetrics)
				//fmt.Printf("Number of unique Ruleids...HHHHHHHHHHHH.: %d\n", numUniqueRuleIDs)
				if numUniqueRuleIDs < 5 {
					for k := range groupedMetrics {
						delete(groupedMetrics, k)
					}
					continue //not over ten rule are running, we donot necessary lanuch scheduling
				}
				leastuesedrule := findLeastUsedRule(allrulesmetrics, GetAllRuleIds())
				//every time we stop the rule ,we should collect the metrics tables
				stopRuleByRuleid(leastuesedrule.Ruleid)
				go func(ruleidd string) {
					restartqueue <- ruleidd
				}(leastuesedrule.Ruleid)
				//
				maxincomingmessageRule := getMostInputMessagesRule(allrulesmetrics)
				//we find the max incoming message rules
				go func(ruleidd string) {
					addresourcequeue <- ruleidd
				}(maxincomingmessageRule.Ruleid)

				minincomingmessageRule := getLeastInputMessagesRule(allrulesmetrics)
				//we find the max incoming message rules
				go func(ruleidd string) {
					decreaseresourcequeue <- ruleidd
				}(minincomingmessageRule.Ruleid)

			case <-collectRuleMetrics.C:
				ks := globalcache.Items()
				for k, _ := range ks {
					tmpRuleState, found := globalcache.Get(k)
					if found {
						rssatus, _ := tmpRuleState.(*rule.RuleState)
						//read metric table
						metrics, err := GetMetricsInLastTenMinutes(rssatus.Topology.Db)
						if err != nil || len(metrics) <= 0 {
							continue
						}
						for _, rm := range metrics {
							f, err := strconv.ParseFloat(rm.Timespent, 64)
							if err != nil {
								continue
							}

							processedcount, err := strconv.Atoi(rm.ProcessedOperationcount)
							if err != nil {
								panic(err)
							}

							inputmessagecount, er1 := strconv.Atoi(rm.InputMessageCount)
							if er1 != nil {
								panic(er1)
							}

							myoperationcost, err2 := decompressString(rm.OperatorCost)
							if err2 != nil {
								panic(err2)
							}
							total := sumValues(myoperationcost)

							rulestaticcache.Set(string(uuid.NewV4().String()),
								&ruleMetrics{Ruleid: rssatus.RuleId, Timespent: f,
									Isremote:              rssatus.Transfered,
									RuleSql:               rssatus.Rule.Sql,
									LastCompletiontime:    rm.LastCompletiontime,
									SamplingTime:          time.Now().Format(time.RFC3339Nano),
									InputMessageCount:     inputmessagecount,
									ProcessedMessageCount: processedcount,
									OperatorCost:          total,
								}, 0)
						}

					}

				}

			}
		}
	}()

}
