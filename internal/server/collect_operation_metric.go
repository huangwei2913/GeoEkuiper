package server

// func GetOpetationMetric() {
// 	collectoperationlatency := time.NewTicker(1000 * time.Millisecond)
// 	for {
// 		select {
// 		case <-collectoperationlatency.C:
// 			ks := globalcache.Items()
// 			for k, _ := range ks {
// 				tmpRuleState, found := globalcache.Get(k)
// 				if found {
// 					rssatus, _ := tmpRuleState.(*rule.RuleState)
// 					result, _ := rssatus.GetState()
// 					if result == "Running" {
// 						tmpdb := rssatus.Topology.Db
// 						txn := tmpdb.Txn(false)
// 						defer txn.Abort()

// 						it, err := txn.Get("operation", "ruleid", rssatus.RuleId)
// 						if err != nil {
// 							panic(err)
// 						}
// 						//seach which one is regarding the sources
// 						type Group struct {
// 							Sum   float64
// 							Count int
// 						}
// 						groups := make(map[string]*Group)

// 						for obj := it.Next(); obj != nil; obj = it.Next() {
// 							operation := obj.(*node.OperationMetric)

// 							intime, err := time.Parse(time.RFC3339Nano, operation.GetIntime())
// 							if err != nil {
// 								panic(err)
// 							}

// 							outtime, err := time.Parse(time.RFC3339Nano, operation.GetOuttime())
// 							if err != nil {
// 								panic(err)
// 							}

// 							duration := math.Abs(outtime.Sub(intime).Seconds())

// 							group, ok := groups[operation.GetOp()]
// 							if !ok {
// 								group = &Group{}
// 								groups[operation.GetOp()] = group
// 							}

// 							group.Sum += duration
// 							group.Count++
// 						}
// 						results := make(map[string]float64)

// 						for op, group := range groups {
// 							results[op] = group.Sum / float64(group.Count)
// 						}
// 						rulesopeationscache.Set(rssatus.RuleId, results, 0)
// 					}
// 				}
// 			}

// 		}
// 	}

// }
