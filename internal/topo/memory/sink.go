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

package memory

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"strings"
)

type sink struct {
	topic string
}

func (s *sink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening memory sink: %v", s.topic)
	createPub(s.topic)
	return nil
}

func (s *sink) Configure(props map[string]interface{}) error {
	if t, ok := props[IdProperty]; ok {
		if id, casted := t.(string); casted {
			if strings.ContainsAny(id, "#+") {
				return fmt.Errorf("invalid memory topic %s: wildcard found", id)
			}
			s.topic = id
			return nil
		} else {
			return fmt.Errorf("can't cast value %s to string", t)
		}
	}
	return fmt.Errorf("there is no topic property in the memory action")
}

func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	ctx.GetLogger().Debugf("receive %+v", data)
	var outs []map[string]interface{}
	switch d := data.(type) {
	case []map[string]interface{}:
		outs = d
	case map[string]interface{}:
		outs = append(outs, d)
	default:
		return fmt.Errorf("unrecognized format of %s", data)
	}
	for _, el := range outs {
		produce(ctx, s.topic, el)
	}
	return nil
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing memory sink")
	return closeSink(s.topic)
}
