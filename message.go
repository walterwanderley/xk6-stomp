package stomp

import (
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"
	"github.com/go-stomp/stomp/v3"
	"github.com/tidwall/gjson"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

// Message is a decorator to add string and json methods
type Message struct {
	*stomp.Message
	vu            modules.VU
	cachedJSON    interface{}
	validatedJSON bool
}

func (m *Message) String() string {
	return string(m.Body)
}

func (m *Message) JSON(selector ...string) goja.Value {
	rt := m.vu.Runtime()

	if m.Body == nil {
		err := fmt.Errorf("the body is null so we can't transform it to JSON")
		common.Throw(rt, err)
	}

	hasSelector := len(selector) > 0
	if m.cachedJSON == nil || hasSelector { //nolint:nestif
		var v interface{}

		body, err := common.ToBytes(m.Body)
		if err != nil {
			common.Throw(rt, err)
		}

		if hasSelector {
			if !m.validatedJSON {
				if !gjson.ValidBytes(body) {
					return goja.Undefined()
				}
				m.validatedJSON = true
			}

			result := gjson.GetBytes(body, selector[0])

			if !result.Exists() {
				return goja.Undefined()
			}
			return rt.ToValue(result.Value())
		}

		if err := json.Unmarshal(body, &v); err != nil {
			common.Throw(rt, err)
		}
		m.cachedJSON = v
	}

	return rt.ToValue(m.cachedJSON)
}
