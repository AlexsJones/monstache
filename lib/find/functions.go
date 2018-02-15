package find

import (
	"errors"
	"strings"

	"github.com/AlexsJones/monstache/lib/log"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/robertkrimen/otto"
)

func (fc *FindCall) setDatabase(topts map[string]interface{}) (err error) {
	if ov, ok := topts["database"]; ok {
		if ovs, ok := ov.(string); ok {
			fc.db = ovs
		} else {
			err = errors.New("Invalid database option value")
		}
	}
	return
}

func (fc *FindCall) setCollection(topts map[string]interface{}) (err error) {
	if ov, ok := topts["collection"]; ok {
		if ovs, ok := ov.(string); ok {
			fc.col = ovs
		} else {
			err = errors.New("Invalid collection option value")
		}
	}
	return
}

func (fc *FindCall) setSelect(topts map[string]interface{}) (err error) {
	if ov, ok := topts["select"]; ok {
		if ovsel, ok := ov.(map[string]interface{}); ok {
			for k, v := range ovsel {
				if vi, ok := v.(int64); ok {
					fc.sel[k] = int(vi)
				}
			}
		} else {
			err = errors.New("Invalid select option value")
		}
	}
	return
}

func (fc *FindCall) setSort(topts map[string]interface{}) (err error) {
	if ov, ok := topts["sort"]; ok {
		if ovs, ok := ov.([]string); ok {
			fc.sort = ovs
		} else {
			err = errors.New("Invalid sort option value")
		}
	}
	return
}

func (fc *FindCall) setLimit(topts map[string]interface{}) (err error) {
	if ov, ok := topts["limit"]; ok {
		if ovl, ok := ov.(int64); ok {
			fc.limit = int(ovl)
		} else {
			err = errors.New("Invalid limit option value")
		}
	}
	return
}

func (fc *FindCall) setQuery(v otto.Value) (err error) {
	var q interface{}
	if q, err = v.Export(); err == nil {
		fc.query = fc.restoreIds(q)
	}
	return
}

func (fc *FindCall) setOptions(v otto.Value) (err error) {
	var opts interface{}
	if opts, err = v.Export(); err == nil {
		switch topts := opts.(type) {
		case map[string]interface{}:
			if err = fc.setDatabase(topts); err != nil {
				return
			}
			if err = fc.setCollection(topts); err != nil {
				return
			}
			if err = fc.setSelect(topts); err != nil {
				return
			}
			if fc.isMulti() {
				if err = fc.setSort(topts); err != nil {
					return
				}
				if err = fc.setLimit(topts); err != nil {
					return
				}
			}
		default:
			err = errors.New("Invalid options argument")
			return
		}
	} else {
		err = errors.New("Invalid options argument")
	}
	return
}

func (fc *FindCall) setDefaults() {
	ns := strings.Split(fc.config.ns, ".")
	fc.db = ns[0]
	fc.col = ns[1]
}

func (fc *FindCall) getCollection() *mgo.Collection {
	return fc.session.DB(fc.db).C(fc.col)
}

func (fc *FindCall) getVM() *otto.Otto {
	return fc.config.vm
}

func (fc *FindCall) getFunctionName() string {
	return fc.config.name
}

func (fc *FindCall) isMulti() bool {
	return fc.config.multi
}

func (fc *FindCall) logError(err error) {
	log.GetInstance().ErrorLog.Printf("Error in function %s: %s\n", fc.getFunctionName(), err)
}

func (fc *FindCall) restoreIds(v interface{}) (r interface{}) {
	switch vt := v.(type) {
	case string:
		if bson.IsObjectIdHex(vt) {
			r = bson.ObjectIdHex(vt)
		} else {
			r = v
		}
	case []interface{}:
		var avs []interface{}
		for _, av := range vt {
			avs = append(avs, fc.restoreIds(av))
		}
		r = avs
	case map[string]interface{}:
		mvs := make(map[string]interface{})
		for k, v := range vt {
			mvs[k] = fc.restoreIds(v)
		}
		r = mvs
	default:
		r = v
	}
	return
}

func (fc *FindCall) execute() (r otto.Value, err error) {
	col := fc.getCollection()
	if fc.isMulti() {
		var docs []map[string]interface{}
		mq := col.Find(fc.query)
		if fc.limit > 0 {
			mq.Limit(fc.limit)
		}
		if len(fc.sort) > 0 {
			mq.Sort(fc.sort...)
		}
		if len(fc.sel) > 0 {
			mq.Select(fc.sel)
		}
		if err = mq.All(&docs); err == nil {
			r, err = fc.getVM().ToValue(docs)
		}
	} else {
		doc := make(map[string]interface{})
		if fc.config.byId {
			if err = col.FindId(fc.query).One(doc); err == nil {
				r, err = fc.getVM().ToValue(doc)
			}
		} else {
			if err = col.Find(fc.query).One(doc); err == nil {
				r, err = fc.getVM().ToValue(doc)
			}
		}
	}
	return
}

func makeFind(fa *FindConf) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) (r otto.Value) {
		var err error
		fc := &FindCall{
			config:  fa,
			session: fa.session.Copy(),
			sel:     make(map[string]int),
		}
		defer fc.session.Close()
		fc.setDefaults()
		args := call.ArgumentList
		argLen := len(args)
		r = otto.NullValue()
		if argLen >= 1 {
			if argLen >= 2 {
				if err = fc.setOptions(call.Argument(1)); err != nil {
					fc.logError(err)
					return
				}
			}
			if err = fc.setQuery(call.Argument(0)); err == nil {
				var result otto.Value
				if result, err = fc.execute(); err == nil {
					r = result
				} else {
					fc.logError(err)
				}
			} else {
				fc.logError(err)
			}
		} else {
			fc.logError(errors.New("At least one argument is required"))
		}
		return
	}
}
