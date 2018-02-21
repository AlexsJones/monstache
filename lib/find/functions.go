package find

import (
	"errors"
	"strings"

	"github.com/AlexsJones/monstache/lib/log"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/robertkrimen/otto"
)

func (fc *FindCall) SetDatabase(topts map[string]interface{}) (err error) {
	if ov, ok := topts["database"]; ok {
		if ovs, ok := ov.(string); ok {
			fc.Db = ovs
		} else {
			err = errors.New("Invalid database option value")
		}
	}
	return
}

func (fc *FindCall) SetCollection(topts map[string]interface{}) (err error) {
	if ov, ok := topts["collection"]; ok {
		if ovs, ok := ov.(string); ok {
			fc.Col = ovs
		} else {
			err = errors.New("Invalid collection option value")
		}
	}
	return
}

func (fc *FindCall) SetSelect(topts map[string]interface{}) (err error) {
	if ov, ok := topts["select"]; ok {
		if ovsel, ok := ov.(map[string]interface{}); ok {
			for k, v := range ovsel {
				if vi, ok := v.(int64); ok {
					fc.Sel[k] = int(vi)
				}
			}
		} else {
			err = errors.New("Invalid select option value")
		}
	}
	return
}

func (fc *FindCall) SetSort(topts map[string]interface{}) (err error) {
	if ov, ok := topts["sort"]; ok {
		if ovs, ok := ov.([]string); ok {
			fc.Sort = ovs
		} else {
			err = errors.New("Invalid sort option value")
		}
	}
	return
}

func (fc *FindCall) SetLimit(topts map[string]interface{}) (err error) {
	if ov, ok := topts["limit"]; ok {
		if ovl, ok := ov.(int64); ok {
			fc.Limit = int(ovl)
		} else {
			err = errors.New("Invalid limit option value")
		}
	}
	return
}

func (fc *FindCall) SetQuery(v otto.Value) (err error) {
	var q interface{}
	if q, err = v.Export(); err == nil {
		fc.Query = fc.restoreIds(q)
	}
	return
}

func (fc *FindCall) SetOptions(v otto.Value) (err error) {
	var opts interface{}
	if opts, err = v.Export(); err == nil {
		switch topts := opts.(type) {
		case map[string]interface{}:
			if err = fc.SetDatabase(topts); err != nil {
				return
			}
			if err = fc.SetCollection(topts); err != nil {
				return
			}
			if err = fc.SetSelect(topts); err != nil {
				return
			}
			if fc.isMulti() {
				if err = fc.SetSort(topts); err != nil {
					return
				}
				if err = fc.SetLimit(topts); err != nil {
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
	ns := strings.Split(fc.Config.Ns, ".")
	fc.Db = ns[0]
	fc.Col = ns[1]
}

func (fc *FindCall) getCollection() *mgo.Collection {
	return fc.Session.DB(fc.Db).C(fc.Col)
}

func (fc *FindCall) getVM() *otto.Otto {
	return fc.Config.Vm
}

func (fc *FindCall) getFunctionName() string {
	return fc.Config.Name
}

func (fc *FindCall) isMulti() bool {
	return fc.Config.Multi
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
		mq := col.Find(fc.Query)
		if fc.Limit > 0 {
			mq.Limit(fc.Limit)
		}
		if len(fc.Sort) > 0 {
			mq.Sort(fc.Sort...)
		}
		if len(fc.Sel) > 0 {
			mq.Select(fc.Sel)
		}
		if err = mq.All(&docs); err == nil {
			r, err = fc.getVM().ToValue(docs)
		}
	} else {
		doc := make(map[string]interface{})
		if fc.Config.ById {
			if err = col.FindId(fc.Query).One(doc); err == nil {
				r, err = fc.getVM().ToValue(doc)
			}
		} else {
			if err = col.Find(fc.Query).One(doc); err == nil {
				r, err = fc.getVM().ToValue(doc)
			}
		}
	}
	return
}

func MakeFind(fa *FindConf) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) (r otto.Value) {
		var err error
		fc := &FindCall{
			Config:  fa,
			Session: fa.Session.Copy(),
			Sel:     make(map[string]int),
		}
		defer fc.Session.Close()
		fc.setDefaults()
		args := call.ArgumentList
		argLen := len(args)
		r = otto.NullValue()
		if argLen >= 1 {
			if argLen >= 2 {
				if err = fc.SetOptions(call.Argument(1)); err != nil {
					fc.logError(err)
					return
				}
			}
			if err = fc.SetQuery(call.Argument(0)); err == nil {
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
