package types

import (
	"errors"

	"github.com/globalsign/mgo"
	"github.com/robertkrimen/otto"
)

type FindConf struct {
	vm      *otto.Otto
	ns      string
	name    string
	session *mgo.Session
	byId    bool
	multi   bool
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
