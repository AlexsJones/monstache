package find

import (
	"github.com/globalsign/mgo"
	"github.com/robertkrimen/otto"
)

type FindCall struct {
	Config  *FindConf
	Session *mgo.Session
	Query   interface{}
	Db      string
	Col     string
	Limit   int
	Sort    []string
	Sel     map[string]int
}

type FindConf struct {
	Vm      *otto.Otto
	Ns      string
	Name    string
	Session *mgo.Session
	ById    bool
	Multi   bool
}
