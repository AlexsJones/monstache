package find

import (
	"github.com/globalsign/mgo"
	"github.com/robertkrimen/otto"
)

type FindCall struct {
	config  *FindConf
	session *mgo.Session
	query   interface{}
	db      string
	col     string
	limit   int
	sort    []string
	sel     map[string]int
}

type FindConf struct {
	vm      *otto.Otto
	ns      string
	name    string
	session *mgo.Session
	byId    bool
	multi   bool
}
