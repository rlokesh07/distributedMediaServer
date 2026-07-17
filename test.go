package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type reqMsg struct {
	endname string
	svcMeth string
	argType reflect.Type
	args []byte
	replyChan chan rplMsg
}

type rplMsg struct {
	ok bool
	reply []byte
}


type Service struct {
	name string
	rcvr reflect.Value
	typ reflect.Type
	methods map[string]reflect.Method
}


func MakeService(rcvr interface{}){
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++{
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name
		if method.PkgPath == "" && mtype.NumIn() == 3 && mtype.NumOut() == 0{
			svc.methods[mname] = method
		}
	}
}

func(svc *Service) dispatch(methodName string, req reqMsg) rplMsg {
	if method, ok := svc.methods[methodName]; ok {

		args := reflect.New(req.argType)

		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface())

		replyType := method.Type.In(2).Elem()
		replyValue := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyValue})
		
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.Encode(replyValue)
		

		return rplMsg{true, rb.Bytes()}
	}
}



func main(){
	requests := make(chan Request, 100)
	var wg sync.WaitGroup
	go server(requests)
	defer close(requests)

	go client(requests, wg)
	
	wg.Wait()
	
}	
