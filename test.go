package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)



type ReqMsg struct {
	endname string
	svcMeth string
	argType reflect.Type
	args []byte
	replyChan chan RplMsg
}

type RplMsg struct {
	ok bool
	reply []byte
}


type Service struct {
	name string
	rcvr reflect.Value
	typ reflect.Type
	methods map[string]reflect.Method
}

type ClientEnd struct {
	endname string
	ch chan ReqMsg
	done chan struct{}
}

type Server struct {
	mu sync.Mutex
	services map[string]*Service
	count int
}

type Network struct {
	mu sync.Mutex
	ends map[string]*ClientEnd
	enabled map[string]bool
	servers map[string]*Server
	connections map[string]string
	endCh chan ReqMsg
	done chan struct{}
	count int32
}


func MakeNetwork() *Network {

	network := Network{}
	network.ends = map[string]*ClientEnd{}
	network.enabled = map[string]bool{}
	network.servers = map[string]*Server{}
	network.connections = map[string]string{}
	network.endCh = make(chan ReqMsg)
	network.done = make(chan struct{})
		
	go func(){
		for{
			select{
			case req := <- network.endCh:
				atomic.AddInt32(&network.count, 1)
				go network.processReq(req)
			case <- network.done:
				return 
			}
		}

	}()
	return &network
}

func(network *Network) processReq(req ReqMsg){
	network.mu.Lock()

	serverName, ok := network.connections[req.endname]
	var server *Server;
	if ok {
		server = network.servers[serverName]			
	} else {
		req.replyChan <- RplMsg{ok: false, reply: nil}
	}

	network.mu.Unlock()

	ech := make(chan RplMsg)
	go func() {
		ech <- server.dispatch(req)
	}()
		
	var rpl RplMsg


	replyOk := false
	timeout := false

	for !replyOk && !timeout {
		select{
		case rpl = <-ech:
			replyOk = true;
		case <- time.After(100 * time.Millisecond):
			
		}

	}

}

func(network *Network) IsServerDead(endname string, server *Server) bool {
	network.mu.Lock()
	defer network.mu.Unlock()

	if network.servers[endname] != server {
		return true
	}
	 return false
}

func(client *ClientEnd) Call(svcMeth string, args interface{}, resp interface{}) bool { // returns if the call was successful
	req := ReqMsg{}
	req.endname = client.endname
	req.svcMeth = svcMeth
	req.argType = reflect.TypeOf(args)
	req.replyChan = make(chan RplMsg)

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	select {
	// client.ch is a copy of the endpoint in the network
	case client.ch <- req:
	// if that doesnt work then that means that the network failed and should be done
	case <-client.done:
		return false
	}

	reply := <- req.replyChan

	if reply.ok {
		rb := bytes.NewBuffer(reply.reply)
		re := gob.NewDecoder(rb)
		if err := re.Decode(reply); err != nil {
			log.Fatal("func call failed");
		}
		return true

	} 

	return false
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


// server dispatch to format the service dispatch

func(server *Server) dispatch(req ReqMsg) RplMsg {
	server.mu.Lock()
	defer server.mu.Unlock()

	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methName := req.svcMeth[dot+1:]

	service, ok := server.services[serviceName]

	if ok {
		return service.dispatch(methName, req)
	} else {

		return RplMsg{ok: false, reply: nil}
	}

}

// service dispatch 
func(svc *Service) dispatch(methodName string, req ReqMsg) RplMsg {
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
		

		return RplMsg{true, rb.Bytes()}
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
