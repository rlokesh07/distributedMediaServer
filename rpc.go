package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
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
	connections map[string]interface{}
	endCh chan ReqMsg
	done chan struct{}
	count int32
}


func MakeNetwork() *Network {

	network := Network{}
	network.ends = map[string]*ClientEnd{}
	network.enabled = map[string]bool{}
	network.servers = map[string]*Server{}
	network.connections = map[string](interface{}){}
	network.endCh = make(chan ReqMsg)
	network.done = make(chan struct{})
		
	go func(){
		for{
			select{
			case req := <- network.endCh:
				atomic.AddInt32(&network.count, 1)
				go network.ProcessReq(req)
			case <- network.done:
				return 
			}
		}

	}()
	return &network
}

func(network *Network) AddServer(serverName string, server *Server){
	network.mu.Lock()
	defer network.mu.Unlock()

	network.servers[serverName] = server
}

func(network *Network) ProcessReq(req ReqMsg){
	network.mu.Lock()

	serverName, ok := network.connections[req.endname]
	var server *Server;
	var serverNameStr string
	if ok {
		serverNameStr = serverName.(string)
		server = network.servers[serverNameStr]
	} else {
		log.Printf("[DEBUG] ProcessReq: no connection for endname=%q (connections=%v)", req.endname, network.connections)
		req.replyChan <- RplMsg{ok: false, reply: nil}
		return
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
			timeout = true
			log.Printf("[DEBUG] ProcessReq: timeout waiting for dispatch reply (endname=%q, svcMeth=%q)", req.endname, req.svcMeth)
			if network.IsServerDead(serverNameStr, server) {
				go func(){
					<-ech;
				}()
			}
		}
	}

	serverDead := network.IsServerDead(serverNameStr, server)
	
	if serverDead {
		log.Printf("[DEBUG] ProcessReq: server dead after dispatch (endname=%q, svcMeth=%q)", req.endname, req.svcMeth)
		req.replyChan <- RplMsg{ok: false, reply: nil}
	} else {
		log.Printf("[DEBUG] ProcessReq: sending reply ok=%v (endname=%q, svcMeth=%q)", rpl.ok, req.endname, req.svcMeth)
		req.replyChan <- rpl
	}
}

func(network *Network) IsServerDead(servername string, server *Server) bool {
	network.mu.Lock()
	defer network.mu.Unlock()

	if network.servers[servername] != server {
		return true
	}
	 return false
}

func(network *Network) MakeEnd(endname string) *ClientEnd {
	network.mu.Lock()
	defer network.mu.Unlock()

	e := &ClientEnd{}
	e.endname  = endname
	e.ch = network.endCh
	e.done = network.done
	network.ends[endname] = e
	network.connections[endname] = nil

	return e

}

func(network *Network) Connect(endname string, servername string){
	network.mu.Lock()
	defer network.mu.Unlock()

	network.connections[endname] = servername
}

func(client *ClientEnd) Call(svcMeth string, args interface{}, resp interface{}) bool { // returns if the call was successful
	req := ReqMsg{}
	req.endname = client.endname
	req.svcMeth = svcMeth
	req.argType = reflect.TypeOf(args)
	req.replyChan = make(chan RplMsg)

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	if err := qe.Encode(args); err != nil {
		log.Println("encode args failed:", err)
	}
	req.args = qb.Bytes()

	select {
	// client.ch is a copy of the endpoint in the network
	case client.ch <- req:
	// if that doesnt work then that means that the network failed and should be done
	case <-client.done:
		return false
	}

	reply := <- req.replyChan
	log.Printf("[DEBUG] Call: received reply ok=%v (svcMeth=%q)", reply.ok, svcMeth)

	if reply.ok {
		rb := bytes.NewBuffer(reply.reply)
		re := gob.NewDecoder(rb)
		if err := re.Decode(resp); err != nil {
			log.Fatal("func call failed:", err);
		}
		return true

	} 

	return false
}

func MakeServer() *Server{
	rs := &Server{}	
	rs.services = map[string]*Service{}
	return rs
}

func MakeService(rcvr interface{}) *Service{
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
	return svc
}

func (server *Server) AddService(svc *Service) {
	server.mu.Lock()
	defer server.mu.Unlock()

	server.services[svc.name] = svc
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
		log.Printf("[DEBUG] server.dispatch: service %q not found (known services: %v)", serviceName, server.services)
		return RplMsg{ok: false, reply: nil}
	}

}

// service dispatch 
func(svc *Service) dispatch(methodName string, req ReqMsg) RplMsg {
	log.Printf("[DEBUG] svc.dispatch: service=%q methodName=%q (known methods: %v)", svc.name, methodName, svc.methods)
	if method, ok := svc.methods[methodName]; ok {

		args := reflect.New(req.argType)

		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		if err := ad.Decode(args.Interface()); err != nil {
			log.Println("decode args failed:", err)
		}
		replyType := method.Type.In(2).Elem()
		replyValue := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyValue})
	
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.Encode(replyValue.Interface())
		

		return RplMsg{true, rb.Bytes()}
	} else {
		log.Printf("[DEBUG] svc.dispatch: method %q not found in service %q", methodName, svc.name)
		return RplMsg{false, nil}
	}
}

func Call(addr string, rpcmeth string, args interface{}, rpl interface{}) bool {
	c, erx := rpc.Dial("unix", addr)
	if erx != nil {
		log.Println(erx)
		return false
	}
	defer c.Close()

	err := c.Call(rpcmeth, args, rpl)
	if err == nil {
		return true
	} else {
		println(err)
		return false
	}


}

type Test struct{
	
}

type AddArgs struct{
	I int
	J int
}

type AddRpl struct {
	Ans int
}

func(t Test) Add(args *AddArgs, rpl *AddRpl){
	rpl.Ans = args.I + args.J
}


func main(){
	network := MakeNetwork()	
	
	server1 := MakeServer()
	
	tester := &Test{}

	testService := MakeService(tester)

	server1.AddService(testService)

	network.AddServer("server1", server1)
	
	endpoint := network.MakeEnd("endpoint")

	network.Connect("endpoint", "server1")

	args := &AddArgs{I: 1, J: 2}
	rpl := &AddRpl{}

	ok := endpoint.Call("Test.Add", args, rpl)

	if ok{
		fmt.Println(rpl.Ans)
	} else {
		fmt.Println("failed")
	}
}	
