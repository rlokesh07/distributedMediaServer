package main

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"os"
	"sync"
	"time"
)


type Master struct {
	mu sync.Mutex
	filenameToChunks map[string][]string
	chunkToServer map[string][]string
	chunkServers string
	serverEndpoints map[string]*ClientEnd

	spaceRemaining map[string]int

	lastHeard map[string]time.Time

	leases map[string]*Lease // chunkID to lease
	versions map[string]int // chunk to version
	replicas map[string]int

	killCh chan bool

	leasePeriod int // in seconds
	timeoutDuration int // in seconds
}

type Chunk struct {
	id string
	data []byte
	version int
}

type Lease struct {
	chunkId string
	chunkServerID string
	expiration time.Time
}

func(ms *Master) service() {
	for {
		if <-ms.killCh{
			return;
		}
		
		ms.BroadcastHeartbeat()	

	}
}


func(ms *Master) Persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(ms.versions)
	e.Encode(ms.filenameToChunks)
	data := w.Bytes()
	err := os.WriteFile("masterState", data, 0644)
	if err != nil {
		println("persister failed")
	}
}

func(ms *Master) ReadState() {
	data, err := os.ReadFile("masterState")
	if err != nil {
		println("persister read failed")
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var versions map[string]int
	var chunkHandles map[string][]string

	if d.Decode(&versions) != nil || d.Decode(&chunkHandles) != nil{
		println("persister read failed")
	} else {
		ms.mu.Lock()
		defer ms.mu.Unlock()

		ms.versions = versions
		ms.filenameToChunks = chunkHandles
	}
}


func(ms *Master) BroadcastHeartbeat() {
	for serverId, end := range ms.serverEndpoints{
	
		args := HeartBeatArgs{} 
		rpl := HeartBeatRpl {}
		ok := end.Call("ChunkServer.Heartbeat", args, rpl)
		

		now := time.Now()
		if !ok {
			if now.Before(ms.lastHeard[serverId].Add(time.Duration(ms.timeoutDuration))) {
				// TODO: dead server protocol
			}
		} else {
			ms.lastHeard[serverId] = now
		}
		
		for chunkId, v := range rpl.versions {
			if v != ms.versions[chunkId] {
				
				deleteArgs := DeleteArgs{chunkId: chunkId} 
				deleteRpl := DeleteRpl {}

				okDel := end.Call("ChunkServer.DeleteChunk", deleteArgs, deleteRpl)
				if !okDel {} // TODO: retry logic	
				for i, v := range ms.chunkToServer[chunkId]{
					if v == serverId{
						ms.chunkToServer[chunkId] =  append(ms.chunkToServer[chunkId][:i], ms.chunkToServer[chunkId][i+1:]...)
					}
				}


	
				servers := ms.chunkToServer[chunkId]

				idx := rand.Intn(len(servers))
				
				replicateArgs := ReplicateStartArgs{ chunkId: chunkId, server: ms.serverEndpoints[servers[idx]]}
				replicateRpl := ReplicateRpl{}

				okReplicate := end.Call("ChunkServer.StartReplicate", replicateArgs, replicateRpl)

				if !okReplicate {} //TODO: retry logic

				/* THOUGHTS: 

				First, I know that the retry logic is wack but it should work on the principle that 
				the correct version number should persist until the others find this but this could
				take a few cycles

				Second, the replicate and delete should be put into their own methods in the master
				namespace. With retry logic this is going to be too much. There is already too much
				colliding variable names
				
				Third, i am potentially worried about the amount of maps im using since it turns 
				everything into a O(n log(n)) operation. Gut says realistically I won't be running
				this with more than 20 replicas max which represent like 3 times speed improvement
				but oh well

				Fourth, I think we should seperate some of the structs into the rpc file this is
				like a 500 line doc as is

				*/
				
				
				// TODO: replicate[chunkId]
			
			}
		}

		ms.spaceRemaining[serverId] = rpl.spaceRemaining

	}

}

func(ms *Master) GetServerRead(filename string, chunkIdx int) string {
	chunks := ms.filenameToChunks[filename]
	
	servers := ms.chunkToServer[chunks[chunkIdx]]

	idx := rand.Intn(len(servers))

	return servers[idx]	
}


func(ms *Master) GetServerEndpoint(serverId string) *ClientEnd {
	return ms.serverEndpoints[serverId]
}


func(ms *Master) GetLease(chunkId string) *Lease{
	ms.mu.Lock()
	defer ms.mu.Unlock()

	lease, exists := ms.leases[chunkId]
	
	now := time.Now()

	if exists && now.Before(lease.expiration) {
		return lease	
	}

	ms.versions[chunkId]++

	servers := ms.chunkToServer[chunkId]

	// TODO: load distributer

	primary := servers[0]

	ms.leases[chunkId] = &Lease{
		chunkId: chunkId,
		chunkServerID: primary,
		expiration: now.Add(time.Duration(ms.leasePeriod)),
	}

	return ms.leases[chunkId]
	
}




// ChunkServer
type ChunkServer struct {
	id string
	master *ClientEnd
	chunks map[string]Chunk
	leases map[string]*Lease

	spaceRemaining int
}

type ReadChunkArgs struct {
	chunkId string
	start int
	end int
} 

type ReadChunkRpl struct {
	data []byte
}

func(cs *ChunkServer) ReadData(args *ReadChunkArgs, rpl *ReadChunkRpl) {
	chunk := cs.chunks[args.chunkId]
	rpl.data = chunk.data[args.start:args.end]
}

type HeartBeatArgs struct{}

type HeartBeatRpl struct{
	versions map[string]int
	spaceRemaining int
}

func(cs *ChunkServer) Heartbeat(args *HeartBeatArgs, rpl *HeartBeatRpl) {

	rpl.versions = make(map[string]int)

	for _, c := range cs.chunks {
		rpl.versions[c.id] = c.version
	}

	rpl.spaceRemaining = cs.spaceRemaining
}

// SendChunksArgs
type SendChunksArgs struct {}

type SendChunksRpl struct {
	chunks []string	
}

func(cs *ChunkServer) SendChunks(args *SendChunksArgs, rpl *SendChunksRpl) {
	for _, c := range cs.chunks {
		rpl.chunks = append(rpl.chunks, c.id)
	}
}


// SetLeaseArgs
type SetLeaseArgs struct {
	lease Lease
}

type SetLeaseRpl struct {}

func(cs *ChunkServer) SetLease(args *SetLeaseArgs, rpl *SetLeaseRpl) {
	cs.leases[args.lease.chunkId] = &args.lease
}



// Garbage Collector
type DeleteArgs struct {
	chunkId string
}

type DeleteRpl struct {}

func(cs *ChunkServer) DeleteChunk(args *DeleteArgs, rpl *DeleteRpl) {
	path :=  cs.id + "/" + args.chunkId
	err := os.Remove(path)

	if err != nil {
		println("problem deleting chunk")
		return
	}

	delete(cs.chunks, args.chunkId)
	cs.leases[args.chunkId] = nil
}


type ReplicateStartArgs struct {
	chunkId string
	server *ClientEnd
}

type ReplicateStartRpl struct {
}


func(cs *ChunkServer) StartReplicate(args *ReplicateStartArgs, rpl *ReplicateStartRpl){
	chunk := cs.chunks[args.chunkId]	
	replicateArgs := ReplicateArgs{chunk: chunk}
	replicateRpl := ReplicateRpl{}

	args.server.Call("ChunkServer.Replicate", replicateArgs, replicateRpl)
}

type ReplicateArgs struct {
	chunk Chunk
}

type ReplicateRpl struct {}

func (cs *ChunkServer) Replicate(args *ReplicateArgs, rpl *ReplicateRpl) {
	chunk := args.chunk

	cs.chunks[chunk.id] = chunk
}
