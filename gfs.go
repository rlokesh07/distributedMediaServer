package main

import (
	"sync"
	"time"
)


type Master struct {
	mu sync.Mutex
	filenameToChunks map[string][]string
	chunkToServer map[string][]string
	chunkServers map[string]string
	serverEndpoints map[string]*ClientEnd

	// TODO: persister for persistant states

	leases map[string]*Lease // chunkID to lease
	versions map[string]int // chunk to version


	leasePeriod int // in seconds
}

type ChunkServer struct {
	id string
	master *ClientEnd
	chunks []Chunk
	leases map[string]*Lease
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


type HeartBeatArgs struct{}

type HeartBeatRpl struct{
	versions map[string]int
}

func(cs *ChunkServer) Heartbeat(args *HeartBeatArgs, rpl *HeartBeatRpl) {

	rpl.versions = make(map[string]int)

	for _, c := range cs.chunks {
		rpl.versions[c.id] = c.version
	}
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


