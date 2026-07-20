package main

import "sync"


type Master struct {
	mu sync.Mutex
	filenameToChunks map[string][]string
	chunkToServer map[string][]int
	chunkServers map[int]*ClientEnd

	// TODO: persister for persistant states

}

type ChunkServer struct {
	id int
	master *ClientEnd
}


func(ms *Master) Heartbeat() {

}
