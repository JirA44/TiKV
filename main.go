package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Simulated metrics
var (
	readBlockedDurationMs int64
	readQueueTimeMs       int64
)

type PeerState int32

const (
	StateNormal PeerState = iota
	StateApplyingSnapshot
)

type Peer struct {
	mu          sync.RWMutex
	state       PeerState
	regionID    uint64
	pendingRead chan chan error
}

func NewPeer(regionID uint64) *Peer {
	return &Peer{
		regionID:    regionID,
		state:       StateNormal,
		pendingRead: make(chan chan error, 100),
	}
}

// ApplySnapshot simulates asynchronous snapshot ingestion
func (p *Peer) ApplySnapshot() {
	p.mu.Lock()
	p.state = StateApplyingSnapshot
	p.mu.Unlock()

	fmt.Println("[Peer] Started applying snapshot asynchronously...")
	
	// Simulate heavy I/O and ingestion in a background task
	go func() {
		start := time.Now()
		// Simulate downloading and ingesting SSTs
		time.Sleep(200 * time.Millisecond)
		
		p.mu.Lock()
		p.state = StateNormal
		p.mu.Unlock()
		
		duration := time.Since(start).Milliseconds()
		fmt.Printf("[Peer] Snapshot applied successfully in %dms\n", duration)
	}()
}

// ProcessReadIndex handles ReadIndex requests with fail-fast mechanism
func (p *Peer) ProcessReadIndex() error {
	start := time.Now()

	p.mu.RLock()
	currentState := p.state
	p.mu.RUnlock()

	// Fail-Fast: If snapshot is applying, return RegionNotReady immediately
	if currentState == StateApplyingSnapshot {
		atomic.AddInt64(&readBlockedDurationMs, time.Since(start).Milliseconds())
		return errors.New("RegionNotReady: snapshot is being applied")
	}

	// Simulate normal ReadIndex processing
	time.Sleep(10 * time.Millisecond)
	
	queueTime := time.Since(start).Milliseconds()
	atomic.AddInt64(&readQueueTimeMs, queueTime)
	
	return nil
}

func main() {
	fmt.Println("Starting TiKV Snapshot Read-Block Mitigation Simulation...")
	
	peer := NewPeer(1)
	
	// Start snapshot application
	peer.ApplySnapshot()
	
	// Concurrently send ReadIndex requests
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := peer.ProcessReadIndex()
			if err != nil {
				fmt.Printf("[ReadIndex %d] Failed: %v\n", id, err)
			} else {
				fmt.Printf("[ReadIndex %d] Succeeded\n", id)
			}
		}(i)
	}
	
	// Wait for snapshot to finish applying
	time.Sleep(300 * time.Millisecond)
	
	// Send ReadIndex request after snapshot is applied
	err := peer.ProcessReadIndex()
	if err != nil {
		fmt.Printf("[ReadIndex Post-Snap] Failed: %v\n", err)
	} else {
		fmt.Println("[ReadIndex Post-Snap] Succeeded")
	}
	
	wg.Wait()
	
	fmt.Printf("Metrics - Blocked Duration: %dms, Queue Time: %dms\n", 
		atomic.LoadInt64(&readBlockedDurationMs), 
		atomic.LoadInt64(&readQueueTimeMs),
	)
}