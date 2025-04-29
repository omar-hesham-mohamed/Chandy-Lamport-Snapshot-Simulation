package asg3

import (
	"log"
	"math/rand"
	"sync"
	// "fmt"
)

// Max random delay added to packet delivery
const maxDelay = 5

type ChandyLamportSim struct {
	time           int
	nextSnapshotId int
	nodes          map[string]*Node // key = node ID
	logger         *Logger
	// TODO: You can add more fields here.

	// i think we need to add a map of snapshots with all remaining nodes, when notify remove node and when empty collect snapshots.
	//nodesLeft map[int][]string // store nodes unsnapped in each snapshot
	snapshotWaitGroups map[int]*sync.WaitGroup
	snapshotMutex sync.Mutex

}

func NewSimulator() *ChandyLamportSim {
	return &ChandyLamportSim{
		time:           0,
		nextSnapshotId: 0,
		nodes:          make(map[string]*Node),
		logger:         NewLogger(),
		// ToDo: you may need to modify this if you modify the above struct

		// nodesLeft: make(map[int][]string),
		snapshotWaitGroups: make(map[int]*sync.WaitGroup),

	}	
}

// Add a node to this simulator with the specified number of starting tokens
func (sim *ChandyLamportSim) AddNode(id string, tokens int) {
	node := CreateNode(id, tokens, sim)
	sim.nodes[id] = node
}

// Add a unidirectional link between two nodes
func (sim *ChandyLamportSim) AddLink(src string, dest string) {
	node1, ok1 := sim.nodes[src]
	node2, ok2 := sim.nodes[dest]
	if !ok1 {
		log.Fatalf("Node %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Node %v does not exist\n", dest)
	}
	node1.AddOutboundLink(node2)
}

func (sim *ChandyLamportSim) ProcessEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.nodes[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.nodeId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step and deliver at most one packet per node
func (sim *ChandyLamportSim) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the nodes,
	// we must also iterate through the nodes and the links in a deterministic way
	for _, nodeId := range getSortedKeys(sim.nodes) {
		node := sim.nodes[nodeId]
		for _, dest := range getSortedKeys(node.outboundLinks) {
			link := node.outboundLinks[dest]
			// Deliver at most one packet per node at each time step to
			// establish total ordering of packet delivery to each node
			if !link.msgQueue.Empty() {
				e := link.msgQueue.Peek().(SendMsgEvent)
				if e.receiveTime <= sim.time {
					link.msgQueue.Pop()
					sim.logger.RecordEvent(
						sim.nodes[e.dest],
						ReceivedMsgRecord{e.src, e.dest, e.message})
					sim.nodes[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Return the receive time of a message after adding a random delay.
// Note: At each time step, only one message is delivered to a destination.
// This means that the message may be received *after* the time step returned in this function.
// See the clarification in the document of the assignment
func (sim *ChandyLamportSim) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(maxDelay)
}

func (sim *ChandyLamportSim) StartSnapshot(nodeId string) {
	sim.snapshotMutex.Lock()
    defer sim.snapshotMutex.Unlock()

	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.nodes[nodeId], StartSnapshotRecord{nodeId, snapshotId})
	// TODO: Complete this method

	var wg sync.WaitGroup
    wg.Add(len(sim.nodes)) // Wait for all nodes to complete
    sim.snapshotWaitGroups[snapshotId] = &wg
	
	sim.logger.RecordEvent(sim.nodes[nodeId], StartSnapshotRecord{nodeId, snapshotId})
	sim.nodes[nodeId].StartSnapshot(snapshotId)

	// fmt.Printf("Started snapshot: %d\n", snapshotId)
	// fmt.Printf("Nodes waiting on: %d\n", len(sim.nodes))

	// go sim.CollectSnapshot(snapshotId)
}

func (sim *ChandyLamportSim) NotifyCompletedSnapshot(nodeId string, snapshotId int) {
	sim.logger.RecordEvent(sim.nodes[nodeId], EndSnapshotRecord{nodeId, snapshotId})

	// TODO: Complete this method
	sim.snapshotMutex.Lock()
    defer sim.snapshotMutex.Unlock()

	if wg, exists := sim.snapshotWaitGroups[snapshotId]; exists {
        wg.Done() // Signal that this node has completed
		// fmt.Printf("One node done on snapshot: %d\n", snapshotId)
    }

}

func (sim *ChandyLamportSim) CollectSnapshot(snapshotId int) *GlobalSnapshot {

	// TODO: Complete this method
	snap := GlobalSnapshot{snapshotId, make(map[string]int), make([]*MsgSnapshot, 0)}
	
	sim.snapshotMutex.Lock()
    wg := sim.snapshotWaitGroups[snapshotId]
    sim.snapshotMutex.Unlock()

	// fmt.Printf("Waiting to collect snapshot: %d\n", snapshotId)

	wg.Wait()

	// fmt.Printf("Collecting snapshot: %d\n", snapshotId)

	for nodeId, node := range sim.nodes {
        node.snapshotStateMutex.Lock() // need to lock node
        
		snap.tokenMap[nodeId] = node.snapshots[snapshotId].tokens
		
		// loop over map to add recordings
		for _, snapSlice := range node.snapshots[snapshotId].channels {
			// snapSlice is slice of msgSnapshots
			snap.messages = append(snap.messages, snapSlice...) // ... unpacks snapSlice instead of iterating over it and appendinng every message alone
		}
        
        node.snapshotStateMutex.Unlock()
    }

	return &snap
}
