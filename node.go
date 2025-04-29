package asg3

import (
	"log"
	"sync"
	// "fmt"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	snapshotStateMutex sync.Mutex // need for conc
	snapshots map[int]*NodeSnapshot // map snapId to snapshot struct
}

type NodeSnapshot struct {
    tokens    int  // num of tokens in snapshot of node     
    channels  map[string][]*MsgSnapshot // this is to store recording of channel, needed for globalSnap to be slice of msgSnap
    rec map[string]bool // to know what channels im recording
	startTime int
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		// TODO: You may need to modify this if you make modifications above
		snapshots: make(map[int]*NodeSnapshot),
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

// dont forget to add details to globalSnapshot when done!
func (node *Node) HandlePacket(src string, message Message) {
	// TODO: Write this method

	if message.isMarker {
		node.snapshotStateMutex.Lock()
		defer node.snapshotStateMutex.Unlock()

		snapId := message.data

		// fmt.Printf("Node %v recieved snapshot: %d\n", node.id,snapId)
		snap, exists := node.snapshots[snapId]

		if !exists {
			snap = &NodeSnapshot{
				tokens: node.tokens,
				channels: make(map[string][]*MsgSnapshot),
				rec: make(map[string]bool),
			}
			
			// dont record from channel that sent marker, send marker to everyone except source node
			for snapSrc := range node.inboundLinks {
				if snapSrc != src {
					snap.rec[snapSrc] = true
				}
			}

			msg := Message{isMarker: true, data: snapId}
			node.SendToNeighbors(msg)

			node.snapshots[snapId] = snap

			// fmt.Printf("Node %v started snapshot: %d\n", node.id,snapId)

			if len(snap.rec) == 0 {
				
				// go func() {
					node.sim.NotifyCompletedSnapshot(node.id, snapId)
				// }()
			}

			// fmt.Printf("Node %v finished snapshot: %d\n", node.id,snapId)

		} else{
			delete(snap.rec, src)

			if len(snap.rec) == 0 {
				
				// go func() {
					node.sim.NotifyCompletedSnapshot(node.id, snapId)
				// }()
			}

			// fmt.Printf("Node %v finished snapshot: %d\n", node.id,snapId)

		}

	} else{
		node.snapshotStateMutex.Lock()
		defer node.snapshotStateMutex.Unlock()

        node.tokens += message.data
        
		// can have multiple snapshots at same time, need to loop over all of them to record
        for _, snapshot := range node.snapshots {
            if snapshot.rec[src] {
				msgSnap := MsgSnapshot{src, node.id, message}
				snapshot.channels[src] = append(snapshot.channels[src], &msgSnap)
            }
        }
	}
}

func (node *Node) StartSnapshot(snapshotId int) {

	// ToDo: Write this method

	node.snapshotStateMutex.Lock()
	defer node.snapshotStateMutex.Unlock()

	snap := &NodeSnapshot{
		tokens: node.tokens,
		channels: make(map[string][]*MsgSnapshot),
		rec: make(map[string]bool),
		startTime: node.sim.time,
	}
	
	for snapSrc := range node.inboundLinks {
		snap.rec[snapSrc] = true
	}

	msg := Message{isMarker: true, data: snapshotId}
	node.SendToNeighbors(msg)

	node.snapshots[snapshotId] = snap

	if len(snap.rec) == 0 {
				
		// go func() {
			node.sim.NotifyCompletedSnapshot(node.id, snapshotId)
		// }()
	}

}
