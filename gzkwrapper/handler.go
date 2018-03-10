package gzkwrapper

type NodeStore struct {
	New      NodesPair
	Dead     NodesPair
	Recovery NodesPair
}

func NewNodeStore() *NodeStore {

	return &NodeStore{
		New:      make(NodesPair),
		Dead:     make(NodesPair),
		Recovery: make(NodesPair),
	}
}

func (nodestore *NodeStore) NewTotalSize() int {

	return len(nodestore.New)
}

func (nodestore *NodeStore) DeadTotalSize() int {

	return len(nodestore.Dead)
}

func (nodestore *NodeStore) RecoveryTotalSize() int {

	return len(nodestore.Recovery)
}

func (nodestore *NodeStore) TotalSize() int {

	return len(nodestore.New) + len(nodestore.Dead) + len(nodestore.Recovery)
}

type PulseHandlerFunc func(key string, nodedata *NodeData, err error)
type NodeHandlerFunc func(nodestore *NodeStore)
type WatchHandlerFunc func(path string, data []byte, err error)

type INodeNotifyHandler interface {
	OnZkWrapperPulseHandlerFunc(key string, nodedata *NodeData, err error)
	OnZkWrapperNodeHandlerFunc(nodestore *NodeStore)
}

func (fn PulseHandlerFunc) OnZkWrapperPulseHandlerFunc(key string, nodedata *NodeData, err error) {
	fn(key, nodedata, err)
}

func (fn NodeHandlerFunc) OnZkWrapperNodeHandlerFunc(nodestore *NodeStore) {
	fn(nodestore)
}
