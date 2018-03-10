package gzkwrapper

import (
	"bytes"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyInvalid      = errors.New("key invalid.")
	ErrArgsInvalid     = errors.New("args invalid.")
	ErrNodeIsNull      = errors.New("node is nil.")
	ErrNodeConnInvalid = errors.New("node conn invalid.")
)

type NodeType int

const (
	NODE_SERVER NodeType = iota + 1 //服务节点
	NODE_WORKER                     //工作节点
)

func (t NodeType) String() string {
	switch t {
	case NODE_SERVER:
		return "NODE_SERVER"
	case NODE_WORKER:
		return "NODE_WORKER"
	}
	return ""
}

var buffer_pool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 1<<10))
	},
}

func encode(nodedata *NodeData) ([]byte, error) {

	buffer := buffer_pool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer buffer_pool.Put(buffer)
	if err := json.NewEncoder(buffer).Encode(nodedata); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func decode(b []byte) (*NodeData, error) {

	if len(b) <= 0 {
		return nil, errors.New("nodedata invalid.")
	}

	nodedata := &NodeData{}
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(nodedata); err != nil {
		return nil, err
	}
	return nodedata, nil
}

type NodesPair map[string]*NodeData

type BaseNode struct {
	NodeType `json:"type"` //节点类型
	HostName string        `json:"hostname"` //主机名称
}

type NodeData struct {
	BaseNode
	DataCenter string `json:"datacenter"` //数据中心名称(一般为调度服务器位置)
	Location   string `json:"location"`   //节点分区位置
	OS         string `json:"os"`         //节点系统
	Platform   string `json:"platform"`   //节点平台
	IpAddr     string `json:"ipaddr"`     //网络地址
	APIAddr    string `json:"apiaddr"`    //节点API
	ProcessId  int    `json:"pid"`        //节点进程号
	Singin     bool   `json:"singin"`     //签到状态
	Timestamp  int64  `json:"timestamp"`  //心跳时间戳
	Alivestamp int64  `json:"alivestamp"` //存活时间戳，若发生改变则代表节点已重启
	Attach     []byte `json:"attach"`     //附加数据
}

func NewNodeData(nodetype NodeType, hostname string, datacenter string, location string,
	os string, platform string, ipaddr string, apiaddr string, processid int) *NodeData {

	if os == "" {
		os = runtime.GOOS
	}

	if platform == "" {
		platform = runtime.GOARCH
	}

	addrSplit := strings.SplitN(apiaddr, ":", 2)
	if addrSplit[0] == "" {
		apiaddr = ipaddr + apiaddr
	}

	if ret := strings.HasPrefix(apiaddr, "http://"); !ret {
		apiaddr = "http://" + apiaddr
	}

	return &NodeData{
		BaseNode: BaseNode{
			NodeType: nodetype,
			HostName: hostname,
		},
		DataCenter: datacenter,
		Location:   location,
		OS:         os,
		Platform:   platform,
		IpAddr:     ipaddr,
		APIAddr:    apiaddr,
		ProcessId:  processid,
		Singin:     false,
		Timestamp:  0,
		Alivestamp: time.Now().Unix(),
		Attach:     nil,
	}
}

type NodeMapper struct {
	sync.RWMutex
	keys  []string
	items map[string]*NodeData
}

func NewNodeMapper() *NodeMapper {

	return &NodeMapper{
		keys:  make([]string, 0),
		items: make(map[string]*NodeData),
	}
}

func (mapper *NodeMapper) Count() int {

	mapper.RLock()
	defer mapper.RUnlock()
	return len(mapper.items)
}

func (mapper *NodeMapper) GetKeys() []string {

	mapper.RLock()
	defer mapper.RUnlock()
	return mapper.keys
}

func (mapper *NodeMapper) Contains(key string) bool {

	mapper.RLock()
	defer mapper.RUnlock()
	for _, k := range mapper.keys {
		if k == key {
			return true
		}
	}
	return false
}

func (mapper *NodeMapper) Get(key string) *NodeData {

	mapper.RLock()
	defer mapper.RUnlock()
	if _, ret := mapper.items[key]; !ret {
		return nil
	}
	return mapper.items[key]
}

func (mapper *NodeMapper) GetNodes(location string, ipaddr string, hostname string) NodesPair {

	mapper.RLock()
	defer mapper.RUnlock()
	nodes := make(NodesPair)
	ipaddr = strings.TrimSpace(ipaddr)
	hostname = strings.TrimSpace(hostname)
	for key, value := range mapper.items {
		if value.Location == location {
			if value.IpAddr == ipaddr || strings.ToUpper(value.HostName) == strings.ToUpper(hostname) {
				nodes[key] = value
			}
		}
	}
	return nodes
}

func (mapper *NodeMapper) Copy(m map[string]*NodeData) {

	if len(m) <= 0 {
		return
	}

	mapper.Lock()
	defer mapper.Unlock()
	mapper.items = m
	mapper.keys = mapper.keys[0:0]
	for key := range mapper.items {
		mapper.keys = append(mapper.keys, key)
	}
}

func (mapper *NodeMapper) Append(key string, value *NodeData) int {

	if value == nil {
		return -1
	}

	mapper.Lock()
	defer mapper.Unlock()
	if _, ret := mapper.items[key]; !ret {
		mapper.items[key] = value
		mapper.keys = append(mapper.keys, key)
		return 0
	}
	return -1
}

func (mapper *NodeMapper) Remove(key string) int {

	mapper.Lock()
	defer mapper.Unlock()
	if _, ret := mapper.items[key]; ret {
		delete(mapper.items, key)
		for i, k := range mapper.keys {
			if k == key {
				mapper.keys = append(mapper.keys[:i], mapper.keys[i+1:]...)
				break
			}
		}
		return 0
	}
	return -1
}

func (mapper *NodeMapper) Set(key string, value *NodeData) int {

	if value == nil {
		return -1
	}

	mapper.Lock()
	defer mapper.Unlock()
	if _, ret := mapper.items[key]; ret {
		mapper.items[key] = value
		return 0
	}
	return -1
}

func (mapper *NodeMapper) Clear() {

	mapper.Lock()
	for key := range mapper.items {
		delete(mapper.items, key)
	}
	mapper.keys = []string{}
	mapper.Unlock()
}
