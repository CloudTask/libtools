package gzkwrapper

import "github.com/samuel/go-zookeeper/zk"

import (
	"errors"
	"strings"
	"sync"
	"time"
)

var flags = int32(0)
var acl = zk.WorldACL(zk.PermAll)
var defaultTimeout = 10 * time.Second

type Node struct {
	Hosts    []string
	Conn     *zk.Conn
	mutex    *sync.RWMutex
	wobjects map[string]*WatchObject
}

func NewNode(hosts string) *Node {

	return &Node{
		Hosts:    strings.Split(hosts, ","),
		Conn:     nil,
		mutex:    new(sync.RWMutex),
		wobjects: make(map[string]*WatchObject, 0),
	}
}

func (n *Node) Open() error {

	if n.Conn == nil {
		conn, event, err := zk.Connect(n.Hosts, defaultTimeout)
		if err != nil {
			return err
		}
		<-event
		n.Conn = conn
	}
	return nil
}

func (n *Node) Close() {

	n.mutex.Lock()
	defer n.mutex.Unlock()
	for path, wo := range n.wobjects {
		ReleaseWatchObject(wo)
		delete(n.wobjects, path)
	}

	if n.Conn != nil {
		n.Conn.Close()
		n.Conn = nil
	}
}

func (n *Node) Server() string {

	if n.Conn != nil {
		return n.Conn.Server()
	}
	return ""
}

func (n *Node) State() string {

	if n.Conn != nil {
		return n.Conn.State().String()
	}
	return ""
}

func (n *Node) WatchOpen(path string, callback WatchHandlerFunc) error {

	n.mutex.Lock()
	defer n.mutex.Unlock()
	if _, ret := n.wobjects[path]; ret {
		return nil
	}

	wo := CreateWatchObject(path, n.Conn, callback)
	if wo != nil {
		n.wobjects[path] = wo
		return nil
	}
	return errors.New("watch path failed.")
}

func (n *Node) WatchClose(path string) {

	n.mutex.Lock()
	defer n.mutex.Unlock()
	if wo, ret := n.wobjects[path]; ret {
		ReleaseWatchObject(wo)
		delete(n.wobjects, path)
	}
}

func (n *Node) Exists(path string) (bool, error) {

	if n.Conn == nil {
		return false, ErrNodeConnInvalid
	}

	ret, _, err := n.Conn.Exists(path)
	if err != nil {
		return false, err
	}
	return ret, nil
}

func (n *Node) Children(path string) ([]string, error) {

	if n.Conn == nil {
		return nil, ErrNodeConnInvalid
	}

	v, _, err := n.Conn.Children(path)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (n *Node) Get(path string) ([]byte, error) {

	if n.Conn == nil {
		return nil, ErrNodeConnInvalid
	}

	buffer, _, err := n.Conn.Get(path)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (n *Node) Create(path string, buffer []byte) error {

	if n.Conn == nil {
		return ErrNodeConnInvalid
	}

	if _, err := n.Conn.Create(path, buffer, flags, acl); err != nil {
		return err
	}
	return nil
}

func (n *Node) Remove(path string) error {

	if n.Conn != nil {
		return n.Conn.Delete(path, -1)
	}
	return ErrNodeConnInvalid
}

func (n *Node) Set(path string, buffer []byte) error {

	if n.Conn == nil {
		return ErrNodeConnInvalid
	}

	if _, err := n.Conn.Set(path, buffer, -1); err != nil {
		return err
	}
	return nil
}
