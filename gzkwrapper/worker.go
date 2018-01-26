package gzkwrapper

import "github.com/cloudtask/libtools/gounits/network"
import "github.com/samuel/go-zookeeper/zk"

import (
	"errors"
	"os"
	"strings"
	"time"
)

type WorkerArgs struct {
	Hosts      string
	Root       string
	Device     string
	DataCenter string
	Location   string
	OS         string
	Platform   string
	APIAddr    string
	Pulse      string
	Threshold  int
}

type Worker struct {
	Key        string
	Root       string
	Path       string
	Pulse      time.Duration
	Node       *Node
	Data       *NodeData
	Handler    INodeNotifyHandler
	pulseAlive bool
	stopCh     chan struct{}
}

func NewWorker(key string, args *WorkerArgs, handler INodeNotifyHandler) (*Worker, error) {

	if len(strings.TrimSpace(key)) == 0 {
		return nil, ErrKeyInvalid
	}

	if args == nil {
		return nil, ErrArgsInvalid
	}

	addr, err := network.GetLocalNetDeviceInfo(args.Device)
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	pulse, err := time.ParseDuration(args.Pulse)
	if err != nil {
		return nil, err
	}

	return &Worker{
		Key:        key,
		Root:       args.Root,
		Path:       args.Root + "/WORKER-" + key,
		Pulse:      pulse,
		Node:       NewNode(args.Hosts),
		Data:       NewNodeData(NODE_WORKER, hostname, args.DataCenter, args.Location, args.OS, args.Platform, addr.IP, args.APIAddr, os.Getpid()),
		Handler:    handler,
		pulseAlive: false,
		stopCh:     nil,
	}, nil
}

func (w *Worker) Open() error {

	if w.Node != nil {
		err := w.Node.Open()
		if err == nil {
			ret, err := w.Node.Exists(w.Root)
			if err != nil {
				return err
			}
			if !ret {
				w.Node.Create(w.Root, nil)
			}
			w.stopCh = make(chan struct{})
		}
		return err
	}
	return ErrNodeIsNull
}

func (w *Worker) Close() error {

	if w.Node != nil {
		w.pulseAlive = false
		if w.stopCh != nil {
			close(w.stopCh)
			w.stopCh = nil
		}
		w.Node.Close()
		return nil
	}
	return ErrNodeIsNull
}

func (w *Worker) Server() string {

	if w.Node != nil {
		return w.Node.Server()
	}
	return ""
}

func (w *Worker) State() string {

	if w.Node != nil {
		return w.Node.State()
	}
	return ""
}

func (w *Worker) SetPulse(value string) error {

	pulse, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	w.Pulse = pulse
	return nil
}

func (w *Worker) GetLocation() string {

	return w.Data.Location
}

func (w *Worker) GetOS() string {

	return w.Data.OS
}

func (w *Worker) GetPlatform() string {

	return w.Data.Platform
}

func (w *Worker) WatchOpen(path string, callback WatchHandlerFunc) error {

	return w.Node.WatchOpen(path, callback)
}

func (w *Worker) WatchClose(path string) {

	w.Node.WatchClose(path)
}

func (w *Worker) Exists(path string) (bool, error) {

	return w.Node.Exists(path)
}

func (w *Worker) Children(path string) ([]string, error) {

	return w.Node.Children(path)
}

func (w *Worker) Get(path string) ([]byte, error) {

	return w.Node.Get(path)
}

func (w *Worker) Create(path string, buffer []byte) error {

	return w.Node.Create(path, buffer)
}

func (w *Worker) Remove(path string) error {

	return w.Node.Remove(path)
}

func (w *Worker) Set(path string, buffer []byte) error {

	return w.Node.Set(path, buffer)
}

func (w *Worker) SetAttach(attach []byte) {

	w.Data.Attach = attach
}

func (w *Worker) Signin(attach []byte) error {

	if w.Node == nil {
		return ErrNodeIsNull
	}

	ret, err := w.Node.Exists(w.Path)
	if err != nil {
		return err
	}

	w.Data.Singin = true
	w.Data.Timestamp = time.Now().Unix()
	w.Data.Attach = attach
	buffer, err := encode(w.Data)
	if err != nil {
		return err
	}

	if !ret {
		if err := w.Node.Create(w.Path, buffer); err != nil {
			return err
		}
	} else {
		if err := w.Node.Set(w.Path, buffer); err != nil {
			return err
		}
	}

	if !w.pulseAlive {
		w.pulseAlive = true
		go w.pulseKeepAlive()
	}
	return nil
}

func (w *Worker) Signout() error {

	if w.Node == nil {
		return ErrNodeIsNull
	}

	w.Data.Singin = false
	w.Data.Timestamp = time.Now().Unix()
	buffer, err := encode(w.Data)
	if err != nil {
		return err
	}

	if err := w.Node.Set(w.Path, buffer); err != nil {
		return err
	}
	return nil
}

func (w *Worker) pulseKeepAlive() {

NEW_TICK_DURATION:
	ticker := time.NewTicker(w.Pulse)
	for {
		select {
		case <-w.stopCh: //node退出
			{
				ticker.Stop()
				return
			}
		case <-ticker.C: //node心跳
			{
				ticker.Stop()
				w.Data.Singin = true
				w.Data.Timestamp = time.Now().Unix()
				buffer, err := encode(w.Data)
				if err != nil {
					err = errors.New("encode worker pulse data error, " + err.Error())
					w.Handler.OnZkWrapperPulseHandlerFunc(w.Key, w.Data, err)
					goto NEW_TICK_DURATION
				}
				err = w.Node.Set(w.Path, buffer)
				if err != nil {
					if err == zk.ErrNoNode {
						if er := w.Node.Create(w.Path, buffer); er != nil {
							err = errors.New("create worker error, " + er.Error())
						}
					} else {
						err = errors.New("set worker error, " + err.Error())
					}
				}
				w.Handler.OnZkWrapperPulseHandlerFunc(w.Key, w.Data, err)
			}
			goto NEW_TICK_DURATION
		}
	}
	w.pulseAlive = false
}
