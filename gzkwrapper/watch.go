package gzkwrapper

import "github.com/samuel/go-zookeeper/zk"

import (
	"errors"
	"time"
)

const RetryInterval time.Duration = time.Second * 5

type WatchObject struct {
	Path string
	exit chan bool
}

func CreateWatchObject(path string, conn *zk.Conn, callback WatchHandlerFunc) *WatchObject {

	if conn == nil {
		return nil
	}

	watchobject := &WatchObject{
		Path: path,
		exit: make(chan bool),
	}

	go func(wo *WatchObject, c *zk.Conn, fn WatchHandlerFunc) {
		listen := true
		for listen {
			ret, _, ev, err := c.ExistsW(wo.Path)
			if err != nil {
				if callback != nil {
					callback(wo.Path, nil, err)
				}
				time.Sleep(RetryInterval)
				continue
			}

			select {
			case <-ev:
				{
					if ret {
						go func() {
							data, _, err := c.Get(wo.Path)
							if callback != nil {
								callback(wo.Path, data, err)
							}
						}()
					} else {
						if callback != nil {
							callback(wo.Path, nil, errors.New("watch exists not found."))
						}
						time.Sleep(RetryInterval)
					}
				}
			case <-wo.exit:
				{
					listen = false
				}
			}
		}
	}(watchobject, conn, callback)
	return watchobject
}

/*
func CreateWatchObject(path string, conn *zk.Conn, callback WatchHandlerFunc) *WatchObject {

	if conn == nil {
		return nil
	}

	watchobject := &WatchObject{
		Path: path,
		exit: make(chan bool),
	}

	go func(wo *WatchObject, c *zk.Conn, fn WatchHandlerFunc) {
		listen := true
	NEW_WATCH:
		for listen {
			ret, _, ev, err := c.ExistsW(wo.Path)
			if err != nil {
				if callback != nil {
					callback(wo.Path, nil, err)
				}
				time.Sleep(RetryInterval)
				goto NEW_WATCH
			}

			select {
			case <-ev:
				{
					if ret {
						data, _, err := c.Get(wo.Path)
						if err != nil {
							if callback != nil {
								callback(wo.Path, nil, err)
							}
							time.Sleep(RetryInterval)
							goto NEW_WATCH
						}
						if callback != nil {
							callback(wo.Path, data, nil)
						}
					} else {
						if callback != nil {
							callback(wo.Path, nil, errors.New("watch exists not found."))
						}
						time.Sleep(RetryInterval)
						goto NEW_WATCH
					}
				}
			case <-wo.exit:
				{
					listen = false
				}
			}
		}
	}(watchobject, conn, callback)
	return watchobject
}
*/
func ReleaseWatchObject(wo *WatchObject) {

	if wo != nil {
		wo.exit <- true
		close(wo.exit)
	}
}
