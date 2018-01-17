/*
Copyright (C) 2017 Verizon. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zklib

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"reflect"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/zklib/config"
	"sync"
	"time"
)

//Errors that could be accessed in msgq library
var (
	ErrNoNode       = zk.ErrNoNode
	StateHasSession = zk.StateHasSession
	zkInstanceLock  sync.Mutex
)

// ZK wraps a zookeeper connection plus local structure
type ZK struct {
	Conn        *zk.Conn
	watcherLock sync.Mutex
	/* Keep a map of paths which are being watched */
	watchers map[string]int
}

type NodePathValueInfo map[string][]byte

type SubEventData struct {
	SubPath string
	SubData NodePathValueInfo
}

// LockOptions contains optional request parameters
type LockOptions struct {
	Value     []byte        // Optional, value to associate with the lock
	TTL       time.Duration // Optional, expiration ttl associated with the lock
	RenewLock chan struct{} // Optional, chan used to control and stop the session ttl renewal for the lock
}

// Locker provides locking mechanism on top of the store.
// Similar to `sync.Lock` except it may return errors.
type Locker interface {
	Lock(stopChan chan struct{}) (<-chan struct{}, error)
	Unlock() error
}

type zkLock struct {
	client *zk.Conn
	lock   *zk.Lock
	key    string
	value  []byte
}

type Store interface {
	WriteData(node string, data []byte, ephemeral bool) (err error)
	GetData(node string) ([]byte, *zk.Stat, error)
	Exists(node string) (ok bool, err error)
	DeleteAll(node string) (err error)
	Delete(node string) (err error)
	MkdirAll(node string) (err error)
	Create(node string, value []byte, ephemeral bool) (err error)
	GetDataWithWatchNCreate(zkpath string, value []byte, ephemeral bool) ([]byte, <-chan zk.Event, error)
	GetChildrenWithWatchNCreate(zkpath string) ([]string, <-chan zk.Event, error)
	GetChildrenWithCreate(zkpath string) ([]string, error)
	Subscribe(zkpath string, dir bool) (<-chan SubEventData, *SubEventData, error)
	GetAllChildrenInfo(path string) NodePathValueInfo
	// NewLock creates a lock for a given key.
	// The returned Locker is not held and must be acquired
	// with `.Lock`. The Value is optional.
	NewLock(key string, options *LockOptions) (Locker, error)
}

var zkInstance *ZK

func GetZkInstance(servers []string, recvTimeout time.Duration) (*ZK, error) {
	zkInstanceLock.Lock()
	defer zkInstanceLock.Unlock()

	if zkInstance != nil {
		return zkInstance, nil
	}

	if recvTimeout <= 0 {
		recvTimeout = config.DefaultZkConnectionTimeout
	}

	var err error
	zkInstance, err = NewZK(servers, recvTimeout)
	if err != nil {
		return nil, err
	}

	return zkInstance, nil
}

// NewZK creates a new connection instance
// Note that this function returns soon after the connection towards
// the server is "initiated" and it does not wait for the connection
// to be ready. This can be checked by invoking ZK.State() function.
// The connection is in ready state when the State is set to StateHasSession
// value

func NewZK(servers []string, recvTimeout time.Duration) (*ZK, error) {

	var conn *zk.Conn
	var err error
	var curState zk.State = zk.StateDisconnected
	var numRetries int
	var retryInterval int

zkRetryConnLoop:
	for {
		if nil != conn {
			curState = conn.State()
		}

		switch curState {
		case zk.StateUnknown, zk.StateDisconnected, zk.StateExpired:
			numRetries = 0
			retryInterval = config.RetryStartInterval
			mlog.Info("Connecting to Zookeeper servers %v with timeout %v", servers, recvTimeout)
			conn, _, err = zk.Connect(servers, recvTimeout)
			if err != nil {
				return nil, err
			}
			/* As initial time would always given quite less so this much short wait is OK */
			waitRetryBackoff(&numRetries, &retryInterval, fmt.Sprintf("Waiting some time after connect to get the onnection state established, now the current state is:%v", curState), nil)
		case StateHasSession:
			mlog.Event("zookeeper session is established successfully")
			break zkRetryConnLoop
		default:
			/* This API is to establish connection with service so number of retries don't apply here */
			waitRetryBackoff(&numRetries, &retryInterval, fmt.Sprintf("Waiting to connect to Zookeeper. Connection state:%v", curState), nil)
		}
	}
	return &ZK{Conn: conn, watchers: make(map[string]int, 0)}, nil
}

/*******************************************************************
 * HIGH LEVEL API
 *******************************************************************/

// Create plus write the data to a node
func (z *ZK) WriteData(node string, data []byte, ephemeral bool) (err error) {
	_, stat, err := z.GetData(node)

	// Try to create new node
	if err == zk.ErrNoNode {
		return z.Create(node, data, ephemeral)
	} else if err != nil {
		return err
	}

	numRetries := 0
	retryInterval := config.RetryStartInterval
	for {
		_, err := z.Conn.Set(node, data, stat.Version)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error setting data from zookeeper", err)
			} else {
				mlog.Error("ZK lib WriteData():Set returned with error %v", err)
				return err
			}
		} else {
			return err
		}
	}
}

// Get value of the node
func (z *ZK) GetData(node string) ([]byte, *zk.Stat, error) {
	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		val, stat, err := z.Conn.Get(node)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error getting data from zookeeper", err)
			} else {
				mlog.Error("ZK lib GetData returned with error %v", err)
				return nil, stat, err
			}
		} else {
			return val, stat, err
		}
	}
}

// return content of a node and sets a watch
func (z *ZK) GetW(node string) ([]byte, *zk.Stat, <-chan zk.Event, error) {

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		byte, stat, channel, err := z.Conn.GetW(node)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error in GetW operation from zookeeper", err)
			} else {
				mlog.Error("ZK lib GetW returned with error %v", err)
				return byte, stat, channel, err
			}
		} else {
			return byte, stat, channel, err
		}
	}
}

// Exists checks existence of a node
func (z *ZK) Exists(node string) (ok bool, err error) {

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		ok, _, err = z.Conn.Exists(node)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error checking exist data from zookeeper", err)
			} else {
				mlog.Error("ZK lib Exist returned with error %v", err)
				return
			}
		} else {
			return
		}
	}
}

// children checks of a node
func (z *ZK) Children(node string) ([]string, *zk.Stat, error) {

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		Children, stat, err := z.Conn.Children(node)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error checking children data from zookeeper", err)
			} else {
				mlog.Error("ZK lib Children returned with error %v", err)
				return Children, stat, err
			}
		} else {
			return Children, stat, err
		}
	}
}

// checks children of a node and set a watch
func (z *ZK) ChildrenW(node string) ([]string, *zk.Stat, <-chan zk.Event, error) {

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		Children, stat, channel, err := z.Conn.ChildrenW(node)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error checking ChildrenW data from zookeeper", err)
			} else {
				mlog.Error("ZK lib Children returned with error %v", err)
				return Children, stat, channel, err
			}
		} else {
			return Children, stat, channel, err
		}
	}
}

// DeleteAll deletes a node recursively: Not used currently
func (z *ZK) DeleteAll(node string) (err error) {
	children, stat, err := z.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = z.DeleteAll(path.Join(node, child)); err != nil {
			return
		}
	}

	return z.Conn.Delete(node, stat.Version)
}

//Delete only the node (not recursive)
func (z *ZK) Delete(node string) (err error) {
	_, stat, err := z.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		err := z.Conn.Delete(node, stat.Version)
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error Delete data from zookeeper", err)
			} else {
				mlog.Error("ZK lib Delete returned with error %v", err)
				return err
			}
		} else {
			return err
		}
	}
}

// MkdirAll creates a directory recursively
func (z *ZK) MkdirAll(node string) (err error) {

	numRetries := 0
	retryInterval := config.RetryStartInterval
	parent := path.Dir(node)

	if parent != "/" {
		for {
			err = z.MkdirAll(parent)
			if err != nil {
				if (numRetries < config.NumRetries) && isConnError(err) {
					waitRetryBackoff(&numRetries, &retryInterval, "Error in MkdirAll from zookeeper", err)
				} else {
					mlog.Error("ZK lib in MkdirAll returned with error %v", err)
					return err
				}
			} else {
				break
			}
		}
	}

	for {
		_, err = z.Conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
		if err == zk.ErrNodeExists {
			return nil
		} else if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error creating data from zookeeper", err)
			} else {
				mlog.Error("ZK lib CreateData in MkdirAll returned with error %v", err)
				return err
			}
		} else {
			return nil
		}
	}
}

// Create stores a new value at node. Fails if already set.
func (z *ZK) Create(node string, value []byte, ephemeral bool) (err error) {
	if err = z.MkdirAll(path.Dir(node)); err != nil {
		mlog.Error("Failed to create zookeeper dirs %v", err)
		return
	}

	flags := int32(0)
	if ephemeral {
		flags = zk.FlagEphemeral
	}

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		_, err := z.Conn.Create(node, value, flags, zk.WorldACL(zk.PermAll))
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error creating data from zookeeper", err)
			} else {
				mlog.Error("ZK lib CreateData returned with error %v", err)
				return err
			}
		} else {
			return err
		}
	}
	return
}

func (z *ZK) GetDataWithWatchNCreate(zkpath string, value []byte, ephemeral bool) ([]byte, <-chan zk.Event, error) {
	exists, err := z.Exists(zkpath)
	if !exists {
		z.Create(zkpath, value, ephemeral)
	}

	data, _, ch, err := z.GetW(zkpath)

	return data, ch, err
}

func (z *ZK) GetChildrenWithWatchNCreate(zkpath string) ([]string, <-chan zk.Event, error) {
	exists, err := z.Exists(zkpath)
	if !exists {
		z.Create(zkpath, []byte(""), false)
	}

	children, _, ch, err := z.ChildrenW(zkpath)

	return children, ch, err
}

func (z *ZK) GetChildrenWithCreate(zkpath string) ([]string, error) {
	exists, err := z.Exists(zkpath)
	if !exists {
		z.Create(zkpath, []byte(""), false)
	}

	children, _, err := z.Children(zkpath)

	return children, err
}

func (z *ZK) delWatcher(path string) {
	z.watcherLock.Lock()
	defer z.watcherLock.Unlock()
	mlog.Debug("Request for deleting watcher for [%v]", path)
	if _, ok := z.watchers[path]; ok {
		count := z.watchers[path]
		if count == 1 {
			delete(z.watchers, path)
			mlog.Info("Watcher deleted for [%v]", path)
		} else {
			z.watchers[path]--
		}
	}
}

func (z *ZK) addWatcher(path string) {
	z.watcherLock.Lock()
	defer z.watcherLock.Unlock()
	mlog.Info("Adding watcher for [%v]", path)
	if _, ok := z.watchers[path]; !ok {
		z.watchers[path] = 1
	} else {
		z.watchers[path]++
	}
}

func (z *ZK) watcherExist(path string) bool {
	z.watcherLock.Lock()
	defer z.watcherLock.Unlock()
	_, ok := z.watchers[path]
	return ok
}

func getDiffStringArray(prev, curr []string) (added, deleted []string) {
	/*  prev is nil, it means all nodes are added */
	if prev == nil {
		return curr, nil
	}

	/*  curr is nil, it means all nodes are deleted */
	if curr == nil {
		return nil, prev
	}

	/* Get added items */
	for _, currItem := range curr {
		found := false
		for _, prevItem := range prev {
			if prevItem == currItem {
				found = true
				break
			}
		}
		if found == false {
			added = append(added, currItem)
		}
	}

	/* Get deleted items */
	for _, prevItem := range prev {
		found := false
		for _, currItem := range curr {
			if prevItem == currItem {
				found = true
				break
			}
		}
		if found == false {
			deleted = append(deleted, prevItem)
		}
	}

	return added, deleted

}

func (z *ZK) updateWatchers(basePath string, prevChildren []string, currChildren []string, retChan chan zk.Event) {
	/* Get the added/deleted children list and act accordingly */
	added, deleted := getDiffStringArray(prevChildren, currChildren)
	mlog.Debug("AddedChildren [%+v] DeletedChildren [%+v] on path[%v]", added, deleted, basePath)

	/* Add data watcher for all the added children */
	for _, child := range added {
		if z.watcherExist(child) == false {
			z.startPeriodicDataWatcher(basePath+"/"+child, retChan, false)
		}
	}

	/* Delete the paths of deleted children from local */
	for _, child := range deleted {
		z.delWatcher(basePath + "/" + child)
	}

}

func (z *ZK) startPeriodicDataWatcher(zkpath string, retChan chan zk.Event, closeRetChan bool) error {
	var done bool = true
	var currData, prevData []byte

	/* Put a data watcher */
	_, _, ch, err := z.GetW(zkpath)
	if err != nil {
		mlog.Error("DataWatcher could not be started for path[%v] err[%+v]", zkpath, err)
		return err
	}
	z.addWatcher(zkpath)
	mlog.Debug("Starting periodic data watcher for [%s]", zkpath)

	go func() {
		for done {
			/* In case data is changed watcher behavior is
			 * 1. First an event will be received in the ch
			 * 2. ch will be closed
			 */
			select {
			case evt, ok := <-ch:
				/* Library closed the channel, which an indication that one-time watcher
				 * is done. So reregister the watch
				 */
				if !ok {
					mlog.Debug("Reregistring data watcher for [%s]", zkpath)
					/* Put a data watcher */
					if currData, _, ch, err = z.GetW(zkpath); err != nil {
						mlog.Error("Error in reregistring watch for [%s]", zkpath)
						done = false
						break
					}
				} else {
					/* Any change in node data will result in EventNodeDataChanged
					 * from application and we are interested in that only. So all events
					 * other than EventNodeDataChanged are error cases, which will result
					 * in exit from infinite loop
					 */
					if evt.Type == zk.EventNodeDataChanged {
						/* Get the data when EventNodeDataChanged event is received
						 * this will help in comparing with the data received last
						 * time to determine whether to invoke an event for application or not
						 */
						if prevData, _, err = z.GetData(zkpath); err != nil {
							mlog.Info("Error Ignoring the Get Error [%+v] path[%+v] evt[%+v]", err, zkpath, evt)
						}
					} else {
						/* EventNodeDeleted event is received when the node on which data watcher is applied
						 * is deleted.
						 */
						mlog.Info("Evt [%+v] DataWatch no longer valid for [%s]", evt, zkpath)
						/* Make for loop exit */
						done = false
						break
					}
				}
				// Check if there is a change in data for node, if yes raise an event for application
				if reflect.DeepEqual(currData, prevData) == false {
					mlog.Debug("DataWatcher got an event for [%s] [%+v]", zkpath, evt)
					retChan <- zk.Event{Type: zk.EventNodeDataChanged, State: zk.StateSyncConnected, Path: zkpath}
				}
			}
		}
		mlog.Debug("DataWatcher on path [%s] done", zkpath)
		z.delWatcher(zkpath)
		if closeRetChan == true {
			close(retChan)
		}
	}()

	return nil

}

func (z *ZK) startPeriodicChildWatcher(zkpath string, retChan chan zk.Event) ([]string, error) {
	var done bool = true
	var prevChildren, currChildren []string

	/* Put a child watcher */
	children, _, ch, err := z.ChildrenW(zkpath)
	if err != nil {
		return nil, err
	}

	// Will start a go routine for data watcher per child
	z.addWatcher(zkpath)

	mlog.Debug("Starting periodic children watcher for [%s]", zkpath)
	go func() {
		for done {
			prevChildren = currChildren
			select {
			/* In case of a child created or deleted, watcher behavior is
			 * 1. First an event will be received in the ch
			 * 2. ch will be closed
			 */
			case evt, ok := <-ch:
				/* Library closed the channel, which an indication that one-time watcher
				 * is done. So reregister the watch
				 */
				if !ok {
					mlog.Debug("Reregistring children watcher for [%s]", zkpath)
					/* Put a child watcher */
					if currChildren, _, ch, err = z.ChildrenW(zkpath); err != nil {
						mlog.Error("Error in reregistring watch for [%s]", zkpath)
						done = false
						break
					}
				} else {
					/* Any child node created/deleted will result in EventNodeChildrenChanged
					 * from application and we are interested in that only. So all events
					 * other than EventNodeChildrenChanged are error cases, which will result
					 * in exit from infinite loop
					 */
					if evt.Type == zk.EventNodeChildrenChanged {
						/* Get the Children list when EventNodeChildrenChanged event is received
						 * this will help in comparing with the children list received last
						 * time to determine whether to invoke an event for application or not
						 */
						if currChildren, _, err = z.Conn.Children(zkpath); err != nil {
							mlog.Error("Chidren could not retrieved [%+v]. Will stop", evt, err)
							done = false
							break
						}
					} else {
						/* EventNodeDeleted event is received when the node on which watcher is applied
						 * is deleted.
						 */
						mlog.Info("Evt [%+v] ChildWatch no longer valid for [%s]", evt, zkpath)
						done = false
						/* Make for loop exit */
						break
					}
				}
				// Check if there is a change in children list, if yes raise an event for application
				if reflect.DeepEqual(currChildren, prevChildren) == false {
					mlog.Debug("ChildrenWatcher got event for [%s] [%+v] [%+v] [%+v]", zkpath, evt, prevChildren, currChildren)
					z.updateWatchers(zkpath, prevChildren, currChildren, retChan)
					retChan <- zk.Event{Type: zk.EventNodeChildrenChanged, State: zk.StateSyncConnected, Path: zkpath}
				}

			}
		}
		mlog.Debug("ChildrenWatcher on path [%s] done", zkpath)
		z.delWatcher(zkpath)
		close(retChan)
	}()

	return children, nil

}

/* Function to subscribe a znode in either of following cases
 * 1. For child create/delete and data value changes for the child node.
 * 2. Data value changes
 *
 * Anytime a node change is observed, Path information will be returned to
 * application via SubEventData channel.
 *
 * IN :
 * 		zpath 		: Path on which data or child + data watch is to be applied
 *		childWatch 	: Flag to confirm whether a data watch on zpath or child watch on zpath
 * OUT :
 *		error 		: In case watch could not be put
 *		SubEventData: A channel which application should listen for the node change events
 *
 * WARNING:: Please note following
 * 1. zkpath could be nested path /A/B/C, in that case watch will be on C
 * 2. In case child watch is put it is only 1 level deep i.e. if watch is put on
 *	  /A and /A/B is created there will be event generated or B's value is changed event
 *	  will be generated. But if /A/B/C is created no event will be generated
 */
func (z *ZK) Subscribe(zkpath string, childWatch bool) (<-chan SubEventData, *SubEventData, error) {
	var done bool = true
	events := make(chan zk.Event, config.SubscribeChanBufferedItems)

	exists, err := z.Exists(zkpath)
	if err != nil {
		return nil, nil, err
	} else if !exists {
		if childWatch {
			/* Create node recursively */
			z.MkdirAll(zkpath)
		} else {
			z.Create(zkpath, []byte(""), false)
		}
	}

	retChan := make(chan SubEventData, config.SubscribeChanBufferedItems)

	if childWatch == true {
		/* Start a Child watcher on the path */
		if children, err := z.startPeriodicChildWatcher(zkpath, events); err != nil {
			return nil, nil, err
		} else {
			/* Start data watcher on existing children */
			z.updateWatchers(zkpath, nil, children, events)
		}
	} else {
		/* Create Data watcher on the node, and if node is deleted close the retChan
		 * so that application can be notified accordinly
		 */
		z.startPeriodicDataWatcher(zkpath, events, true)
	}
	go func() {
		for done {
			select {
			case evt, ok := <-events:
				/* retChan is closed, an indictaion that watcher no longer valid.
				 * Either node is deleted or network connectivity is broken. Come
				 * out of for loop in this case
				 */
				if !ok {
					/* Make for loop exit */
					done = false
				} else {
					/* Watched node children or children data is changed. Notify application */
					info := z.GetAllChildrenInfo(zkpath)
					mlog.Debug("Reporting SubEventData[%+v] to APP for event received[%+v] for [%s]", info, evt, zkpath)
					retChan <- SubEventData{SubPath: zkpath, SubData: info}
				}
			}
		}
		close(retChan)
		mlog.Info("Subscribe on path [%s] done", zkpath)
	}()

	info := z.GetAllChildrenInfo(zkpath)
	mlog.Debug("Reporting SubEventData[%+v] to APP for event received[%+v] for [%s]", info, zkpath)
	return retChan, &SubEventData{SubPath: zkpath, SubData: info}, err
}

/* Function recursively traverse a path and returns Path:Value pair */
func (z *ZK) GetAllChildrenInfo(path string) NodePathValueInfo {
	var nodeInfo NodePathValueInfo = make(NodePathValueInfo, 0)

	if d, s, err := z.GetData(path); err == nil {
		if s.NumChildren != 0 {
			/* There are children, get the list of children nodes */
			if data, _, err := z.Conn.Children(path); err != nil {
				mlog.Error("Error [%+v]", err)
			} else {
				/* Parse the children list */
				for i := range data {
					zpath := path + "/" + data[i]
					/* Get the value of a child node */
					if d1, s1, err1 := z.GetData(zpath); err1 != nil {
						mlog.Error("Error [%+v] [%+v]", zpath, err1)
					} else {
						/* No Error. Check if there are any children for node */
						if s1.NumChildren != 0 {
							/* For children of the nodes get the path:value pair
							 * via recursion
							 */
							for k, v := range z.GetAllChildrenInfo(zpath) {
								nodeInfo[k] = v
							}
						} else {
							nodeInfo[zpath] = d1
						}
					}
				}
			}
		} else {
			nodeInfo[path] = d
		}
	}
	return nodeInfo
}

// NewLock returns a handle to a lock struct which can
// be used to provide mutual exclusion on a key
func (z *ZK) NewLock(key string, options *LockOptions) (lock Locker, err error) {
	value := []byte("")

	// Apply options
	if options != nil {
		if options.Value != nil {
			value = options.Value
		}
	}

	lock = &zkLock{
		client: z.Conn,
		key:    Normalize(key),
		value:  value,
		lock:   zk.NewLock(z.Conn, Normalize(key), zk.WorldACL(zk.PermAll)),
	}

	return lock, err
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *zkLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	err := l.lock.Lock()

	if err == nil {
		_, err = l.client.Set(l.key, l.value, -1)
	}

	return make(chan struct{}), err
}

// Unlock the "key". Calling unlock while
// not holding the lock will throw an error
func (l *zkLock) Unlock() error {
	return l.lock.Unlock()
}

// This function check if it connection related error
func isConnError(err error) bool {
	switch err {
	case zk.ErrNoServer:
		return true
	default:
		return false
	}
}

// This function provide retry mechanism compliance (do waittime doubling until retry max backoff)
func waitRetryBackoff(numRetries *int, retryInterval *int, msg string, err error) {
	time.Sleep(time.Duration(*retryInterval) * time.Second)

	if *numRetries >= config.RetryMaxBeforeAlarm {
		mlog.Alarm("%s:%v", msg, err)
	} else {
		mlog.Error("%s:%v", msg, err)
	}

	if 2**retryInterval <= config.RetryMaxBackoff {
		*retryInterval = 2 * *retryInterval
	}

	*numRetries++
}
