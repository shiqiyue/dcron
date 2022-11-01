package dcron

import (
	"encoding/json"
	"github.com/libi/dcron/consistenthash"
	"github.com/libi/dcron/driver"
	"sync"
	"time"
)

//NodePool is a node pool
type NodePool struct {
	serviceName string
	NodeID      string

	mu    sync.Mutex
	nodes *consistenthash.Map

	jobMetas []*driver.JobMeta

	Driver         driver.Driver
	hashReplicas   int
	hashFn         consistenthash.Hash
	updateDuration time.Duration

	client *Client

	isRun bool
}

func newNodePool(serverName string, driver driver.Driver, dcron *Client, updateDuration time.Duration, hashReplicas int) *NodePool {

	err := driver.Ping()
	if err != nil {
		panic(err)
	}

	nodePool := &NodePool{
		Driver:         driver,
		serviceName:    serverName,
		client:         dcron,
		hashReplicas:   hashReplicas,
		updateDuration: updateDuration,
		isRun:          false,
	}
	return nodePool
}

// StartPool Start Service Watch Pool
func (np *NodePool) StartPool() error {
	if np.isRun {
		return nil
	}
	var err error
	np.Driver.SetTimeout(np.updateDuration)
	np.NodeID, err = np.Driver.RegisterServiceNode(np.serviceName)
	if err != nil {
		return err
	}
	np.Driver.SetHeartBeat(np.NodeID)

	err = np.updatePool()
	if err != nil {
		return err
	}

	go np.tickerUpdatePool()

	np.isRun = true

	return nil
}

func (np *NodePool) updatePool() error {
	np.mu.Lock()
	defer np.mu.Unlock()
	nodes, err := np.Driver.GetServiceNodeList(np.serviceName)
	if err != nil {
		return err
	}
	np.nodes = consistenthash.New(np.hashReplicas, np.hashFn)
	for _, node := range nodes {
		np.nodes.Add(node)
	}

	jobList, err := np.Driver.GetJobList(np.serviceName)
	if err != nil {
		return err
	}
	oldBs, err := json.Marshal(np.jobMetas)
	if err != nil {
		return err
	}
	newBs, err := json.Marshal(jobList)
	np.jobMetas = jobList
	if string(oldBs) != string(newBs) {
		go np.jobMetaChange()
	}

	return nil
}
func (np *NodePool) tickerUpdatePool() {
	tickers := time.NewTicker(np.updateDuration)
	for range tickers.C {
		if np.client.isRun {
			err := np.updatePool()
			if err != nil {
				np.client.err("update node pool error %+v", err)
			}
		}
	}
}

//PickNodeByJobName : 使用一致性hash算法根据任务名获取一个执行节点
func (np *NodePool) PickNodeByJobName(jobName string) string {
	np.mu.Lock()
	defer np.mu.Unlock()
	if np.nodes.IsEmpty() {
		return ""
	}
	return np.nodes.Get(jobName)
}

// 任务元数据变动
func (np *NodePool) jobMetaChange() {
	np.client.info("任务元数据变动")
	np.client.reloadJobMeta(np.jobMetas)
}
