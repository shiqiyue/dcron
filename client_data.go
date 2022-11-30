package dcron

import (
	"encoding/json"
	"github.com/libi/dcron/consistenthash"
	"time"
)

func (d *Client) loadNodes() error {
	nodes, err := d.Driver.GetServiceNodeList(d.ServiceName)
	if err != nil {
		return err
	}
	oldBs, err := json.Marshal(d.nodeIds)
	if err != nil {
		return err
	}
	newBs, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	d.nodeIds = nodes
	if string(oldBs) != string(newBs) {
		ns := consistenthash.New(d.hashReplicas, d.hashFn)
		for _, node := range nodes {
			ns.Add(node)
		}
		d.nodes = ns
	}
	return nil
}

func (d *Client) loadJobMetas() error {
	jobList, err := d.Driver.GetJobList(d.ServiceName)
	if err != nil {
		return err
	}
	oldBs, err := json.Marshal(d.jobMetas)
	if err != nil {
		return err
	}
	newBs, err := json.Marshal(jobList)
	if err != nil {
		return err
	}
	d.jobMetas = jobList
	if string(oldBs) != string(newBs) {
		go d.reloadJobMeta(d.jobMetas)
	}
	return nil
}

func (d *Client) startNodeWatch() error {
	nodeId, err := d.Driver.RegisterServiceNode(d.ServiceName)
	if err != nil {
		return err
	}
	d.NodeID = nodeId
	d.Driver.SetHeartBeat(nodeId, d.ServiceName)
	err = d.loadNodes()
	if err != nil {
		return err
	}
	go func() {
		tickers := time.NewTicker(d.nodeUpdateDuration)
		for range tickers.C {
			if d.isRun {
				err := d.loadNodes()
				if err != nil {
					d.err("update node pool error %+v", err)
				}
			}
		}
	}()
	return nil
}

func (d *Client) startJobMetaWatch() error {
	err := d.loadJobMetas()
	if err != nil {
		return err
	}
	go func() {
		tickers := time.NewTicker(d.jobMetaUpdateDuration)
		for range tickers.C {
			if d.isRun {
				err := d.loadJobMetas()
				if err != nil {
					d.err("update node pool error %+v", err)
				}
			}
		}
	}()
	return nil
}

//PickNodeByJobName : 使用一致性hash算法根据任务名获取一个执行节点
func (d *Client) PickNodeByJobName(jobName string) string {
	if d.nodes.IsEmpty() {
		return ""
	}
	return d.nodes.Get(jobName)
}
