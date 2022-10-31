package etcd

import (
	"context"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// GetServiceNodeList get service notes
func (e *EtcdDriver) GetServiceNodeList(serviceName string) ([]string, error) {
	return e.getServiceNodes(serviceName), nil
}

// RegisterServiceNode register a node to service
func (e *EtcdDriver) RegisterServiceNode(serviceName string) (string, error) {
	nodeId := e.randNodeID(serviceName)
	_, err := e.putKeyWithLease(nodeId, nodeId)
	if err != nil {
		return "", err
	}
	err = e.watchService(serviceName)
	if err != nil {
		return "", err
	}
	return nodeId, nil
}

func (s *EtcdDriver) randNodeID(serviceName string) (nodeID string) {
	return getNodePrefix(serviceName) + uuid.New().String()
}

func getNodePrefix(serviceName string) string {
	return "dcron/" + serviceName + "/nodes/"
}

// watchNodes 监听节点变动
func (s *EtcdDriver) watchNodes(serviceName string) {
	prefix := getNodePrefix(serviceName)
	rch := s.cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				s.putServiceNode(serviceName, string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				s.delServiceNode(serviceName, string(ev.Kv.Key))
			}
		}
	}
}

// putServiceNode 新增服务地址
func (s *EtcdDriver) putServiceNode(serviceName, key, val string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if nodeMap, ok := s.serviceNodeList[serviceName]; !ok {
		nodeMap = map[string]string{
			key: val,
		}
		s.serviceNodeList[serviceName] = nodeMap
	} else {
		s.serviceNodeList[serviceName][key] = val
	}
}

// delServiceNode 删除服务地址
func (s *EtcdDriver) delServiceNode(serviceName, key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if nodeMap, ok := s.serviceNodeList[serviceName]; ok {
		delete(nodeMap, key)
	}
}

// getServiceNodes 获取服务地址
func (s *EtcdDriver) getServiceNodes(serviceName string) []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	addrs := make([]string, 0)
	if nodeMap, ok := s.serviceNodeList[serviceName]; ok {
		for _, v := range nodeMap {
			addrs = append(addrs, v)
		}
	}
	return addrs
}
