package redis

import (
	"fmt"
	"github.com/google/uuid"
	"time"
)

func (rd *RedisDriver) getNodeKeyPrefix(serviceName string) string {
	return fmt.Sprintf("%s%s%s", "dcron:", serviceName+":", "nodes:")
}

//GetServiceNodeList get a serveice node  list
func (rd *RedisDriver) GetServiceNodeList(serviceName string) ([]string, error) {
	mathStr := fmt.Sprintf("%s*", rd.getNodeKeyPrefix(serviceName))
	return rd.scan(mathStr)
}

//RegisterServiceNode  register a service node
func (rd *RedisDriver) RegisterServiceNode(serviceName string) (nodeID string, err error) {
	nodeID = rd.randNodeID(serviceName)
	if err := rd.registerServiceNode(nodeID); err != nil {
		return "", err
	}
	return nodeID, nil
}

func (rd *RedisDriver) randNodeID(serviceName string) (nodeID string) {
	return rd.getNodeKeyPrefix(serviceName) + uuid.New().String()
}

func (rd *RedisDriver) registerServiceNode(nodeID string) error {
	_, err := rd.do("SETEX", nodeID, int(rd.timeout/time.Second), nodeID)
	return err
}
