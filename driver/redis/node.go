package redis

import (
	"context"
	"fmt"
	"github.com/google/uuid"
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
	nodeID = uuid.New().String()

	key := rd.getNodeKeyPrefix(serviceName) + nodeID

	if err := rd.redisClient.Set(context.Background(), key, nodeID, rd.timeout).Err(); err != nil {
		return "", err
	}
	return key, nil
}
