package redis_cluster

import (
	"fmt"
	"github.com/google/uuid"
)

func (rd *RedisClusterDriver) getNodeKeyPre(serviceName string) string {
	return fmt.Sprintf("%s%s%s", "dcron:", serviceName+":", "nodes:")
}

//GetServiceNodeList get a service node  list on redis cluster
func (rd *RedisClusterDriver) GetServiceNodeList(serviceName string) ([]string, error) {
	mathStr := fmt.Sprintf("%s*", rd.getNodeKeyPre(serviceName))
	return rd.scan(mathStr)
}

//RegisterServiceNode  register a service node
func (rd *RedisClusterDriver) RegisterServiceNode(serviceName string) (nodeID string, err error) {

	nodeID = uuid.New().String()

	key := rd.getNodeKeyPre(serviceName) + nodeID

	if err := rd.redisClient.Set(rd.ctx, key, nodeID, rd.timeout).Err(); err != nil {
		return "", err
	}
	return key, nil
}
