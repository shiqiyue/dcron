package redis_cluster

import (
	"context"
	"strings"
)

func (e *RedisClusterDriver) AddService(serviceName string) (string, error) {
	servicePath := e.getServicePath(serviceName)
	ctx := context.Background()
	err := e.redisClient.Set(ctx, servicePath, serviceName, 0).Err()
	if err != nil {
		return "", err
	}
	return servicePath, nil
}

func (e *RedisClusterDriver) RemoveService(serviceName string) (string, error) {
	servicePath := e.getServicePath(serviceName)
	ctx := context.Background()
	err := e.redisClient.Del(ctx, servicePath).Err()
	if err != nil {
		return "", err
	}
	return servicePath, nil
}

func (e *RedisClusterDriver) GetServiceList() ([]string, error) {

	pathPrefix := e.getServicePrefix()
	keys, err := e.scan(pathPrefix + "*")
	if err != nil {
		return nil, err
	}
	rs := make([]string, 0)
	for _, key := range keys {
		r := strings.ReplaceAll(key, pathPrefix, "")
		rs = append(rs, r)
	}
	return rs, nil

}

func (e *RedisClusterDriver) getServicePath(serviceName string) string {
	return e.getServicePrefix() + serviceName
}

func (e *RedisClusterDriver) getServicePrefix() string {
	return "dcron-service:"
}
