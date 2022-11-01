package etcd

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (e *EtcdDriver) AddService(serviceName string) (string, error) {
	servicePath := e.getServicePath(serviceName)
	ctx := context.Background()
	_, err := e.cli.Put(ctx, servicePath, serviceName)
	if err != nil {
		return "", err
	}
	return servicePath, nil
}

func (e *EtcdDriver) RemoveService(serviceName string) (string, error) {
	servicePath := e.getServicePath(serviceName)
	ctx := context.Background()
	_, err := e.cli.Delete(ctx, servicePath)
	if err != nil {
		return "", err
	}
	return servicePath, nil
}

func (e *EtcdDriver) GetServiceList() ([]string, error) {

	pathPrefix := e.getServicePrefix()
	ctx := context.Background()
	getResponse, err := e.cli.Get(ctx, pathPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	rs := make([]string, 0)
	for _, kv := range getResponse.Kvs {
		rs = append(rs, string(kv.Value))
	}
	return rs, nil
}

func (e *EtcdDriver) getServicePath(serviceName string) string {
	return e.getServicePrefix() + serviceName
}

func (e *EtcdDriver) getServicePrefix() string {
	return "dcron-service/"
}
