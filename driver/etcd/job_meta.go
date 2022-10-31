package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/libi/dcron/driver"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (e *EtcdDriver) AddJob(serviceName string, jobName string, cron string) (string, error) {
	jobPath := e.getJobPath(serviceName, jobName)
	ctx := context.Background()
	jobMetaBs, err := e.marshalJobMeta(&driver.JobMeta{
		ServiceName: serviceName,
		JobName:     jobName,
		Cron:        cron,
	})
	if err != nil {
		return "", err
	}
	_, err = e.cli.Put(ctx, jobPath, string(jobMetaBs))
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (e *EtcdDriver) RemoveJob(serviceName string, jobName string) (string, error) {
	jobPath := e.getJobPath(serviceName, jobName)
	ctx := context.Background()
	_, err := e.cli.Delete(ctx, jobPath)
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (e *EtcdDriver) UpdateJob(serviceName string, jobName string, cron string) (string, error) {
	jobPath := e.getJobPath(serviceName, jobName)
	ctx := context.Background()
	jobMetaBs, err := e.marshalJobMeta(&driver.JobMeta{
		ServiceName: serviceName,
		JobName:     jobName,
		Cron:        cron,
	})
	if err != nil {
		return "", err
	}
	_, err = e.cli.Put(ctx, jobPath, string(jobMetaBs))
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (e *EtcdDriver) GetJobList(serviceName string) ([]*driver.JobMeta, error) {
	return e.getServiceJobMetas(serviceName), nil
}

func (e *EtcdDriver) marshalJobMeta(jobMeta *driver.JobMeta) ([]byte, error) {
	return json.Marshal(jobMeta)
}

func (e *EtcdDriver) unMarshalJobMeta(bs []byte) (*driver.JobMeta, error) {
	r := &driver.JobMeta{}
	err := json.Unmarshal(bs, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (e *EtcdDriver) getJobPath(serviceName, jobName string) string {
	return getJobMetaPrefix(serviceName) + jobName
}

func getJobMetaPrefix(serviceName string) string {
	return "dcron/" + serviceName + "/jobs/"
}

// watchJobMetas 监听作业元数据变动
func (s *EtcdDriver) watchJobMetas(serviceName string) {
	prefix := getJobMetaPrefix(serviceName)
	rch := s.cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				s.putServiceJobMeta(serviceName, string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				s.delServiceJobMeta(serviceName, string(ev.Kv.Key))
			}
		}
	}
}

// putServiceJobMeta 新增服务地址
func (s *EtcdDriver) putServiceJobMeta(serviceName, key, val string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	jobMetaMap, ok := s.serviceJobMetaList[serviceName]
	if !ok {
		jobMetaMap = make(map[string]*driver.JobMeta, 0)
		s.serviceJobMetaList[serviceName] = jobMetaMap
	}
	jobMeta, err := s.unMarshalJobMeta([]byte(val))
	if err != nil {
		fmt.Println("反序列化jobMeta信息异常", err)
		return
	}
	jobMetaMap[key] = jobMeta
}

// delServiceJobMeta 删除服务地址
func (s *EtcdDriver) delServiceJobMeta(serviceName, key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if jobMetaMap, ok := s.serviceJobMetaList[serviceName]; ok {
		delete(jobMetaMap, key)
	}
}

// getServiceJobMetas 获取服务作业元数据
func (s *EtcdDriver) getServiceJobMetas(serviceName string) []*driver.JobMeta {
	s.lock.RLock()
	defer s.lock.RUnlock()
	jobMetas := make([]*driver.JobMeta, 0)
	if jobMetaMap, ok := s.serviceJobMetaList[serviceName]; ok {
		for _, v := range jobMetaMap {
			jobMetas = append(jobMetas, v)
		}
	}
	return jobMetas
}
