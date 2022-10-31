package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/libi/dcron/driver"
)

func (rd *RedisDriver) AddJob(serviceName string, jobName string, cron string) (string, error) {
	jobPath := rd.getJobMetaPath(serviceName, jobName)
	jobMetaBs, err := rd.marshalJobMeta(&driver.JobMeta{
		ServiceName: serviceName,
		JobName:     jobName,
		Cron:        cron,
	})
	if err != nil {
		return "", err
	}
	err = rd.redisClient.Set(context.Background(), jobPath, string(jobMetaBs), 0).Err()
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (rd *RedisDriver) RemoveJob(serviceName string, jobName string) (string, error) {
	jobPath := rd.getJobMetaPath(serviceName, jobName)
	err := rd.redisClient.Del(context.Background(), jobPath).Err()
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (rd *RedisDriver) UpdateJob(serviceName string, jobName, cron string) (string, error) {
	jobPath := rd.getJobMetaPath(serviceName, jobName)
	jobMetaBs, err := rd.marshalJobMeta(&driver.JobMeta{
		ServiceName: serviceName,
		JobName:     jobName,
		Cron:        cron,
	})
	err = rd.redisClient.Set(context.Background(), jobPath, string(jobMetaBs), 0).Err()
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (rd *RedisDriver) GetJobList(serviceName string) ([]*driver.JobMeta, error) {
	mathStr := fmt.Sprintf("%s*", rd.getJobMetaKeyPrefix(serviceName))
	jobMetaKeys, err := rd.scan(mathStr)
	if err != nil {
		return nil, err
	}
	if len(jobMetaKeys) == 0 {
		return []*driver.JobMeta{}, nil
	}
	jobMetas := make([]*driver.JobMeta, 0)
	for _, jobMetaKey := range jobMetaKeys {
		getResp := rd.redisClient.Get(context.Background(), jobMetaKey)
		if getResp.Err() != nil {
			return nil, getResp.Err()
		}

		jobMeta, err := rd.unMarshalJobMeta([]byte(getResp.Val()))
		if err != nil {
			return nil, err
		}
		jobMetas = append(jobMetas, jobMeta)
	}
	return jobMetas, nil
}

func (rd *RedisDriver) marshalJobMeta(jobMeta *driver.JobMeta) ([]byte, error) {
	return json.Marshal(jobMeta)
}

func (rd *RedisDriver) unMarshalJobMeta(bs []byte) (*driver.JobMeta, error) {
	r := &driver.JobMeta{}
	err := json.Unmarshal(bs, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (rd RedisDriver) getJobMetaKeyPrefix(serviceName string) string {
	return fmt.Sprintf("%s%s%s", "dcron:", serviceName+":", "jobs:")
}

func (rd RedisDriver) getJobMetaPath(serviceName string, jobName string) string {
	return rd.getJobMetaKeyPrefix(serviceName) + jobName
}
