package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/libi/dcron/driver"
)

func (d *RedisDriver) AddJob(serviceName string, jobName string, cron string) (string, error) {
	jobPath := d.getJobMetaPath(serviceName, jobName)
	jobMetaBs, err := d.marshalJobMeta(&driver.JobMeta{
		ServiceName: serviceName,
		JobName:     jobName,
		Cron:        cron,
	})
	if err != nil {
		return "", err
	}
	err = d.redisClient.Set(context.Background(), jobPath, string(jobMetaBs), 0).Err()
	if err != nil {
		return "", err
	}
	_, err = d.incrMetaVersion()
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (d *RedisDriver) RemoveJob(serviceName string, jobName string) (string, error) {
	jobPath := d.getJobMetaPath(serviceName, jobName)
	err := d.redisClient.Del(context.Background(), jobPath).Err()
	if err != nil {
		return "", err
	}
	_, err = d.incrMetaVersion()
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (d *RedisDriver) UpdateJob(serviceName string, jobName, cron string) (string, error) {
	jobPath := d.getJobMetaPath(serviceName, jobName)
	jobMetaBs, err := d.marshalJobMeta(&driver.JobMeta{
		ServiceName: serviceName,
		JobName:     jobName,
		Cron:        cron,
	})
	err = d.redisClient.Set(context.Background(), jobPath, string(jobMetaBs), 0).Err()
	if err != nil {
		return "", err
	}
	_, err = d.incrMetaVersion()
	if err != nil {
		return "", err
	}
	return jobPath, nil
}

func (d *RedisDriver) GetJobList(serviceName string) ([]*driver.JobMeta, error) {
	jobMetas, jobMetasExist := d.serviceJobMetaList[serviceName]
	if jobMetasExist {
		currentMetaVersion, err := d.getMetaVersion()
		if err != nil {
			return nil, err
		}
		oldMetaVersion := d.metaVersion
		d.metaVersion = currentMetaVersion
		if currentMetaVersion == oldMetaVersion {
			return jobMetas, nil
		}
	}

	mathStr := fmt.Sprintf("%s*", d.getJobMetaKeyPrefix(serviceName))
	jobMetaKeys, err := d.scan(mathStr)
	if err != nil {
		return nil, err
	}
	if len(jobMetaKeys) == 0 {
		return []*driver.JobMeta{}, nil
	}
	jobMetas = make([]*driver.JobMeta, 0)
	for _, jobMetaKey := range jobMetaKeys {
		getResp := d.redisClient.Get(context.Background(), jobMetaKey)
		if getResp.Err() != nil {
			return nil, getResp.Err()
		}

		jobMeta, err := d.unMarshalJobMeta([]byte(getResp.Val()))
		if err != nil {
			return nil, err
		}
		jobMetas = append(jobMetas, jobMeta)
	}
	d.serviceJobMetaList[serviceName] = jobMetas
	return jobMetas, nil
}

func (d *RedisDriver) marshalJobMeta(jobMeta *driver.JobMeta) ([]byte, error) {
	return json.Marshal(jobMeta)
}

func (d *RedisDriver) unMarshalJobMeta(bs []byte) (*driver.JobMeta, error) {
	r := &driver.JobMeta{}
	err := json.Unmarshal(bs, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (d RedisDriver) getJobMetaKeyPrefix(serviceName string) string {
	return fmt.Sprintf("%s%s%s", "dcron:", serviceName+":", "jobs:")
}

func (d RedisDriver) getJobMetaPath(serviceName string, jobName string) string {
	return d.getJobMetaKeyPrefix(serviceName) + jobName
}
