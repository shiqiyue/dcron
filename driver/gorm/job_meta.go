package gorm

import (
	"github.com/libi/dcron/driver"
	"gorm.io/gorm"
)

func (g *GormDriver) AddJob(serviceName string, jobName string, cron string) (string, error) {
	err := g.DB.Transaction(func(tx *gorm.DB) error {
		updateNum, err := NewJobMetaQuerySet(tx).ServiceNameEq(serviceName).JobNameEq(jobName).GetUpdater().SetCron(cron).UpdateNum()
		if err != nil {
			return err
		}
		if updateNum == 0 {
			jobMeta := &JobMeta{
				ServiceName: serviceName,
				JobName:     jobName,
				Cron:        cron,
			}
			err := jobMeta.Create(tx)
			if err != nil {
				return err
			}
		}
		err = g.incrMetaVersion(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	return jobName, nil
}

func (g *GormDriver) RemoveJob(serviceName string, jobName string) (string, error) {
	err := g.DB.Transaction(func(tx *gorm.DB) error {
		err := NewJobMetaQuerySet(tx).ServiceNameEq(serviceName).JobNameEq(jobName).Delete()
		if err != nil {
			return err
		}
		err = g.incrMetaVersion(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return jobName, nil
}

func (g *GormDriver) UpdateJob(serviceName string, jobName, cron string) (string, error) {
	err := g.DB.Transaction(func(tx *gorm.DB) error {
		updateNum, err := NewJobMetaQuerySet(tx).ServiceNameEq(serviceName).JobNameEq(jobName).GetUpdater().SetCron(cron).UpdateNum()
		if err != nil {
			return err
		}
		if updateNum == 0 {
			jobMeta := &JobMeta{
				ServiceName: serviceName,
				JobName:     jobName,
				Cron:        cron,
			}
			err := jobMeta.Create(tx)
			if err != nil {
				return err
			}
		}
		err = g.incrMetaVersion(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	return jobName, nil
}

func (g *GormDriver) GetJobList(serviceName string) ([]*driver.JobMeta, error) {
	djobMetas, jobMetasExist := g.serviceJobMetaList[serviceName]
	if jobMetasExist {
		currentMetaVersion, err := g.getMetaVersion()
		if err != nil {
			return nil, err
		}
		oldMetaVersion := g.metaVersion
		g.metaVersion = currentMetaVersion
		if currentMetaVersion == oldMetaVersion {
			return djobMetas, nil
		}
	}

	jobMetas := make([]*JobMeta, 0)
	err := NewJobMetaQuerySet(g.DB).ServiceNameEq(serviceName).All(&jobMetas)
	if err != nil {
		return nil, err
	}
	rs := make([]*driver.JobMeta, 0)
	if len(jobMetas) > 0 {
		for _, jobMeta := range jobMetas {
			rs = append(rs, &driver.JobMeta{
				ServiceName: jobMeta.ServiceName,
				JobName:     jobMeta.JobName,
				Cron:        jobMeta.Cron,
			})
		}
	}
	g.serviceJobMetaList[serviceName] = rs
	return rs, nil

}
