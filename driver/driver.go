package driver

import "time"

// 作业元数据
type JobMeta struct {
	ServiceName string
	JobName     string
	Cron        string
}

//Driver is a driver interface
type Driver interface {
	// Ping is check dirver is valid
	Ping() error
	SetHeartBeat(nodeID string)
	SetTimeout(timeout time.Duration)
	GetServiceNodeList(ServiceName string) ([]string, error)
	RegisterServiceNode(ServiceName string) (string, error)
	// 添加作业
	AddJob(serviceName string, jobName string, cron string) (string, error)
	// 删除作业
	RemoveJob(serviceName string, jobName string) (string, error)
	// 更新作业
	UpdateJob(serviceName string, jobName, cron string) (string, error)
	// 获取作业列表
	GetJobList(serviceName string) ([]*JobMeta, error)
}
