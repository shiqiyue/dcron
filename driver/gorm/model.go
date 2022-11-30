package gorm

import (
	"gorm.io/gorm"
	"time"
)

//go:generate goqueryset -in $GOFILE
// gen:qs
type JobMeta struct {
	ID          int64          `gorm:"primarykey"`
	CreatedAt   time.Time      `gorm:"not null"`
	UpdatedAt   time.Time      `gorm:"not null"`
	DeletedAt   gorm.DeletedAt `gorm:"index"`
	ServiceName string         `gorm:"not null; size:255; comment:服务名称"`
	JobName     string         `gorm:"not null; size:255; comment:作业名称"`
	Cron        string         `gorm:"not null; size:255; comment:cron表达式"`
}

func (j JobMeta) TableName() string {
	return "dcron_job_meta"
}

//go:generate goqueryset -in $GOFILE
// gen:qs
type JobService struct {
	ID          int64          `gorm:"primarykey"`
	CreatedAt   time.Time      `gorm:"not null"`
	UpdatedAt   time.Time      `gorm:"not null"`
	DeletedAt   gorm.DeletedAt `gorm:"index"`
	ServiceName string         `gorm:"not null; size:255; comment:服务名称"`
}

func (j JobService) TableName() string {
	return "dcron_job_service"
}

//go:generate goqueryset -in $GOFILE
// gen:qs
type JobNode struct {
	NodeId      string    `gorm:"primarykey; size:255; comment:节点ID"`
	CreatedAt   time.Time `gorm:"not null"`
	UpdatedAt   time.Time `gorm:"not null"`
	ServiceName string    `gorm:"not null; index:idx_service_expired,priority:1; size:255; comment:服务名称"`
	ExpiredAt   int64     `gorm:"not null; index:idx_service_expired,priority:1; comment:过期时间戳"`
}

func (j JobNode) TableName() string {
	return "dcron_job_node"
}

//go:generate goqueryset -in $GOFILE
// gen:qs
type MetaVersion struct {
	ID        int64     `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"not null"`
	UpdatedAt time.Time `gorm:"not null"`
	C         int64     `gorm:"not null;  版本号"`
}

func (j MetaVersion) TableName() string {
	return "dcron_meta_version"
}
