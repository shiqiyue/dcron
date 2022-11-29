package gorm

import (
	"github.com/libi/dcron/driver"
	"gorm.io/gorm"
	"log"
	"time"
)

type GormDriver struct {
	DB      *gorm.DB
	timeout time.Duration
	// 元数据版本号
	metaVersion        map[string]int64
	serviceJobMetaList map[string][]*driver.JobMeta
}

func (g *GormDriver) Ping() error {
	return nil
}

func (g *GormDriver) SetHeartBeat(nodeID string) {
	go g.heartBear(nodeID)
}

func (d *GormDriver) heartBear(nodeID string) {

	//每间隔timeout/2设置一次key的超时时间为timeout
	tickers := time.NewTicker(d.timeout / 2)
	for range tickers.C {
		expiredAt := time.Now().Add(d.timeout)
		err := d.updateNodeExpiredAt(nodeID, expiredAt)
		if err != nil {
			log.Printf("gorm expire error %+v", err)
			continue
		}
	}
}

func (g *GormDriver) SetTimeout(timeout time.Duration) {
	g.timeout = timeout
}

func NewDriver(DB *gorm.DB) (*GormDriver, error) {

	err := DB.AutoMigrate(&JobMeta{}, &JobNode{}, &JobService{}, &MetaVersion{})
	if err != nil {
		return nil, err
	}
	return &GormDriver{
		DB:                 DB,
		serviceJobMetaList: make(map[string][]*driver.JobMeta, 0),
		metaVersion:        make(map[string]int64, 0)}, nil
}
