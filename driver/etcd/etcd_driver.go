package etcd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/libi/dcron/driver"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultLease    = 5 // 5 second ttl
	dialTimeout     = 3 * time.Second
	businessTimeout = 5 * time.Second
)

type EtcdDriver struct {
	cli *clientv3.Client
	// ttl
	lease              int64
	serviceNodeList    map[string]map[string]string
	serviceJobMetaList map[string]map[string]*driver.JobMeta
	lock               sync.RWMutex
	leaseID            clientv3.LeaseID
}

//NewEtcdDriver ...
func NewEtcdDriver(config *clientv3.Config) (*EtcdDriver, error) {
	cli, err := clientv3.New(*config)
	if err != nil {
		return nil, err
	}

	ser := &EtcdDriver{
		cli:                cli,
		serviceNodeList:    make(map[string]map[string]string, 10),
		serviceJobMetaList: make(map[string]map[string]*driver.JobMeta, 10),
	}

	return ser, nil
}

func (e *EtcdDriver) Ping() error {
	return nil
}

func (e *EtcdDriver) SetHeartBeat(nodeID string, serviceName string) {
	leaseCh, err := e.keepAlive(context.Background(), nodeID)
	if err != nil {
		log.Printf("setHeartBeat error: %v", err)
		return
	}
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				log.Printf("keepAlive panic: %v", err)
				return
			}
		}()
		for {
			select {
			case _, ok := <-leaseCh:
				if !ok {
					e.revoke()
					e.SetHeartBeat(nodeID, "")
					return
				}
			case <-time.After(businessTimeout):
				log.Printf("ectd cli keepalive timeout")
				return
			}
		}
	}()
}

// SetTimeout set etcd lease timeout
func (e *EtcdDriver) SetTimeout(timeout time.Duration) {
	e.lease = int64(timeout.Seconds())
}

//设置key value，绑定租约
func (s *EtcdDriver) putKeyWithLease(key, val string) (clientv3.LeaseID, error) {
	//设置租约时间，最少5s
	if s.lease < defaultLease {
		s.lease = defaultLease
	}

	ctx, cancel := context.WithTimeout(context.Background(), businessTimeout)
	defer cancel()

	// create a lease
	resp, err := s.cli.Grant(ctx, s.lease)
	if err != nil {
		return 0, err
	}
	leaseId := resp.ID

	//注册服务并绑定租约
	_, err = s.cli.Put(ctx, key, val, clientv3.WithLease(leaseId))
	if err != nil {
		return 0, err
	}

	return leaseId, nil
}

//WatchService 初始化服务列表和监视
func (s *EtcdDriver) watchService(serviceName string) error {
	nodePrefix := getNodePrefix(serviceName)
	// 根据前缀获取现有的key
	resp, err := s.cli.Get(context.Background(), nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, ev := range resp.Kvs {
		s.putServiceNode(serviceName, string(ev.Key), string(ev.Value))
	}

	jobMetaPrefix := getJobMetaPrefix(serviceName)
	resp, err = s.cli.Get(context.Background(), jobMetaPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, ev := range resp.Kvs {
		s.putServiceJobMeta(serviceName, string(ev.Key), string(ev.Value))
	}

	// 监视前缀，修改变更的server
	go s.watchNodes(serviceName)
	go s.watchJobMetas(serviceName)
	return nil
}

func (e *EtcdDriver) keepAlive(ctx context.Context, nodeID string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	var err error
	e.leaseID, err = e.putKeyWithLease(nodeID, nodeID)
	if err != nil {
		log.Printf("putKeyWithLease error: %v", err)
		return nil, err
	}

	return e.cli.KeepAlive(ctx, e.leaseID)
}

func (e *EtcdDriver) revoke() {
	_, err := e.cli.Lease.Revoke(context.Background(), e.leaseID)
	if err != nil {
		log.Printf("lease revoke error: %v", err)
	}
}
