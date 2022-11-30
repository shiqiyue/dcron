package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/libi/dcron/driver"
	"log"
	"time"
)

// RedisConf is redis config
type Conf struct {
	Addr     string
	Password string
	DB       int
}

// RedisDriver is redisDriver
type RedisDriver struct {
	conf        *Conf
	redisClient *redis.Client
	timeout     time.Duration
	Key         string

	serviceJobMetaList map[string][]*driver.JobMeta
	// 元数据版本号
	metaVersion int64
}

// NewDriver return a redis driver
func NewDriver(conf *Conf) (*RedisDriver, error) {
	opts := &redis.Options{
		Addr:     conf.Addr,
		Password: conf.Password,
		DB:       conf.DB,
	}
	redisClient := redis.NewClient(opts)

	return &RedisDriver{
		conf:               conf,
		redisClient:        redisClient,
		serviceJobMetaList: make(map[string][]*driver.JobMeta, 0),
	}, nil
}

// Ping is check redis valid
func (d *RedisDriver) Ping() error {
	if err := d.redisClient.Set(context.Background(), "ping", "pong", 0).Err(); err != nil {
		return err
	}
	return nil
}

//SetTimeout set redis timeout
func (d *RedisDriver) SetTimeout(timeout time.Duration) {
	d.timeout = timeout
}

//SetHeartBeat set herbear
func (d *RedisDriver) SetHeartBeat(nodeID string, serviceName string) {
	go d.heartBear(nodeID)
}
func (d *RedisDriver) heartBear(nodeID string) {

	//每间隔timeout/2设置一次key的超时时间为timeout
	key := nodeID
	tickers := time.NewTicker(d.timeout / 2)
	for range tickers.C {
		if err := d.redisClient.Expire(context.Background(), key, d.timeout).Err(); err != nil {
			log.Printf("redis expire error %+v", err)
			continue
		}
	}
}

func (d *RedisDriver) scan(matchStr string) ([]string, error) {
	ret := make([]string, 0)
	iter := d.redisClient.Scan(context.Background(), 0, matchStr, -1).Iterator()
	for iter.Next(context.Background()) {
		ret = append(ret, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return ret, nil
}
