package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

// RedisConf is redis config
type Conf struct {
	Proto string

	// first use addr
	Addr     string
	Password string

	Host string
	Port int

	MaxActive   int
	MaxIdle     int
	IdleTimeout time.Duration
	Wait        bool
}

// RedisDriver is redisDriver
type RedisDriver struct {
	conf        *Conf
	redisClient *redis.Client
	timeout     time.Duration
	Key         string
}

// NewDriver return a redis driver
func NewDriver(conf *Conf) (*RedisDriver, error) {
	opts := &redis.Options{
		Addr:     conf.Addr,
		Password: conf.Password,
	}
	redisClient := redis.NewClient(opts)

	return &RedisDriver{
		conf:        conf,
		redisClient: redisClient,
	}, nil
}

// Ping is check redis valid
func (rd *RedisDriver) Ping() error {
	if err := rd.redisClient.Set(context.Background(), "ping", "pong", 0).Err(); err != nil {
		return err
	}
	return nil
}

//SetTimeout set redis timeout
func (rd *RedisDriver) SetTimeout(timeout time.Duration) {
	rd.timeout = timeout
}

//SetHeartBeat set herbear
func (rd *RedisDriver) SetHeartBeat(nodeID string) {
	go rd.heartBear(nodeID)
}
func (rd *RedisDriver) heartBear(nodeID string) {

	//每间隔timeout/2设置一次key的超时时间为timeout
	key := nodeID
	tickers := time.NewTicker(rd.timeout / 2)
	for range tickers.C {
		if err := rd.redisClient.Expire(context.Background(), key, rd.timeout).Err(); err != nil {
			log.Printf("redis expire error %+v", err)
			continue
		}
	}
}

func (rd *RedisDriver) scan(matchStr string) ([]string, error) {
	ret := make([]string, 0)
	iter := rd.redisClient.Scan(context.Background(), 0, matchStr, -1).Iterator()
	for iter.Next(context.Background()) {
		ret = append(ret, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return ret, nil
}
