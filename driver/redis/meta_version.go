package redis

import "context"

func (d *RedisDriver) getMetaVersionKey() string {
	return "dcron:meta-version"
}

func (d *RedisDriver) getMetaVersion() (int64, error) {
	getResp := d.redisClient.Get(context.Background(), d.getMetaVersionKey())
	if getResp.Err() != nil {
		return 0, getResp.Err()
	}
	if getResp.Val() == "" {
		return 0, nil
	}
	version, err := getResp.Int64()
	if err != nil {
		return 0, err
	}
	return version, nil

}

func (d *RedisDriver) incrMetaVersion() (int64, error) {
	val, err := d.redisClient.Incr(context.Background(), d.getMetaVersionKey()).Result()
	return val, err
}
