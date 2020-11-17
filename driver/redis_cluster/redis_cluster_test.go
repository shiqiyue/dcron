package redis_cluster

import "testing"

func TestClusterScan(t *testing.T) {
	rd, err := NewDriver(&Conf{
		Addrs: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		t.Error(err)
	}
	matchStr := rd.getKeyPre("service")
	ret, err := rd.scan(matchStr)
	if err != nil {
		//todo travis ci not support redis cluster
		//t.Error(err)
	}
	t.Log(ret)
}
