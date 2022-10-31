package dcron

import (
	"fmt"
	dredis "github.com/libi/dcron/driver/redis"
	"testing"
	"time"
)

type TestJob1 struct {
	Name string
}

func (t TestJob1) Run() {
	fmt.Println("执行 testjob ", t.Name, time.Now().Format("15:04:05"))
}

var testData = make(map[string]struct{})

func Test(t *testing.T) {

	drv, _ := dredis.NewDriver(&dredis.Conf{Addr: "127.0.0.1:6379"})
	dcron := NewDcron("server1", drv)
	err := dcron.AddJob("server1", "s1 test1", "*/3 * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = dcron.AddJob("server1", "s1 test2", "* * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}

	go runNode(t, drv)
	// 间隔1秒启动测试节点刷新逻辑
	time.Sleep(time.Second)
	go runNode(t, drv)
	time.Sleep(time.Second * 2)
	go runNode(t, drv)

	//测试120秒后退出
	time.Sleep(120 * time.Second)
	t.Log("testData", testData)
}

func runNode(t *testing.T, drv *dredis.RedisDriver) {
	dcron := NewDcron("server1", drv)
	//添加多个任务 启动多个节点时 任务会均匀分配给各个节点

	err := dcron.RegisterFunc("s1 test1", func() {
		// 同时启动3个节点 但是一个 job 同一时间只会执行一次 通过 map 判重
		key := "s1 test1" + time.Now().Format("15:04:05")
		if _, ok := testData[key]; ok {
			t.Error("job have running in other node")
		}
		testData[key] = struct{}{}
	})
	if err != nil {
		t.Error("add func error")
	}
	err = dcron.RegisterFunc("s1 test2", func() {
		t.Log("执行 service1 test2 任务", time.Now().Format("15:04:05"))
	})
	if err != nil {
		t.Error("add func error")
	}

	dcron.Start()

}
