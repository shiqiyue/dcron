package dcron

import (
	"fmt"
	"github.com/libi/dcron/driver"
	dgorm "github.com/libi/dcron/driver/gorm"
	dredis "github.com/libi/dcron/driver/redis"
	"github.com/robfig/cron/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func Test(t *testing.T) {

	drv, _ := dredis.NewDriver(&dredis.Conf{Addr: "127.0.0.1:6379"})
	_, err := drv.AddJob("server1", "s1 test1", "*/5 * * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = drv.AddJob("server1", "s1 test2", "*/7 * * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	/*client := NewClient("server1", drv)
	err := client.AddJob("server1", "s1 test1", "* * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = client.AddJob("server1", "s1 test2", "* * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}*/

	go runNode(t, drv, 1)
	go runNode(t, drv, 2)
	go runNode(t, drv, 3)

	//测试120秒后退出
	time.Sleep(20 * time.Second)
	_, err = drv.AddJob("server1", "s1 test2", "*/12 * * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(120 * time.Second)
}

func TestGormDriver(t *testing.T) {
	dsn := "root:root@tcp(127.0.0.1:3306)/go-admin?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		fmt.Println(err)
		return
	}
	drv, _ := dgorm.NewDriver(db, time.Second*5)
	_, err = drv.AddJob("server1", "s1 test1", "*/5 * * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = drv.AddJob("server1", "s1 test2", "*/7 * * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	/*client := NewClient("server1", drv)
	err := client.AddJob("server1", "s1 test1", "* * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = client.AddJob("server1", "s1 test2", "* * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}*/

	go runNode(t, drv, 1)
	go runNode(t, drv, 2)
	go runNode(t, drv, 3)

	//测试120秒后退出
	time.Sleep(20 * time.Second)
	_, err = drv.AddJob("server1", "s1 test2", "*/12 * * * * *")
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(120 * time.Second)
}

func runNode(t *testing.T, drv driver.Driver, nodeId int) {
	dcron := NewClient("server1", drv, cron.WithSeconds())
	//添加多个任务 启动多个节点时 任务会均匀分配给各个节点

	err := dcron.RegisterFunc("s1 test1", func() {
		fmt.Println("run s1 test1")
		t.Log("执行 service1 test1 任务", time.Now().Format("15:04:05"), nodeId)

	})
	if err != nil {
		t.Error("add func error")
	}
	err = dcron.RegisterFunc("s1 test2", func() {
		t.Log("执行 service1 test2 任务", time.Now().Format("15:04:05"), nodeId)
	})
	if err != nil {
		t.Error("add func error")
	}

	dcron.Start()

}
