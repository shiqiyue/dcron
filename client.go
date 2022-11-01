package dcron

import (
	"errors"
	"github.com/libi/dcron/consistenthash"
	"github.com/libi/dcron/driver"
	"github.com/robfig/cron/v3"
	"log"
	"os"
	"sync"
	"time"
)

const defaultReplicas = 50
const defaultNodeDuration = time.Second
const defaultJobMetaDuration = time.Second * 5

//Client is main struct
type Client struct {
	ServiceName string
	crOptions   []cron.Option
	logger      interface{ Printf(string, ...interface{}) }
	Driver      driver.Driver

	cr    *cron.Cron
	lock  *sync.RWMutex
	isRun bool

	// 节点信息
	NodeID             string
	nodeIds            []string
	nodes              *consistenthash.Map
	hashFn             consistenthash.Hash
	hashReplicas       int
	nodeUpdateDuration time.Duration
	nodeLock           *sync.RWMutex

	// 作业元数据信息
	jobMetas              []*driver.JobMeta
	jobMetaUpdateDuration time.Duration
	registerJobs          map[string]*JobWarpper
	jobMetaLock           *sync.RWMutex
}

//NewClient create a Client
func NewClient(serviceName string, driver driver.Driver, cronOpts ...cron.Option) *Client {
	err := driver.Ping()
	if err != nil {
		panic(err)
	}
	dcron := newClient(serviceName)
	driver.SetTimeout(dcron.nodeUpdateDuration)
	dcron.Driver = driver
	dcron.crOptions = cronOpts
	dcron.registerJobs = make(map[string]*JobWarpper, 0)
	dcron.lock = &sync.RWMutex{}
	dcron.nodeLock = &sync.RWMutex{}
	dcron.jobMetaLock = &sync.RWMutex{}
	return dcron
}

func newCron(cronOpts ...cron.Option) *cron.Cron {
	return cron.New(cronOpts...)
}

//NewClientWithOption create a Client with Client Option
func NewClientWithOption(serviceName string, driver driver.Driver, dcronOpts ...Option) *Client {
	dcron := newClient(serviceName)
	for _, opt := range dcronOpts {
		opt(dcron)
	}
	return dcron
}

func newClient(serviceName string) *Client {
	return &Client{
		ServiceName:           serviceName,
		logger:                log.New(os.Stdout, "[client] ", log.LstdFlags),
		crOptions:             make([]cron.Option, 0),
		nodeUpdateDuration:    defaultNodeDuration,
		jobMetaUpdateDuration: defaultJobMetaDuration,
		hashReplicas:          defaultReplicas,
	}
}

//RegisterJob  register a job
func (d *Client) RegisterJob(jobName string, job Job) (err error) {
	return d.registerJob(jobName, nil, job)
}

//RegisterFunc add a cron func
func (d *Client) RegisterFunc(jobName string, cmd func()) (err error) {
	return d.registerJob(jobName, cmd, nil)
}

// registerJob register a job
func (d *Client) registerJob(jobName string, cmd func(), job Job) (err error) {
	d.info("register job '%s'", jobName)
	if _, ok := d.registerJobs[jobName]; ok {
		return errors.New("jobName already exist")
	}
	innerJob := JobWarpper{
		Name:  jobName,
		Func:  cmd,
		Job:   job,
		Dcron: d,
	}
	d.registerJobs[jobName] = &innerJob
	return nil
}

// allowThisNodeRun 判断作业是否在改节点运行
func (d *Client) allowThisNodeRun(jobName string) bool {
	allowRunNode := d.PickNodeByJobName(jobName)
	d.info("job '%s' running in node %s", jobName, allowRunNode)
	if allowRunNode == "" {
		d.err("node pool is empty")
		return false
	}
	return d.NodeID == allowRunNode
}

//Start start job
func (d *Client) Start() {
	d.isRun = true
	err := d.startNodeWatch()
	if err != nil {
		d.isRun = false
		d.err("client start node pool error %+v", err)
		return
	}
	err = d.startJobMetaWatch()
	if err != nil {
		d.isRun = false
		d.err("client start job meta pool error %+v", err)
		return
	}
	d.info("client started , nodeID is %s", d.NodeID)
}

func (d *Client) reloadJobMeta(jobMetas []*driver.JobMeta) {
	d.info("刷新任务元数据-开始")
	d.stop()
	d.cr = newCron(d.crOptions...)
	if len(jobMetas) == 0 {
		return
	}
	for _, jobMeta := range jobMetas {
		jobWarpper := d.registerJobs[jobMeta.JobName]
		if jobWarpper != nil {
			jobWarpper.CronStr = jobMeta.Cron
			entryID, err := d.cr.AddJob(jobMeta.Cron, jobWarpper)
			if err != nil {
				d.err("添加任务异常:%v", err)
				continue
			}
			jobWarpper.ID = entryID
		}
	}
	d.cr.Start()
	d.isRun = true
	d.info("刷新任务元数据-结束")

}

//Stop stop job
func (d *Client) Stop() {
	d.stop()
	d.info("client stopped")
}

func (d Client) stop() {
	d.isRun = false
	if d.cr != nil {
		d.cr.Stop()
	}
}
