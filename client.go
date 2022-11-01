package dcron

import (
	"errors"
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
	ServerName string
	crOptions  []cron.Option

	registerJobs          map[string]*JobWarpper
	nodePool              *NodePool
	isRun                 bool
	logger                interface{ Printf(string, ...interface{}) }
	nodeUpdateDuration    time.Duration
	jobMetaUpdateDuration time.Duration
	hashReplicas          int
	cr                    *cron.Cron
	lock                  *sync.RWMutex
}

//NewClient create a Client
func NewClient(serverName string, driver driver.Driver, cronOpts ...cron.Option) *Client {
	dcron := newClient(serverName)
	dcron.crOptions = cronOpts
	dcron.registerJobs = make(map[string]*JobWarpper, 0)
	dcron.nodePool = newNodePool(serverName, driver, dcron, dcron.nodeUpdateDuration, dcron.hashReplicas)
	dcron.lock = &sync.RWMutex{}
	return dcron
}

func newCron(cronOpts ...cron.Option) *cron.Cron {
	return cron.New(cronOpts...)
}

//NewClientWithOption create a Client with Client Option
func NewClientWithOption(serverName string, driver driver.Driver, dcronOpts ...Option) *Client {
	dcron := newClient(serverName)
	for _, opt := range dcronOpts {
		opt(dcron)
	}
	dcron.nodePool = newNodePool(serverName, driver, dcron, dcron.nodeUpdateDuration, dcron.hashReplicas)
	return dcron
}

func newClient(serverName string) *Client {
	return &Client{
		ServerName:            serverName,
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
	allowRunNode := d.nodePool.PickNodeByJobName(jobName)
	d.info("job '%s' running in node %s", jobName, allowRunNode)
	if allowRunNode == "" {
		d.err("node pool is empty")
		return false
	}
	return d.nodePool.NodeID == allowRunNode
}

//Start start job
func (d *Client) Start() {
	d.isRun = true
	err := d.nodePool.StartPool()
	if err != nil {
		d.isRun = false
		d.err("client start node pool error %+v", err)
		return
	}
	d.info("client started , nodeID is %s", d.nodePool.NodeID)
}

func (d *Client) reloadJobMeta(jobMetas []*driver.JobMeta) {
	d.Stop()
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
}

//Stop stop job
func (d *Client) Stop() {
	d.isRun = false
	if d.cr != nil {
		d.cr.Stop()
	}
	d.info("client stopped")
}

func (d *Client) AddJob(serviceName string, jobName string, cron string) error {
	_, err := d.nodePool.Driver.AddJob(serviceName, jobName, cron)
	return err
}

func (d *Client) RemoveJob(serviceName string, jobName string) error {
	_, err := d.nodePool.Driver.RemoveJob(serviceName, jobName)
	return err
}

func (d *Client) UpdateJob(serviceName string, jobName string, cron string) error {
	_, err := d.nodePool.Driver.UpdateJob(serviceName, jobName, cron)
	return err
}

func (d *Client) GetJobList(serviceName string) ([]*driver.JobMeta, error) {
	return d.nodePool.Driver.GetJobList(serviceName)
}
