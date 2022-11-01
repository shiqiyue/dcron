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

//Dcron is main struct
type Dcron struct {
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

//NewDcron create a Dcron
func NewDcron(serverName string, driver driver.Driver, cronOpts ...cron.Option) *Dcron {
	dcron := newDcron(serverName)
	dcron.crOptions = cronOpts
	dcron.registerJobs = make(map[string]*JobWarpper, 0)
	dcron.nodePool = newNodePool(serverName, driver, dcron, dcron.nodeUpdateDuration, dcron.hashReplicas)
	dcron.lock = &sync.RWMutex{}
	return dcron
}

func newCron(cronOpts ...cron.Option) *cron.Cron {
	return cron.New(cronOpts...)
}

//NewDcronWithOption create a Dcron with Dcron Option
func NewDcronWithOption(serverName string, driver driver.Driver, dcronOpts ...Option) *Dcron {
	dcron := newDcron(serverName)
	for _, opt := range dcronOpts {
		opt(dcron)
	}
	dcron.nodePool = newNodePool(serverName, driver, dcron, dcron.nodeUpdateDuration, dcron.hashReplicas)
	return dcron
}

func newDcron(serverName string) *Dcron {
	return &Dcron{
		ServerName:            serverName,
		logger:                log.New(os.Stdout, "[dcron] ", log.LstdFlags),
		crOptions:             make([]cron.Option, 0),
		nodeUpdateDuration:    defaultNodeDuration,
		jobMetaUpdateDuration: defaultJobMetaDuration,
		hashReplicas:          defaultReplicas,
	}
}

//RegisterJob  register a job
func (d *Dcron) RegisterJob(jobName string, job Job) (err error) {
	return d.registerJob(jobName, nil, job)
}

//RegisterFunc add a cron func
func (d *Dcron) RegisterFunc(jobName string, cmd func()) (err error) {
	return d.registerJob(jobName, cmd, nil)
}

// registerJob register a job
func (d *Dcron) registerJob(jobName string, cmd func(), job Job) (err error) {
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
func (d *Dcron) allowThisNodeRun(jobName string) bool {
	allowRunNode := d.nodePool.PickNodeByJobName(jobName)
	d.info("job '%s' running in node %s", jobName, allowRunNode)
	if allowRunNode == "" {
		d.err("node pool is empty")
		return false
	}
	return d.nodePool.NodeID == allowRunNode
}

//Start start job
func (d *Dcron) Start() {
	d.isRun = true
	err := d.nodePool.StartPool()
	if err != nil {
		d.isRun = false
		d.err("dcron start node pool error %+v", err)
		return
	}
	d.info("dcron started , nodeID is %s", d.nodePool.NodeID)
}

func (d *Dcron) reloadJobMeta(jobMetas []*driver.JobMeta) {
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
func (d *Dcron) Stop() {
	d.isRun = false
	if d.cr != nil {
		d.cr.Stop()
	}
	d.info("dcron stopped")
}

func (d *Dcron) AddJob(serviceName string, jobName string, cron string) error {
	_, err := d.nodePool.Driver.AddJob(serviceName, jobName, cron)
	return err
}

func (d *Dcron) RemoveJob(serviceName string, jobName string) error {
	_, err := d.nodePool.Driver.RemoveJob(serviceName, jobName)
	return err
}

func (d *Dcron) UpdateJob(serviceName string, jobName string, cron string) error {
	_, err := d.nodePool.Driver.UpdateJob(serviceName, jobName, cron)
	return err
}

func (d *Dcron) GetJobList(serviceName string) ([]*driver.JobMeta, error) {
	return d.nodePool.Driver.GetJobList(serviceName)
}
