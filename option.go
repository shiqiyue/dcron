package dcron

import (
	"github.com/robfig/cron/v3"
	"time"
)

// Option is Client Option
type Option func(*Client)

// WithLogger both set client and cron logger.
func WithLogger(logger interface{ Printf(string, ...interface{}) }) Option {
	return func(dcron *Client) {
		//set client logger
		dcron.logger = logger
		//set cron logger
		f := cron.WithLogger(cron.PrintfLogger(logger))
		dcron.crOptions = append(dcron.crOptions, f)
	}
}

// WithNodeUpdateDuration set node update duration
func WithNodeUpdateDuration(d time.Duration) Option {
	return func(dcron *Client) {
		dcron.nodeUpdateDuration = d
	}
}

// WithJobMetaUpdateDuration set job meta update duration
func WithJobMetaUpdateDuration(d time.Duration) Option {
	return func(dcron *Client) {
		dcron.jobMetaUpdateDuration = d
	}
}

// WithHashReplicas set hashReplicas
func WithHashReplicas(d int) Option {
	return func(dcron *Client) {
		dcron.hashReplicas = d
	}
}

//CronOptionLocation is warp cron with location
func CronOptionLocation(loc *time.Location) Option {
	return func(dcron *Client) {
		f := cron.WithLocation(loc)
		dcron.crOptions = append(dcron.crOptions, f)
	}
}

//CronOptionSeconds is warp cron with seconds
func CronOptionSeconds() Option {
	return func(dcron *Client) {
		f := cron.WithSeconds()
		dcron.crOptions = append(dcron.crOptions, f)
	}
}

// CronOptionParser is warp cron with schedules.
func CronOptionParser(p cron.ScheduleParser) Option {
	return func(dcron *Client) {
		f := cron.WithParser(p)
		dcron.crOptions = append(dcron.crOptions, f)
	}
}

// CronOptionChain is Warp cron with chain
func CronOptionChain(wrappers ...cron.JobWrapper) Option {
	return func(dcron *Client) {
		f := cron.WithChain(wrappers...)
		dcron.crOptions = append(dcron.crOptions, f)
	}
}
