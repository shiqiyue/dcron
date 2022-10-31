package dcron

import "log"

//SetLogger set dcron logger
func (d *Dcron) SetLogger(logger *log.Logger) {
	d.logger = logger
}

//GetLogger get dcron logger
func (d *Dcron) GetLogger() interface{ Printf(string, ...interface{}) } {
	return d.logger
}

func (d *Dcron) info(format string, v ...interface{}) {
	d.logger.Printf("INFO: "+format, v...)
}
func (d *Dcron) err(format string, v ...interface{}) {
	d.logger.Printf("ERR: "+format, v...)
}
