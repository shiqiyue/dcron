package dcron

import "log"

//SetLogger set client logger
func (d *Client) SetLogger(logger *log.Logger) {
	d.logger = logger
}

//GetLogger get client logger
func (d *Client) GetLogger() interface{ Printf(string, ...interface{}) } {
	return d.logger
}

func (d *Client) info(format string, v ...interface{}) {
	d.logger.Printf("INFO: "+format, v...)
}
func (d *Client) err(format string, v ...interface{}) {
	d.logger.Printf("ERR: "+format, v...)
}
