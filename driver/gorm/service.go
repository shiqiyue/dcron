package gorm

func (g *GormDriver) AddService(serviceName string) (string, error) {
	serviceCount, err := NewJobServiceQuerySet(g.DB).ServiceNameEq(serviceName).Count()
	if err != nil {
		return "", err
	}
	if serviceCount == 0 {
		jobService := &JobService{
			ServiceName: serviceName,
		}
		err := jobService.Create(g.DB)
		if err != nil {
			return "", err
		}
	}
	return serviceName, nil
}

func (g *GormDriver) RemoveService(serviceName string) (string, error) {
	err := NewJobServiceQuerySet(g.DB).ServiceNameEq(serviceName).Delete()
	if err != nil {
		return "", err
	}
	return serviceName, nil
}

func (g *GormDriver) GetServiceList() ([]string, error) {
	jobServices := make([]*JobService, 0)
	err := NewJobServiceQuerySet(g.DB).All(&jobServices)
	if err != nil {
		return nil, err
	}
	rs := make([]string, 0)
	for _, jobService := range jobServices {
		rs = append(rs, jobService.ServiceName)
	}
	return rs, nil
}
