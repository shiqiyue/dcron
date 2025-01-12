package gorm

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

func (g *GormDriver) updateNodeExpiredAt(nodeId, serviceName string, expiredAt time.Time) error {
	err := g.DB.Transaction(func(tx *gorm.DB) error {
		err := NewJobNodeQuerySet(tx).NodeIdEq(nodeId).Delete()
		if err != nil {
			return err
		}
		node := &JobNode{
			ServiceName: serviceName,
			NodeId:      nodeId,
			ExpiredAt:   expiredAt.Unix(),
		}
		err = node.Create(tx)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (g *GormDriver) RegisterServiceNode(ServiceName string) (string, error) {
	nodeID := uuid.New().String()
	expiredAt := time.Now().Add(g.timeout)
	node := &JobNode{
		ServiceName: ServiceName,
		NodeId:      nodeID,
		ExpiredAt:   expiredAt.Unix(),
	}
	err := node.Create(g.DB)
	if err != nil {
		return "", err
	}
	return nodeID, nil
}

func (g *GormDriver) GetServiceNodeList(ServiceName string) ([]string, error) {
	now := time.Now()
	jobNodes := make([]*JobNode, 0)
	err := NewJobNodeQuerySet(g.DB).ServiceNameEq(ServiceName).ExpiredAtGte(now.Unix()).All(&jobNodes)
	if err != nil {
		return nil, err
	}
	rs := make([]string, 0)
	if len(jobNodes) > 0 {
		for _, jobNode := range jobNodes {
			rs = append(rs, jobNode.NodeId)
		}
	}
	return rs, nil
}
