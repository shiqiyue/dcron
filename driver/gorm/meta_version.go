package gorm

import (
	"gorm.io/gorm"
)

func (d *GormDriver) getMetaVersion() (int64, error) {
	r := &MetaVersion{}
	err := NewMetaVersionQuerySet(d.DB).OrderDescByID().Limit(1).One(r)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, nil
		}
		return 0, err
	}
	return r.C, nil

}

func (d *GormDriver) incrMetaVersion(db *gorm.DB) error {
	updateNum, err := NewMetaVersionQuerySet(db).OrderDescByID().Limit(1).GetUpdater().IncC(1).UpdateNum()
	if err != nil {
		return err
	}
	if updateNum == 0 {
		metaVersion := &MetaVersion{
			C: 1,
		}
		err := metaVersion.Create(db)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}
