package dao

import (
	"errors"
	"fmt"
	"go-tools/log"
)

//!统一处理DAO层异常
func DoDaoException(t interface{}) {
	if ri := recover(); ri != nil {
		log.Log.Warn("doDaoException happened! struct=[%+v],  errorMessage=[%+v]", t, ri)
	}
}

//统一处理数据库操作依赖的条件字段为空的错误返回
func errBlankContent(t interface{}, col string, dbTable string) error {
	msg := fmt.Sprintf("Error! col=[%v] is blank, record=[%+v], table=[%v]", col, t, dbTable)
	log.Log.Warn(msg)
	return errors.New(msg)
}
