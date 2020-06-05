package dao

import (
	"errors"
	"fmt"
	"go-tools/log"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
)

const (
	MAX_UNIQUE_ID  = 999999999 // 唯一键最大值
	WARM_UNIQUE_ID = 10000     // 唯一键预警值
)

func InitDao() error {
	//注册数据库驱动
	orm.RegisterDriver(log.Config.RegisterDatabase, orm.DRMySQL)

	//注册数据库
	err := registerDataBase()
	if err != nil {
		panic(err)
	}

	//需要在init中注册定义的model

	orm.RegisterModel(new(DbInstance))
	//开发阶段，开始orm的debug模式，打印SQL日志
	//适用config.go中的配置开关
	orm.Debug = log.Config.OrmDebugSwitch

	return err
}

// 初始化数据库连接配置
func registerDataBase() error {
	var databaseurl string
	if log.Config.OnlineDSN.Disable == false {
		databaseurl = fmt.Sprintf("%v:%v@tcp(%v)/%v?charset=%v&loc=Local",
			log.Config.OnlineDSN.User, log.Config.OnlineDSN.Password, log.Config.OnlineDSN.Addr,
			log.Config.OnlineDSN.Schema, log.Config.OnlineDSN.Charset)
		orm.RegisterDataBase("default", log.Config.RegisterDatabase,
			databaseurl, log.Config.MaxIdleConns, log.Config.MaxOpenConns)
		return nil
	} else if log.Config.TestDSN.Disable == false {
		databaseurl = fmt.Sprintf("%v:%v@tcp(%v)/%v?charset=%v&loc=Local",
			log.Config.TestDSN.User, log.Config.TestDSN.Password, log.Config.TestDSN.Addr,
			log.Config.TestDSN.Schema, log.Config.TestDSN.Charset)
		orm.RegisterDataBase("default", log.Config.RegisterDatabase,
			databaseurl, log.Config.MaxIdleConns, log.Config.MaxOpenConns)
		return nil
	}
	return errors.New("online-dsn and test-dsn are all nil in tinker.yaml!")
}
