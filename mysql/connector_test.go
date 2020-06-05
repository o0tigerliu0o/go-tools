package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"mysql-agent/common"
	"sync"
	"testing"
	"time"
)

type Connector struct {
	Addr              string
	Port              int
	User              string
	Passwd            string
	Charset           string
	Database          string
	connectTimeoutSec string     //连接超时时间，单位秒
	RwTimeoutSec      int        //读写超时时间，单位秒
	maxOpenConns      int        //连接池最大连接数,暂时没用，生效的是sql.DB.maxOpen
	maxIdleConns      int        //连接池最大空闲连接数,暂时没用，生效的是sql.DB.maxIdle
	DbConnLock        sync.Mutex // DbConn使用锁
	DbConn            *sql.Conn
	Db                *sql.DB
	Closed            bool
}

var monitorDBPool DBPool
var param Connector

func init() {

	param := Connector{
		Addr:              "127.0.0.1",
		Port:              3366,
		User:              "passtest",
		Passwd:            "_Y5%C2wncJC6b^frHdiEKw*kn05VNN",
		Charset:           "utf8mb4",
		Database:          "cortex_server",
		connectTimeoutSec: "2s",
		RwTimeoutSec:      2,
		maxOpenConns:      10,
		maxIdleConns:      5,
	}

	var err error
	dataSource := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?timeout=%v&charset=%v,parseTime=%v",
		param.User, param.Passwd, param.Addr, param.Port, param.Database, param.connectTimeoutSec,
		param.Charset, "true")
	monitorDBPool.DB, err = sql.Open("mysql", dataSource)
	if nil != err {
		common.Log.Warning("open fail %v \n", err)
	}

	monitorDBPool.SetMaxOpenConns(param.maxOpenConns)
	monitorDBPool.SetMaxOpenConns(5)
	monitorDBPool.SetMaxIdleConns(2)

}

func TestQuery(t *testing.T) {

	/*
		/*
		 *  同会话执行多条SQL1
	*/
	func() {

		conn, err := monitorDBPool.Conn(context.Background())
		if nil != err {
			common.Log.Warning("open fail %v , conn=[%v]\n", err, conn)
		}
		defer conn.Close()
		if nil != err {
			common.Log.Warning("Get conn fail %v \n", err)
		}
		var a5 string

		sql := "select database();"

		res, err := monitorDBPool.DBQuery(nil, conn, param.RwTimeoutSec, sql)
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
			Rows.Close()
		}

		fmt.Printf("before use db values=[%v]\n", a5)
		fmt.Println("===============conn:", conn)
		_, err = monitorDBPool.DBExec(nil, conn, param.RwTimeoutSec, "use mysql;")
		if nil != err {
			common.Log.Warning("Exec fail %v \n", err)
		}
		fmt.Println("===============conn:", conn)
		res, err = monitorDBPool.DBQuery(nil, conn, param.RwTimeoutSec, sql)
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
			Rows.Close()
		}

		fmt.Printf("after use db values=[%v]\n", a5)
		res1 := &QueryResult{QueryCost: -1.0}
		res1.Warning, res1.QueryCost, err = monitorDBPool.ShowAffect(conn)

		if nil != err {
			common.Log.Warning("ShowAffect fail %v \n", err)
		} else {
			fmt.Println("res.Warning=", res.QueryCost)
			fmt.Println("res.QueryCost=", res.QueryCost)

		}

		var a2 string
		querySql := "show variables like 'sql_log_bin';"

		res, err = monitorDBPool.DBQuery(nil, conn, param.RwTimeoutSec, querySql)
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
			Rows.Close()
		}

		fmt.Printf("before set sql_log_bin values=[%v]\n", a2)
		monitorDBPool.DBExec(nil, conn, param.RwTimeoutSec, "set sql_log_bin = 0;")

		res, err = monitorDBPool.DBQuery(nil, conn, param.RwTimeoutSec, querySql)
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			func() {
				if err := res.Rows.Close(); nil != err {
					common.Log.Warning("err=[%v]", err)
				}
			}()
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
		}
	}()
	getInUse(monitorDBPool.DB)
	fmt.Println("==========================")

	/*
	 *  单条查询
	 */
	func() {

		var a5 string
		res, err := monitorDBPool.DBQuery(nil, nil, param.RwTimeoutSec, "select id, cluster_id, instance_id, ip, executed_gtid_set  from cortex_server.db_instances")
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
			Rows.Close()
		}

		getInUse(monitorDBPool.DB)

		fmt.Println("err=", err)
		type dbInstances struct {
			id                string
			cluster_id        string
			instance_id       string
			ip                string
			executed_gtid_set string
		}
		var ins dbInstances
		for res.Rows.Next() {
			res.Rows.Scan(&ins.id, &ins.cluster_id, &ins.instance_id, &ins.ip, &ins.executed_gtid_set)
			fmt.Println("ins.id=", ins.id)
			fmt.Println("ins.cluster_id=", ins.cluster_id)
			fmt.Println("ins.instance_id=", ins.instance_id)
			fmt.Println("ins.ip=", ins.ip)
			fmt.Println("ins.executed_gtid_set=", ins.executed_gtid_set)
			res.Rows.Close()
		}

		getInUse(monitorDBPool.DB)
		/*
		 *  单条ddl或dml
		 */
		res, err = monitorDBPool.DBExec(nil, nil, param.RwTimeoutSec, "set global read_only = ON;")
		affect, _ := res.Result.RowsAffected()
		fmt.Println("err=", err)
		fmt.Println("res.affect=", affect)
		getInUse(monitorDBPool.DB)
		/*
		 *  show master
		 */
		masterStatus, err := monitorDBPool.QueryMasterStatus()
		fmt.Println("err=", err)
		fmt.Println("masterStatus.File=", masterStatus.File)
		fmt.Println("masterStatus.Position=", masterStatus.Position)
		getInUse(monitorDBPool.DB)
		/*
		 *  show slave
		 */
		slaveStatus, err := monitorDBPool.QuerySlaveStatus()
		fmt.Println("err=", err)
		fmt.Println("slaveStatus=", slaveStatus)
		fmt.Println("slaveStatus.Slave_IO_Running=", slaveStatus.Slave_IO_Running)
		fmt.Println("slaveStatus.Slave_SQL_Running=", slaveStatus.Slave_SQL_Running)
		getInUse(monitorDBPool.DB)
		/*
		 *  因为非事务内的多条查询无法保证在同一个连接内
		 *  所以如果想要得到查询的影响，必须开启事务来保证SQL及show warnings语句在同一个连接内
		 */

		// 事务使用场景：1、需要使用事务  2、需要多条查询在同一个连接内执行

		res, err = monitorDBPool.DBQuery(nil, nil, param.RwTimeoutSec, "select database()")
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
			fmt.Printf("sql=[select database()] values=[%v]\n", a5)
			Rows.Close()
		} // 同一个事务，在执行下一条语句之前必须清空上一条语句的缓存
		getInUse(monitorDBPool.DB)
		fmt.Println("================trx start============================")
		trx, err := monitorDBPool.BeginTrx()
		if nil != err {
			common.Log.Warning("Start trx fail。 err=[%v]", err)
		}
		getInUse(monitorDBPool.DB)
		monitorDBPool.DBExec(trx, nil, param.RwTimeoutSec, "use mysql")
		getInUse(monitorDBPool.DB)
		res, err = monitorDBPool.DBQuery(trx, nil, param.RwTimeoutSec, "select database() db")
		if nil != err {
			common.Log.Warning("Query fail %v \n", err)
		} else {
			Rows := res.Rows
			Rows.Next()
			Rows.Scan(&a5)
			fmt.Printf("sql=[select database() db.] values=[%v]\n", a5)
			Rows.Close()
		}
		getInUse(monitorDBPool.DB)
		if err := trx.Rollback(); nil != err {
			common.Log.Warning("Rollback fail %v \n", err)
		}
		fmt.Println("================trx end============================")
	}()
	getInUse(monitorDBPool.DB)
	fmt.Println(monitorDBPool.DB)

	/*
	 * 超时测试
	 */
	// 连接池查询超时测试
	res, err := monitorDBPool.DBQuery(nil, nil, 2, "select sleep(1)")
	if nil != err {
		log.Printf("exec new query after close fail. err=[%v] \n", err)
	} else {

		for res.Rows.Next() {
			var now string
			res.Rows.Scan(&now)
			fmt.Println("now=", now)

		}
		res.Rows.Close()
	}

	// 连接池执行超时测试
	fmt.Println(time.Now())
	res, err = monitorDBPool.DBExec(nil, nil, 2, "select sleep(1)")
	if nil != err {
		fmt.Println(time.Now())
		log.Printf("exec new query after close fail. err=[%v] \n", err)
	}
	log.Println(res)

	getInUse(monitorDBPool.DB)
	// 单链接查询超时测试
	conn, err := monitorDBPool.Conn(context.Background())
	if nil != err {
		common.Log.Warning("open fail %v , conn=[%v]\n", err, conn)
	}

	res, err = monitorDBPool.DBQuery(nil, conn, 3, "select sleep(2)")
	if nil != err {
		log.Printf("exec new query after close fail. err=[%v] \n", err)
	} else {
		defer res.Rows.Close()
		for res.Rows.Next() {
			var now string
			err := res.Rows.Scan(&now)
			if nil != err {
				fmt.Println(time.Now())
				log.Printf("res.Rows.Scan fail. err=[%v] \n", err)
			}
			fmt.Println("now=", now)

		}
	}

	conn.Close()

	getInUse(monitorDBPool.DB)
	conn, err = monitorDBPool.Conn(context.Background())
	if nil != err {
		common.Log.Warning("open fail %v , conn=[%v]\n", err, conn)
	}

	// 单链接执行超时测试
	_, err = monitorDBPool.DBExec(nil, conn, 2, "select sleep(1)")
	if nil != err {
		log.Printf("exec new query after close fail. err=[%v] \n", err)
	}
	conn.Close()

	conn, err = monitorDBPool.Conn(context.Background())
	if nil != err {
		common.Log.Warning("open fail %v , conn=[%v]\n", err, conn)
	}
	conn.Close()

	/*
	 * mysql 重启
	 */

	monitorDBPool.DBExec(nil, conn, 60, "select sleep(50)")
	// shutdown+restart
	for i := 0; i < 50; i++ {
		time.Sleep(2 * time.Second)
		fmt.Println(monitorDBPool.DB)
		go func() {
			conn, err := monitorDBPool.DB.Conn(context.Background())
			if nil != err {
				log.Printf("get conn %v \n", err)
				return
			}
			defer conn.Close()

			/*rwCtx, rwCancel := context.WithTimeout(context.Background(), time.Duration(connector.RwTimeoutSec)*time.Second)
			defer rwCancel()*/
			_, err = monitorDBPool.DBExec(nil, conn, 3, "use dxm_dba")
			if nil != err {
				log.Printf("use db %v \n", err)
			}

			res, err := monitorDBPool.DBQuery(nil, nil, 3, "select database() db")
			fmt.Println(conn)
			if nil != err {
				log.Printf("select database() %v \n", err)
			} else {
				for res.Rows.Next() {
					var db string
					res.Rows.Scan(&db)
					fmt.Println()
					fmt.Println("db=", db)
					fmt.Println()
					res.Rows.Close()
				}
			}
		}()
	}

	monitorDBPool.Close()
	time.Sleep(1 * time.Second)
	getInUse(monitorDBPool.DB)

}

func getInUse(db *sql.DB) {
	stats := db.Stats()
	common.Log.Notice("in use conns:%v", stats.InUse)

}
