package mysql

/*
问题：
1、rows.scan如果有一列赋值报错，则后续列不会再进行赋值操作
*/
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-tools/log"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//!统一处理mysql层异常
func DoQueryException(t interface{}) {
	if ri := recover(); ri != nil {
		log.Log.Warn("DoQueryException happened! struct=[%+v],  errorMessage=[%+v]", t, ri)
	}
}

func CloseRows(rows *sql.Rows) {
	defer DoQueryException(rows)
	closeErr := rows.Close()
	if nil != closeErr {
		log.Log.Warn("Close rows fail. err=[%v]", closeErr)
	}
}

/*
 * 开启事务
 */
func (db *DBPool) BeginTrx() (trx *sql.Tx, err error) {
	return db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
}

/*
 * 查询，支持事务查询、同会话查询及简单查询
 *
 * Demo：
 *	事务查询：
 *	trx, err := conn.BeginTrx(rwTimeOut)
 *	if nil != err {
 *		log.Log.Warning("Start trx fail。 err=[%v]", err)
 *	}
 *	res, err = conn.Query(trx, rwTimeOut, nil, "select database() db")
 *	if nil != err {
 *		log.Log.Warning("Query fail %v \n", err)
 *	} else {
 *		Rows := res.Rows
 *		Rows.Next()  // Scan()之前需要做Next()
 *		Rows.Scan(&a5)  // Scan只匹配列个数不匹配列名
 *		fmt.Printf("sql=[select database() db.] values=[%v]\n", a5)
 *		Rows.Close()  // 同一个连接或事务内，在执行下一条语句之前必须清空上一条语句的缓存
 *	}
 *	if err := trx.Rollback(); nil != err {
 *		log.Log.Warning("Rollback fail %v \n", err)
 *	}
 *	同会话查询，：
 *	// 得到一个会话
 *	v_conn, err := conn.GetConn(context.Background())
 *	defer v_conn.Close()
 *	res, err := conn.Query(nil, rwTimeOut, v_conn, "select database();")
 *	conn.Exec(nil, rwCtx, v_conn, "use cortex_server;")
 *	res, err := conn.Query(nil, rwTimeOut, v_conn, "select database();")
 */
func (db *DBPool) DBQuery(trxInvalOpt *sql.Tx, connInvalOpt *sql.Conn, timeout int, sqlText string, params ...interface{}) (res *QueryResult, err error) {
	log.Log.Debug("Execute query type SQL . sql=[%s] timeout=[%v]", fmt.Sprintf(sqlText, params...), timeout)

	var ctx context.Context
	// ctx的close在rows close的时候进行
	if timeout <= 0 {
		ctx = context.Background()
	} else {
		ctx, _ = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	}
	defer DoQueryException(ctx)
	res = &QueryResult{QueryCost: -1.0}
	// 根据是否开启事务，判断调用的方法
	// 此处由于需要在外层对查询结果进行解析，所以不能进行res.Rows.Close()
	if nil != trxInvalOpt {
		res.Rows, res.Error = trxInvalOpt.QueryContext(ctx, sqlText, params...)
		if nil != res.Error {
			log.Log.Warning("Query failed. sql=[%s] err=[%v]", fmt.Sprintf(sqlText, params...), res.Error)
		}
		// 同会话查询
	} else if nil != connInvalOpt {
		res.Rows, res.Error = connInvalOpt.QueryContext(ctx, sqlText, params...)
		if nil != res.Error {
			log.Log.Warning("Query failed. sql=[%s] err=[%v]", fmt.Sprintf(sqlText, params...), res.Error)
		}
		// 简单查询
	} else {
		res.Rows, res.Error = db.QueryContext(ctx, sqlText, params...)
		if nil != res.Error {
			log.Log.Warning("Query failed. sql=[%s] err=[%v]", fmt.Sprintf(sqlText, params...), res.Error)
		}
	}

	return res, res.Error

}

/*
 * 执行ddl或dml语句，SQL字符串可使用Sprintf格式拼接
 */
func (db *DBPool) DBExec(trxInvalOpt *sql.Tx, connInvalOpt *sql.Conn, timeout int, sqlText string, params ...interface{}) (res *QueryResult, err error) {

	log.Log.Debug("Execute dml\\ddl type SQL. sql=[%s] timeout=[%v]", fmt.Sprintf(sqlText, params...), timeout)
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout <= 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	}
	defer cancel()
	res = &QueryResult{QueryCost: -1.0}
	defer DoQueryException(ctx)
	// 根据是否开启事务，判断调用的方法
	// 此处由于需要在外层对查询结果进行解析，所以不能进行res.Rows.Close()
	if nil != trxInvalOpt {
		res.Result, res.Error = trxInvalOpt.ExecContext(ctx, sqlText, params...)
		if nil != res.Error {
			log.Log.Warning("Execute failed. sql=[%s] err=[%v]", fmt.Sprintf(sqlText, params...), res.Error)
		}
		// 同会话查询
	} else if nil != connInvalOpt {
		res.Result, res.Error = connInvalOpt.ExecContext(ctx, sqlText, params...)
		if nil != res.Error {
			log.Log.Warning("Execute failed. sql=[%s] err=[%v]", fmt.Sprintf(sqlText, params...), res.Error)
		}
		// 简单查询
	} else {
		res.Result, res.Error = db.ExecContext(ctx, sqlText, params...)
		if nil != res.Error {
			log.Log.Warning("Execute failed. sql=[%s] err=[%v]", fmt.Sprintf(sqlText, params...), res.Error)
		}
	}

	return res, res.Error

}

/*
 * show status 或 show variables语句执行接口
 */
func (db *DBPool) QueryShow(trxInvalOpt *sql.Tx, connInvalOpt *sql.Conn, showTag string, variableName string) (value string, err error) {

	sqlText := fmt.Sprintf("SHOW %v LIKE '%v'", showTag, variableName)
	// 执行查询
	res, err := db.DBQuery(trxInvalOpt, connInvalOpt, common.Config.RWTimeOutSec, sqlText)
	defer DoQueryException(res.Rows)
	defer CloseRows(res.Rows)
	if nil != err {
		log.Log.Warning("Fail to exec SHOW Query. sql=[%v] reason=[%v]", sqlText, err)
		return "", err
	}

	// 读取结果
	var variable queryShow
	if nil == res.Rows {
		errStr := fmt.Sprintf("No result for query . sql=[%v]", sqlText)
		return value, errors.New(errStr)
	}
	res.Rows.Next()
	err = res.Rows.Scan(&variable.VariableName, &variable.Value)
	if nil != err {
		log.Log.Warning("Fail to exec SHOW Query. sql=[%v] reason=[%v]", sqlText, err)
		return "", err
	}
	return variable.Value, err
}

/*
 * 查看执行Warning信息
 */
func (db *DBPool) showWarning(conn *sql.Conn) (warnings []QueryWarning, err error) {
	res, err := db.DBQuery(nil, conn, common.Config.RWTimeOutSec, "SHOW WARNINGS")
	defer DoQueryException(res.Rows)
	defer CloseRows(res.Rows)
	if nil != err {
		log.Log.Warning("Fail to exec SHOW WARNINGS. reason=[%v]", err)
		return nil, err
	}
	if nil == res.Rows {
		return nil, err
	}
	for res.Rows.Next() {
		var warning QueryWarning
		err := res.Rows.Scan(&warning.Level, &warning.Code, &warning.Message)
		if nil != err {
			log.Log.Warning("Fail to exec SHOW WARNINGS. reason=[%v]", err)
			return nil, err
		}
		warnings = append(warnings, warning)
	}
	return warnings, err
}

/*
 * 查询同一个会话内上一条语句的执行影响及消耗
 */
func (db *DBPool) ShowAffect(conn *sql.Conn) (warning []QueryWarning, queryCost float64, err error) {
	// SHOW WARNINGS
	warning, err = db.showWarning(conn)
	if nil != err {
		log.Log.Warning("Fail to show warning. reason=[%v]", err)
	}
	// SHOW session status LIKE 'last_query_cost';
	queryCostStr, err := db.QueryShow(nil, conn, "SESSION STATUS", "last_query_cost")
	if nil != err {
		log.Log.Warning("Fail to get SQL last_query_cost. reason=[%v]", err)
		return warning, queryCost, err
	}
	queryCost, err = strconv.ParseFloat(queryCostStr, 64)
	if nil != err {
		log.Log.Warning("Fail to trans string to float64. reason=[%v] vars=[%v]", err, queryCostStr)
		return warning, queryCost, err
	}
	return warning, queryCost, err
}

/*
 * show master status 语句执行接口
 */
func (db *DBPool) QueryMasterStatus() (masterStatus QueryMasterStatus, err error) {

	//rows, err := db.Db.Query("SHOW MASTER STATUS")
	res, err := db.DBQuery(nil, nil, common.Config.RWTimeOutSec, "SHOW MASTER STATUS")

	defer DoQueryException(res.Rows)
	defer CloseRows(res.Rows)
	if nil != err {
		log.Log.Warning("Fail to exec SHOW MASTER STATUS. reason=[%v]", err)
		return masterStatus, err
	}
	if nil == res.Rows {
		errStr := fmt.Sprintf("No result for query . sql=[SHOW MASTER STATUS]")
		return masterStatus, errors.New(errStr)
	}
	if !res.Rows.Next() {
		errStr := fmt.Sprintf("No result for query")
		log.Log.Debug("Fail to exec SHOW MASTER STATUS. MySQL may have closed the binlog. reason=[%v] sql=[SHOW MASTER STATUS]", errStr)
		return masterStatus, errors.New(errStr)
	}
	if common.Config.Isfdb == 0 {
		err = res.Rows.Scan(&masterStatus.File,
			&masterStatus.Position,
			&masterStatus.Binlog_Do_DB,
			&masterStatus.Binlog_Ignore_DB,
			&masterStatus.Executed_Gtid_Set,
		)
	} else { //适配FDB
		var xacid string
		err = res.Rows.Scan(&masterStatus.File,
			&masterStatus.Position,
			&masterStatus.Binlog_Do_DB,
			&masterStatus.Binlog_Ignore_DB,
			&masterStatus.Executed_Gtid_Set,
			&xacid, //适配FDB,占位
		)
	}
	if nil != err {
		log.Log.Warning("Fail to exec SHOW MASTER STATUS. reason=[%v]", err)
		return masterStatus, err
	}
	return masterStatus, err
}

/*
 * show slave status 语句执行接口
 */
func (db *DBPool) QuerySlaveStatus() (slaveStatus QuerySlaveStatus, err error) {
	//defer DoQueryException(db.DbConn)
	//rows, err := db.Db.Query("SHOW SLAVE STATUS")

	// 获得一个单独的空闲连接
	res, err := db.DBQuery(nil, nil, common.Config.RWTimeOutSec, "SHOW SLAVE STATUS")

	defer DoQueryException(res.Rows)
	defer CloseRows(res.Rows)
	if nil != err {
		log.Log.Warning("Fail to exec SHOW SLAVE STATUS. reason=[%v]", err)
		return slaveStatus, err
	}
	if !res.Rows.Next() {
		errStr := fmt.Sprintf("No result for query")
		log.Log.Debug("Fail to exec SHOW SLAVE STATUS. This node may a master node. reason=[%v] sql=[SHOW SLAVE STATUS]", errStr)
		return slaveStatus, nil
	}
	if common.Config.Isfdb == 0 {
		err = res.Rows.Scan(&slaveStatus.Slave_IO_State,
			&slaveStatus.Master_Host,
			&slaveStatus.Master_User,
			&slaveStatus.Master_Port,
			&slaveStatus.Connect_Retry,
			&slaveStatus.Master_Log_File,
			&slaveStatus.Read_Master_Log_Pos,
			&slaveStatus.Relay_Log_File,
			&slaveStatus.Relay_Log_Pos,
			&slaveStatus.Relay_Master_Log_File,
			&slaveStatus.Slave_IO_Running,
			&slaveStatus.Slave_SQL_Running,
			&slaveStatus.Replicate_Do_DB,
			&slaveStatus.Replicate_Ignore_DB,
			&slaveStatus.Replicate_Do_Table,
			&slaveStatus.Replicate_Ignore_Table,
			&slaveStatus.Replicate_Wild_Do_Table,
			&slaveStatus.Replicate_Wild_Ignore_Table,
			&slaveStatus.Last_Errno,
			&slaveStatus.Last_Error,
			&slaveStatus.Skip_Counter,
			&slaveStatus.Exec_Master_Log_Pos,
			&slaveStatus.Relay_Log_Space,
			&slaveStatus.Until_Condition,
			&slaveStatus.Until_Log_File,
			&slaveStatus.Until_Log_Pos,
			&slaveStatus.Master_SSL_Allowed,
			&slaveStatus.Master_SSL_CA_File,
			&slaveStatus.Master_SSL_CA_Path,
			&slaveStatus.Master_SSL_Cert,
			&slaveStatus.Master_SSL_Cipher,
			&slaveStatus.Master_SSL_Key,
			&slaveStatus.Seconds_Behind_Master,
			&slaveStatus.Master_SSL_Verify_Server_Cert,
			&slaveStatus.Last_IO_Errno,
			&slaveStatus.Last_IO_Error,
			&slaveStatus.Last_SQL_Errno,
			&slaveStatus.Last_SQL_Error,
			&slaveStatus.Replicate_Ignore_Server_Ids,
			&slaveStatus.Master_Server_Id,
			&slaveStatus.Master_UUID,
			&slaveStatus.Master_Info_File,
			&slaveStatus.SQL_Delay,
			&slaveStatus.SQL_Remaining_Delay,
			&slaveStatus.Slave_SQL_Running_State,
			&slaveStatus.Master_Retry_Count,
			&slaveStatus.Master_Bind,
			&slaveStatus.Last_IO_Error_Timestamp,
			&slaveStatus.Last_SQL_Error_Timestamp,
			&slaveStatus.Master_SSL_Crl,
			&slaveStatus.Master_SSL_Crlpath,
			&slaveStatus.Retrieved_Gtid_Set,
			&slaveStatus.Executed_Gtid_Set,
			&slaveStatus.Auto_Position,
			&slaveStatus.Replicate_Rewrite_DB,
			&slaveStatus.Channel_Name,
			&slaveStatus.Master_TLS_Version,
		)
	} else { //适配FDB
		var semiSyncGroup string
		var iOCachedGtidSet string
		err = res.Rows.Scan(&slaveStatus.Slave_IO_State,
			&slaveStatus.Master_Host,
			&slaveStatus.Master_User,
			&slaveStatus.Master_Port,
			&slaveStatus.Connect_Retry,
			&slaveStatus.Master_Log_File,
			&slaveStatus.Read_Master_Log_Pos,
			&slaveStatus.Relay_Log_File,
			&slaveStatus.Relay_Log_Pos,
			&slaveStatus.Relay_Master_Log_File,
			&slaveStatus.Slave_IO_Running,
			&slaveStatus.Slave_SQL_Running,
			&slaveStatus.Replicate_Do_DB,
			&slaveStatus.Replicate_Ignore_DB,
			&slaveStatus.Replicate_Do_Table,
			&slaveStatus.Replicate_Ignore_Table,
			&slaveStatus.Replicate_Wild_Do_Table,
			&slaveStatus.Replicate_Wild_Ignore_Table,
			&slaveStatus.Last_Errno,
			&slaveStatus.Last_Error,
			&slaveStatus.Skip_Counter,
			&slaveStatus.Exec_Master_Log_Pos,
			&slaveStatus.Relay_Log_Space,
			&slaveStatus.Until_Condition,
			&slaveStatus.Until_Log_File,
			&slaveStatus.Until_Log_Pos,
			&slaveStatus.Master_SSL_Allowed,
			&slaveStatus.Master_SSL_CA_File,
			&slaveStatus.Master_SSL_CA_Path,
			&slaveStatus.Master_SSL_Cert,
			&slaveStatus.Master_SSL_Cipher,
			&slaveStatus.Master_SSL_Key,
			&slaveStatus.Seconds_Behind_Master,
			&slaveStatus.Master_SSL_Verify_Server_Cert,
			&slaveStatus.Last_IO_Errno,
			&slaveStatus.Last_IO_Error,
			&slaveStatus.Last_SQL_Errno,
			&slaveStatus.Last_SQL_Error,
			&slaveStatus.Replicate_Ignore_Server_Ids,
			&slaveStatus.Master_Server_Id,
			&slaveStatus.Master_UUID,
			&slaveStatus.Master_Info_File,
			&slaveStatus.SQL_Delay,
			&slaveStatus.SQL_Remaining_Delay,
			&slaveStatus.Slave_SQL_Running_State,
			&semiSyncGroup, //适配FDB,占位
			&slaveStatus.Master_Retry_Count,
			&slaveStatus.Master_Bind,
			&slaveStatus.Last_IO_Error_Timestamp,
			&slaveStatus.Last_SQL_Error_Timestamp,
			&slaveStatus.Master_SSL_Crl,
			&slaveStatus.Master_SSL_Crlpath,
			&iOCachedGtidSet, //适配FDB,占位
			&slaveStatus.Retrieved_Gtid_Set,
			&slaveStatus.Executed_Gtid_Set,
			&slaveStatus.Auto_Position,
			//&slaveStatus.Replicate_Rewrite_DB,
			//&slaveStatus.Channel_Name,
			//&slaveStatus.Master_TLS_Version,
		)
	}
	log.Log.Debug("slaveStatus=[%v]", slaveStatus)
	if nil != err {
		// 列赋值失败的问题不进行报错处理
		if !strings.Contains(err.Error(), ROW_PART_COLUMN_SCAN_ERROR) {
			log.Log.Warning("Fail to exec SHOW SLAVE STATUS. reason=[%v]", err)
			return slaveStatus, err
		}
	}
	// 由于健康的从库状态中SQL_Remaining_Delay的值为nil导致rows.scan赋值到该列后终止，Executed_Gtid_Set的值被丢弃
	// Executed_Gtid_Set暂时通过show master status来获得
	masterStatus, err := db.QueryMasterStatus()
	if nil != err {
		log.Log.Warning("Fail to exec get Executed_Gtid_Set Info. reason=[%v]", err)
		return slaveStatus, err
	}
	slaveStatus.Executed_Gtid_Set = masterStatus.Executed_Gtid_Set
	return slaveStatus, nil
}
