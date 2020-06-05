package dao

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/astaxie/beego/orm"
	"go-tools/log"
	"reflect"
)

//数据库查询条件
type WhereConds struct {
	Column string
	Value  []interface{}
	Expr   MysqlExpr
}

//数据库字段值变更
type ColumnSet struct {
	Column string
	Value  []interface{}
}

type MysqlExpr int

const (
	// 等于
	Expr_Equal MysqlExpr = iota
	// 不等于
	Expr_Not_Equal MysqlExpr = iota
	//大于
	Expr_Greater_Than
	//小于
	Expr_Less_Than
	// in字句
	Expr_In
)


//!@brief:根据fieldName获取在ptrTableStruct中的值(interface{})
// @param: ptrTableStruct ：结构体指针
// @param: fieldName struct字段名称
// @success: val = f
// @fail: val is nil
// 函数暂不会被外部调用，设为内部函数
// golang的reflect使用注意：
// 1、第三反射定律：To modify a reflection object, the value must be settable。为了修改一个反射对象，值必须是settable的。由于golang函数使用值传递，所以临时变量不能作为反射对象的参数。
// 2、struct的private变量也是non-settable的
//
func getFieldVal(ptrTableStruct interface{}, fieldName string) (val interface{}) {
	var value reflect.Value
	defer func() {
		if ri := recover(); ri != nil {
			log.Log.Warn("Exception,fieldName=(%v) value=(%v) recover=(%+v)", fieldName, value, ri)

		}
	}()

	s := reflect.ValueOf(ptrTableStruct).Elem()
	value = s.FieldByName(fieldName)
	f := s.FieldByName(fieldName).Interface()

	return f
}

/*
*   Read -
*
*   DESCRIPTION - select records by condition,根据cols中条件获取数据
*
*   PARAMS:
*       ptrTableStruct: pointer, the table
*       cols:  the name of condition colums. The name is member of struct .
*       ptrOrmer: pointer, when the ptrOrmer is not nil ,it's used to control transaction
*
*   RETURNS:
*       return ptrTableStruct
*              ptrTableStruct is nil when cols is not matched
*
*   Examples:
*        var tableInfo := new(TableInfoStruct)
*        tableInfo.Id = 1
*        ptrM := &tableInfo
*        o := orm.NewOrm()
*        cols := []string{"Id"}
*        err := Read(ptrM, cols, o)
 */
func Read(ptrTableStruct interface{}, cols []string, ptrOrmer orm.Ormer) error {
	defer DoDaoException(ptrTableStruct)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	//传入的cols不能为空
	if len(cols) == 0 {
		msg := fmt.Sprintf("read record=[%+v] failed, len(cols)=[0]", ptrTableStruct)
		log.Log.Warn(msg)
		return errors.New(msg)
	}

	err := ptrOrmer.Read(ptrTableStruct, cols...)

	if err != nil {
		log.Log.Warn("Read record failed! cols=[%+v], record=[%+v], error=[%+v]", cols, ptrTableStruct, err)
		return err
	}

	log.Log.Debug("Read record=[%+v] successfully.", ptrTableStruct)
	return err
}

/*
*   Insert -
*
*   DESCRIPTION - insert a new record
*
*   PARAMS:
*       ptrM: a pointer to the tableStruct
*       ptrOrmer: pointer, when the ptrOrmer is not nil ,it's used to control transaction
*
*   RETURNS:
*       return insertId int64, err
*              if err != nil, insert failed
*
*   Examples:
*        var tableInfo := new(TableInfoStruct)
*        tableInfo.A = 1
*        tableInfo.B = 1
*        ptrM := &tableInfo
*        o := orm.NewOrm()
*        id, err := Insert(ptrM, o) //!transaction
*        id, err :Insert(ptrM, nil)
 */
func Insert(ptrM interface{}, ptrOrmer orm.Ormer) (newId int64, err error) {
	defer DoDaoException(ptrM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	id, err := ptrOrmer.Insert(ptrM)
	if err != nil {
		log.Log.Warn("Insert record=[%+v] failed, error=[%v]", ptrM, err)
		return 0, err
	}

	log.Log.Debug("Insert record=[%+v] successfully, newId=[%v].", ptrM, id)
	return id, err
}

/*
*   update - update records by condition
*
*   DESCRIPTION -  update records by condition
*
*   PARAMS:
*       ptrM:  the objec(pointer) which saves new values
*       Cols: the condition colums(table fields)
*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
*   RETURNS:
*       return updatedCount, err
*               err =nil ; updatedCount means the count to updated data
*
*   Examples:
*        tableInfo := new(TableInfoStruct)
*        tableInfo.Id = 1
*        tableInfo.A = "willbeupdated"
*        ptrM := &tableInfo
*        o := orm.NewOrm()
*        Cols := []string{"A"}
*        r,err := Update(ptrM, Cols, o)
*
 */
func Update(ptrM interface{}, cols []string, ptrOrmer orm.Ormer) (updatedCount int64, err error) {
	defer DoDaoException(ptrM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}
	//传入的cols不能为空
	if len(cols) == 0 {
		msg := fmt.Sprintf("Update record=[%+v] failed, len(cols)=[0]", ptrM)
		log.Log.Warn(msg)
		return 0, errors.New(msg)
	}

	nums, err := ptrOrmer.Update(ptrM, cols...)

	if err != nil {
		log.Log.Warn("Update record=[%+v] failed, error=[%v]", ptrM, err)
		return 0, err
	}

	log.Log.Debug("Update record=[%+v] successfully,  affected rows=[%v] .", ptrM, nums)
	return nums, err
}

/*  根据条件批量更新
*   UpdateByCond - Update records by condition
*
*   DESCRIPTION -  Update records by condition
*
*   PARAMS:
*       ptrM: the pointer of  object which saves the conditions
*       condCols: the condition colums(table fields)
*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
*   RETURNS:
*       return delCnt64 int64, err error
*               err =nil ; delCnt64 means the count to delete data
*
*   Examples:
*        o := orm.NewOrm()
*        condObj := new(TableInfoStruct)
*        condObj.Id = 1

*        tableInfo := new(TableInfoStruct)
*        tableInfo.A = "willbeupdated"
*        ptrM := &tableInfo

*        condCols := []string{"Id"}
*        newCols := []string{"A"}

*        r := omysql.UpdateByCond(&objCond, condCols, ptrM, newCols,  o)
*
 */
func UpdateByCond(ptrM interface{}, condCols []string, ptrMNew interface{}, newCols []string,
	ptrOrmer orm.Ormer) (updatedCount int64, err error) {

	defer DoDaoException(ptrM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	//condCols和newCols都不能为空
	if len(condCols) == 0 || len(newCols) == 0 {
		msg := fmt.Sprintf("condCols is nil or newCols is nil. Update record=[%+v] failed, "+
			"len(condCols)=[%v], len(newCols)=[%v]", ptrM, len(condCols), len(newCols))
		log.Log.Warn(msg)
		return 0, errors.New(msg)
	}

	qs := ptrOrmer.QueryTable(ptrM)
	//循环query，查询需要更新的条目
	for _, colName := range condCols {
		value := getFieldVal(ptrM, colName)
		if value == nil {
			msg := fmt.Sprintf("GetFieldVal fail. ptrM=[%+v], condCols=[%+v]", ptrM, condCols)
			log.Log.Warn(msg)
			return 0, errors.New(msg)
		}
		//按照字段值筛选
		qs = qs.Filter(colName, value)
	}
	//填充需更新的字段名
	var params orm.Params
	params = make(orm.Params)

	for _, one := range newCols {
		val := getFieldVal(ptrMNew, one)
		if val == nil {
			msg := fmt.Sprintf("GetFieldVal from object=[%+v] by fieldName=[%+v]", ptrMNew, one)
			log.Log.Warn(msg)

			return updatedCount, errors.New(msg)
		}
		params[one] = val
	}
	updatedCount, err = qs.Update(params)
	//判断更新是否成功
	if err != nil {
		log.Log.Warn("fail to update to object=[%+v] by fieldNames=[%+v]! error=[%v]",
			ptrMNew, newCols, err)
		return 0, err
	}

	log.Log.Debug("Update object=[%+v] by fieldNames=[%+v] successfully,  affected rows=[%v] .",
		ptrM, newCols, updatedCount)
	return updatedCount, err
}

/*
*   DeleteByCondCols - delete records by condition
*
*   DESCRIPTION -  delete records by condition
*
*   PARAMS:
*       ptrM: the pointer of  object which saves the conditions
*       condCols: the condition colums(table fields)
*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
*   RETURNS:
*       return delCnt64 int64, err error
*               err =nil ; delCnt64 means the count to delete data
*
*   Examples:
*        tableInfo := new(TableInfoStruct)
*        tableInfo.Id = 1
*        tableInfo.A = "willbeupdated"
*        ptrM := &tableInfo
*        o := orm.NewOrm()
*        condCols := []string{"Id","A"}
*        cnt64, r := DeleteByCondCols(ptrM, condCols, o)
*
 */
func DeleteByCondCols(ptrM interface{}, condCols []string,
	ptrOrmer orm.Ormer) (delCnt64 int64, err error) {

	defer DoDaoException(ptrM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}
	//判断删除条件是否为空，为空不允许删除delete *
	if len(condCols) == 0 {
		msg := fmt.Sprintf("Forbid to delete *! object=[%+v], condCols=[%v]", ptrM, condCols)
		log.Log.Warn(msg)
		return 0, errors.New(msg)
	}
	qs := ptrOrmer.QueryTable(ptrM)
	//循环query，查询需要删除的条目
	for _, colName := range condCols {
		value := getFieldVal(ptrM, colName)
		if value == nil {
			msg := fmt.Sprintf("GetFieldVal fail. ptrM=[%+v], condCols=[%+v]", ptrM, condCols)
			log.Log.Warn(msg)
			return 0, errors.New(msg)
		}
		//按照字段值筛选
		qs = qs.Filter(colName, value)
	}
	//删除过滤出的条目
	delCnt64, err = qs.Delete()
	if err != nil {
		log.Log.Warn("Fail to delete record=[%+v] by condCols=[%+v]! error=[%v]",
			ptrM, condCols, err)
		return 0, err
	}

	log.Log.Debug("Delete record=[%+v] by condCols=[%+v] successfully! affected rows=[%v] !",
		ptrM, condCols, delCnt64)
	return delCnt64, err
}

///*
//*   readAllRecords - read records by condition
//*
//*   DESCRIPTION -  get all records
//*
//*   PARAMS:
//*       ptrM: the pointer of  object which saves the conditions
//*       ptrList: []TableInfoStruct
//*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
//*   RETURNS:
//*       return  err error
//*               err =nil ; ptrList is result
//*
//*   Examples:
//*        tableInfo := new(TableInfoStruct)
//*        ptrM := &tableInfo
//*		   o := orm.NewOrm()
//*        var ptrList []TableInfoStruct
//*        err := readAllRecords(ptrM, ptrList, o)
//*
// */
func ReadAllRecords(prtM interface{}, ptrList interface{}, ptrOrmer orm.Ormer) (err error) {
	defer DoDaoException(prtM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	count, err := ptrOrmer.QueryTable(prtM).Limit(-1).All(ptrList)
	if err != nil {
		log.Log.Warn("fail to get all records from object=[%v]!", prtM)
		return err
	}
	log.Log.Debug("Get all records from object=[%+v] successfully, count=[%+v].",
		prtM, count)
	return nil
}

///*
//*   readRecordsByCols - read records by condition
//*
//*   DESCRIPTION -  get records by condition
//*
//*   PARAMS:
//*       ptrM: the pointer of  object which saves the conditions
//*		cols: condition
//*       ptrList: like
//*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
//*   RETURNS:
//*       return err error
//*               err =nil ; ptrList is the result
//*
//*   Examples:
//*        tableInfo := new(TableInfoStruct)
//*        tableInfo.Id = 1
//*        tableInfo.A = "willbeupdated"
//*        ptrM := &tableInfo
//*		 var ptrList []TableInfoStruct
//*        cols := []string{"Id","A"}
//*        err := ReadRecordsByCols(ptrM, cols, ptrList，o)
//*
// */
func ReadRecordsByCols(ptrM interface{}, cols []string, ptrList interface{}, ptrOrmer orm.Ormer) (err error) {
	defer DoDaoException(ptrM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}
	//判断查询条件是否为空，为空不允许查询select *, 返回错误
	if len(cols) == 0 {
		msg := fmt.Sprintf("Forbid to select *! object=[%+v], cols=[%v]", ptrM, cols)
		log.Log.Warn(msg)
		return errors.New(msg)
	}
	qs := ptrOrmer.QueryTable(ptrM)
	//循环query，查询需要删除的条目
	for _, colName := range cols {
		value := getFieldVal(ptrM, colName)
		if value == nil {
			msg := fmt.Sprintf("GetFieldVal fail. ptrM=[%+v], cols=[%+v]", ptrM, cols)
			log.Log.Warn(msg)
			return errors.New(msg)
		}
		//按照字段值筛选
		qs = qs.Filter(colName, value)
	}
	//获取所有过滤出的条目，前期先不限定返回数量，返回所有字段
	count, err := qs.Limit(-1).All(ptrList)
	if err != nil {
		log.Log.Warn("Fail to read records=[%+v] by cols=[%+v]! error=[%v]",
			ptrM, cols, err)
		return err
	}

	log.Log.Debug("Read count=[%v] records=[%+v] by cols=[%+v] successfully!",
		count, ptrM, cols)
	return err
}

//基于复杂条件的数据查询
///*
//*   QueryModelByConds
//*
//*   DESCRIPTION -  Select the specified column based on the criteria
//*
//*   PARAMS:
//*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
//*		  rst: the pointer of  object which saves the query result
//*       ptrM: the pointer of  object which saves the conditions
//*       whereConds: All where conditions
//*   RETURNS:
//*       return err error
//*
//*   Examples:
//*       instList := new([]DbInstance)
//*       whereConds := []global.WhereConds{
//		{
//			Column: "Status",
//			Expr:   global.Expr_In,
//			Value:  []interface{}{1, 2},
//		},
//		{
//			Column: "heartbeat",
//			Expr:   global.Expr_Less_Than,
//			Value:  []interface{}{1111},
//		}}
//*		err := QueryModelByConds(nil, instList, "db_node", whereConds)
//*     fmt.Println(instList)
//*
// */
func QueryModelByConds(ptrOrmer orm.Ormer, rst interface{}, ptrM interface{}, whereConds []WhereConds) (err error) {
	defer DoDaoException(ptrM)
	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}
	conds, err := parseConds(whereConds)
	if nil != err {
		log.Log.Warn("Failed to select, reason=[%v]", err)
		return err
	}
	//需要修改的列定义
	qs := ptrOrmer.QueryTable(ptrM)
	_, err = qs.SetCond(conds).All(&rst)
	if err != nil {
		log.Log.Warn("Failed to select, reason=[%v]", err)
	}
	return err
}

//基于复杂条件的count查询
///*
//*   QueryModelCount
//*
//*   DESCRIPTION -  query the specified column based on the criteria
//*
//*   PARAMS:
//*
//*		  whereConds: All where conditions
//*       ptrM: Ahe name of the query object model name
//*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
//*   RETURNS:
//*       return cnt int64,err error
// 				  cnt: Number of query count
//*               err: error info
//*
//*   Examples:
//*        whereConds := []global.WhereConds{
////		{
////			Column: "Status",
////			Expr:   global.Expr_In,
////			Value:  []interface{}{1, 2},
////		},
////		{
////			Column: "heartbeat",
////			Expr:   global.Expr_Less_Than,
////			Value:  []interface{}{1111},
////		}}
//          cnt, err := QueryModelCount(nil, "db_node", whereConds)
// */
func QueryModelCount(ptrOrmer orm.Ormer, model interface{}, whereConds []WhereConds) (cnt int64, err error) {
	defer DoDaoException(model)
	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}
	conds, err := parseConds(whereConds)
	if nil != err {
		log.Log.Warn("Failed to select, reason=[%v]", err)
		return 0, err
	}
	qs := ptrOrmer.QueryTable(model).SetCond(conds)
	cnt, err = qs.Count()
	if err != nil {
		log.Log.Warn("Failed to select, reason=[%v]", err)
		return 0, err
	}
	return cnt, err
}

//基于复杂条件的数据更新
///*
//*   UpdateByConds
//*
//*   DESCRIPTION -  Updates the specified column based on the criteria
//*
//*   PARAMS:
//*       ptrM: the pointer of  object which saves the conditions
//*		  whereConds: All where conditions
//*       columnSet:The columns to update TYPE: orm.Params example: {"column":value,...}
//*       ptrOrmer: if the ptrOrmer is not nil, it used to control transaction
//*   RETURNS:
//*       return updatedCount int64,err error
// 				  updatedCount: Number of rows affected
//*               err =nil ; ptrList is the result
//*
//*   Examples:
//*        tableInfo := new(TableInfoStruct)
//*        ptrM := &tableInfo
//*        whereConds := []WhereConds{
//		{
//			Column: "Status",
//			Expr:   Expr_In,
//			Value:  []interface{}{1, 2},
//		},
//		{
//			Column: "heartbeat",
//			Expr:   Expr_Less_Than,
//			Value:  []interface{}{1111},
//		}}
//*		 columnSet := orm.Params{"profile_id": 666, "name": "haha"}
//*      num,err := UpdateByConds(ptrM, whereConds,columnSet, ptrM.ptrOrmer)
//*
// */

func UpdateByConds(ptrM interface{}, whereConds []WhereConds, columnSet orm.Params,
	ptrOrmer orm.Ormer) (updatedCount int64, err error) {

	defer DoDaoException(ptrM)

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	//whereConds和columnSet都不能为空
	if len(whereConds) == 0 || len(columnSet) == 0 {
		msg := fmt.Sprintf(" whereConds is nil or columnSet is nil. Update record=[%+v] failed, "+
			"len(columnSet)=[%v],len(columnSet)=[%v]", ptrM, len(whereConds), len(columnSet))
		log.Log.Warn(msg)
		return 0, errors.New(msg)
	}
	//初始化自定义条件表达式
	conds, err := parseConds(whereConds)
	if nil != err {
		log.Log.Warn("Failed to update, reason=[%v]", err)
		return 0, err
	}
	//需要修改的列定义
	qs := ptrOrmer.QueryTable(ptrM)
	num, err := qs.SetCond(conds).Update(columnSet)
	if err != nil {
		log.Log.Warn("Failed to update, reason=[%v]", err)
		return 0, err
	}
	return num, nil
}

/* Orm 原始SQL执行
 * Examples:
 * 非查询类：
 *     err := RawUpdate(o, "update db_clusters set cluster_name = ? where id = ? and status = ?", "cluster01", 1, 0)
 *     err := RawInsert(o, "insert into db_clusters(cluster_id,cluster_name) values(?,?)", 10, "cluster04")
 *     err := RawDelete(o,  "delete from db_clusters where cluster_id = ? and status = ?", 6, 0)
 * 查询类：
 * 结果集为多行：
 *     var cluters []DbCluster
 *     rstNum, err := RawQueryRows(o, &cluters, "select * from db_clusters where id != ? and status = ?", 3, 0)
 *     for clu := range cluters {
 *      	fmt.Println(clu)
 *     }
 * 结果集为单行：
 *     var cnt int64
 *     err := RawQueryRow(o, &cnt, "select count(1) from db_clusters where id != ? and status = ?", 3, 0)
 *     fmt.Println(cnt)
 */

// 执行insert类原始SQL
func RawInsert(ptrOrmer orm.Ormer, sql string, args ...interface{}) (sql.Result, error) {
	return RawExecSql(ptrOrmer, sql, args)
}

// 执行update类原始SQL
func RawUpdate(ptrOrmer orm.Ormer, sql string, args ...interface{}) (sql.Result, error) {
	return RawExecSql(ptrOrmer, sql, args)
}

// 执行delete类原始SQL
func RawDelete(ptrOrmer orm.Ormer, sql string, args ...interface{}) (sql.Result, error) {
	return RawExecSql(ptrOrmer, sql, args)
}

// 执行非查询类SQL实现函数
func RawExecSql(ptrOrmer orm.Ormer, sql string, args ...interface{}) (sql.Result, error) {

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	rawSet := ptrOrmer.Raw(sql, args)
	defer DoDaoException(rawSet)

	result, err := rawSet.Exec()
	if err != nil {
		log.Log.Warn("Execute non query sql failed! Sql=[%v] args=[%v] Err=[%v]", sql, args, err)
	}

	log.Log.Debug("Execute non query sql successfully. Sql=[%v] args=[%v]", sql, args)
	return result, err
}

// 执行查询类SQL，且结果集为单行
func RawQueryRow(ptrOrmer orm.Ormer, rst interface{}, sql string, args ...interface{}) error {

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	rawSet := ptrOrmer.Raw(sql, args)
	defer DoDaoException(rawSet)

	err := rawSet.QueryRow(rst)

	if err != nil {
		log.Log.Warn("Execute query sql failed! Sql=[%v] args=[%v] Err=[%v]", sql, args, err)
	}

	log.Log.Debug("Execute query sql successfully. Sql=[%v] args=[%v]", sql, args)
	return err
}

// 执行查询类SQL，且结果集为多行
func RawQueryRows(ptrOrmer orm.Ormer, rst interface{}, sql string, args ...interface{}) (int64, error) {

	if ptrOrmer == nil {
		ptrOrmer = orm.NewOrm()
	}

	rawSet := ptrOrmer.Raw(sql, args)
	defer DoDaoException(rawSet)

	retNum, err := rawSet.QueryRows(rst)
	if err != nil {
		log.Log.Warn("Execute query sql failed! Sql=[%v] args=[%v] Err=[%v]", sql, args, err)
	}

	log.Log.Debug("Execute query sql successfully. Sql=[%v] args=[%v]", sql, args)
	return retNum, err
}

// 解析where条件
func parseConds(whereConds []WhereConds) (conds *orm.Condition, err error) {
	//whereConds不能为空
	if len(whereConds) == 0 {
		msg := fmt.Sprintf(" whereConds is nil . len(columnSet)=[%v] ", len(whereConds))
		log.Log.Warn(msg)
		return conds, errors.New(msg)
	}
	//初始化自定义条件表达式
	conds = orm.NewCondition()
	for _, whereCond := range whereConds {
		switch whereCond.Expr {
		case Expr_Equal:
			conds = conds.And(whereCond.Column, whereCond.Value[0])
		case Expr_Not_Equal:
			column := whereCond.Column + "__iexact"
			conds = conds.And(column, whereCond.Value[0])
		case Expr_Greater_Than:
			column := whereCond.Column + "__gt"
			conds = conds.And(column, whereCond.Value[0])
		case Expr_Less_Than:
			column := whereCond.Column + "__lt"
			conds = conds.And(column, whereCond.Value[0])
		case Expr_In:
			column := whereCond.Column + "__in"
			conds = conds.And(column, whereCond.Value...)
		default:
			errmsg := fmt.Sprintf("Error Where Expr")
			log.Log.Warn(errmsg)
			return conds, errors.New(errmsg)
		}
	}
	return conds, nil
}
