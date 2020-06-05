package dao

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/orm"
	"go-tools/log"

)

//这部分的column字段定义，等待数据库表结构定义确定后再统一修改，下方代码先暂用测试表中的字段名
//ptrOrmer *orm.Ormer说明：
//1、调用方可以通过SetPtrOrmer()和GetPtrOrmer，显式的传入数据库连接
//2、如果未显式传入连接，默认会在struct第一次sql操作时创建连接，并在结构体变量释放前，一直复用该连接，符合SQL的session一致性原则
//3、经过测试，新建一个连接，等待3分钟后再通过该连接操作sql，未出现问题。
//
type DbInstance struct {
	Id              int64  `orm:"column(id);auto;pk;index;description(主键id)"`
	ClusterId       int64  `orm:"column(cluster_id);index;description(db集群id)"`
	NodeId          int64  `orm:"column(node_id);index;description(db分片id)"`
	InstanceId      int64  `orm:"column(instance_id);index;unique;description(db实例id)"`
	Ip              string `orm:"column(ip);size(100);description(实例ip)"`
	Port            int32  `orm:"column(port);description(实例端口)"`
	Role            int32  `orm:"column(role);description(实例角色)"`
	Status          int32  `orm:"column(status);description(实例状态)"`
	Uuid            string `orm:"column(uuid);size(40)" valid:"Required;MaxSize(40);description(uuid)"`
	HeartBeat       int64  `orm:"column(heartbeat);description(实例的心跳信息)" `
	MysqlAgentPort  int32  `orm:"column(mysql_agent_port);description(mysql agent端口)"`
	File            string `orm:"column(binlog_file);size(100);description(同步的binlog文件名)"`
	Position        int64  `orm:"column(binlog_position);description(同步的binlog位置点)" `
	ExceptionNums   int    `orm:"column(exception_num);description(实例异常计数器)" `
	ExecutedGtidSet string `orm:"column(executed_gtid_set);size(1000);description(实例已执行的gtid)"`
	SwitchPriority  int    `orm:"column(switch_priority);description(集群切换时提升为主的优先级)"`

	ptrOrmer orm.Ormer
}

//显示定义数据表名
func (t *DbInstance) TableName() string {
	return "db_instances"
}

//创建唯一性索引
func (t *DbInstance) TableUnique() [][]string {
	return [][]string{
		[]string{"Ip", "Port"},
	}
}

//设置事务控制的指针
func (t *DbInstance) SetPtrOrmer(ptrOrmer orm.Ormer) {
	t.ptrOrmer = ptrOrmer
}

//获取事务控制的指针
func (t *DbInstance) GetPtrOrmer() (ptrOrmer orm.Ormer) {
	return t.ptrOrmer
}

//!DbInstance索引，可以唯一确定DbInstance记录
func (t *DbInstance) Indexes() (cols []string) {
	cols = []string{"ClusterId", "NodeId", "InstanceId"}
	return cols
}

//!DbInstance索引，可以唯一确定DbInstance记录
func (t *DbInstance) InstanceIdInds() (cols []string) {
	cols = []string{"InstanceId"}
	return cols
}

//通过索引读取唯一的数据
//*RETURNS:
//*       return err error
//*               if err ==nil ; t is result data
//*				  if err != nil; haven't found recrod.
//*EXAMPLE:
//*        t := new(DbInstance)
//*        t.clusterId = 3308
//*		   t.NodeId = 1
//*		   t.clusterId = 2
//*        err := t.ReadByIndexs()
//
func (t *DbInstance) ReadByIndexs() error {
	//按照结构体中已有的值，查询数据库记录
	err := Read(t, t.Indexes(), t.ptrOrmer)
	if err != nil {
		log.Log.Warn("Read record failed! cols=[%+v], record=[%+v], error=[%+v].",
			t.Indexes(), t, err)
		return err
	}
	log.Log.Debug("Read record=[%+v] successfully.", t)
	return err
}

//通过InstanceId读取唯一的数据
//*RETURNS:
//*       return err error
//*               if err ==nil ; t is result data
//*				  if err != nil; haven't found recrod.
//*EXAMPLE:
//*        t := new(DbInstance)
//*		   t.InstanceId = 2
//*        err := t.ReadByIndexs()
//
func (t *DbInstance) ReadByInstanceId() error {
	//按照结构体中已有的值，查询数据库记录
	err := Read(t, t.InstanceIdInds(), t.ptrOrmer)
	if err != nil {
		log.Log.Warn("Read record failed! cols=[%+v], record=[%+v], error=[%+v].",
			t.InstanceIdInds(), t, err)
	} else {
		log.Log.Debug("Read record=[%+v] successfully.", t)
	}

	return err
}

////通过pk读取唯一数据
//*RETURNS:
//*       return err error
//*               if err ==nil ; t is result data
//*				  if err != nil; haven't found recrod.
//*EXAMPLE:
//*        t := new(DbInstance)
//*        tableInfo.Id = 1
//*        err := tableInfo.ReadById()
//
func (t *DbInstance) ReadById() error {
	if t.Id <= 0 {
		return errBlankContent(t, "Id", t.TableName())
	}
	//按照结构体中已有的值，查询数据库记录
	cols := []string{"Id"}
	err := Read(t, cols, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("Read record failed! cols=[%+v], record=[%+v], error=[%+v].", cols, t, err)
		return err
	}
	log.Log.Debug("Read record=[%+v] successfully.", t)
	return err
}

//新增一条数据
//*RETURNS:
//*       return err error
//*               if err ==nil ; t is result data
//*				  if err != nil; haven't found recrod.
//*EXAMPLE:
//*        t := new(DbInstance)
//*        t.ClusterId = 3310
//*		   		.
//*		   		.(其余字段的赋值,insert时所有字段都要赋值，不能使用默认值)
//*		   		.
/////////////////////////
//*如果需要用到事务:
//*        o := orm.NewOrm()
//*        t.SetPtrOrmer(o)//!transaction
/////////////////////////
//*        newId, err := t.Insert()
//
func (t *DbInstance) InsertOneRecord() (int64, error) {
	//调用cmysql.go中的Insert函数，log记录都在Insert()中实现
	//index字段设置了unique属性，无需提前判断新增数据是否重复
	id, err := Insert(t, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("Insert record=[%+v] failed, error=[%v].", t, err)
		return -1, err
	}
	log.Log.Debug("Insert record=[%+v] successfully, newId=[%v].", t, id)
	return id, err
}

//根据索引更新对应的单条数据
//*PARAMS:
//*		  cols: the name of columns which you want to update.
//*RETURNS:
//*       return count int64, err error
//*               if err == nil ; affect count rows, count != 0
//*				  if err != nil; affect count rows. count == 0
//EXAMPLE:
//*        cols := []string{"Status"}
//*        t := new(DbInstance)
//*        t.ClusterId = 1
//*        t.NodeId = 1
//*        t.InstanceId = 1
//*        t.Status = 0
/////////////////////////
//*如果需要用到事务:
//*        o := orm.NewOrm()
//*        t.SetPtrOrmer(o)//!transaction
/////////////////////////
//*        id, err := t.UpdateByIndexs(cols)
//
func (t *DbInstance) UpdateByIndexs(cols []string) (int64, error) {
	v := *t
	count, err := UpdateByCond(&v, t.InstanceIdInds(), t, cols, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("fail to update to object=[%+v] by fieldNames=[%+v]! error=[%v].", t, cols, err)
		return 0, err
	}
	log.Log.Debug("Update object=[%+v] by fieldNames=[%+v] successfully,  affected rows=[%v].",
		t, cols, count)
	return count, err
}

//根据condCols更新，可实现批量更新
//*PARAMS:
//*       cond: the name of columns by whiches you will select records.
//*		  cols: the name of columns which you want to update.
//*RETURNS:
//*       return count int64, err error
//*               if err == nil ; affect count rows, count != 0
//*				  if err != nil; affect count rows. count == 0
//EXAMPLE:
//*        cond := []string{"ClusterName"}
//*        cols := []string{"Shardings"}
//*        t := new(DbInstance)
//*        t.ClusterName = "1"
//*        t.Shardings = "[1,2,3]"
/////////////////////////
//*如果需要用到事务:
//*        o := orm.NewOrm()
//*        t.SetPtrOrmer(o)//!transaction
/////////////////////////
//*        id, err := t.UpdateByCondCols(cond, cols)
//
func (t *DbInstance) UpdateByCondCols(cond []string, cols []string) (int64, error) {
	v := *t
	count, err := UpdateByCond(&v, cond, t, cols, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("fail to update to object=[%+v] by fieldNames=[%+v]! error=[%v].", t, cols, err)
		return 0, err
	}
	log.Log.Debug("Update object=[%+v] by fieldNames=[%+v] successfully,  affected rows=[%v].",
		t, cols, count)
	return count, err
}

//根据condCols删除，可实现批量删除
//*PARAMS:
//*       condCols: the name of columns by whiches you will select records and delete.
//*RETURNS:
//*       return count int64, err error
//*               if err == nil ; affect count rows, count != 0
//*				  if err != nil; affect count rows. count == 0
//EXAMPLE:
//*        condCols := []string{"ClusterName", "Shardings"}
//*        t := new(DbInstance)
//*        t.ClusterName = "test1"
//*        t.Shardings = "[1,2,3]"
/////////////////////////
//*如果需要用到事务:
//*        o := orm.NewOrm()
//*        t.SetPtrOrmer(o)//!transaction
/////////////////////////
//*        delCount, err := t.DeleteByCondCols(condCols)
//
func (t *DbInstance) DeleteByCondCols(condCols []string) (int64, error) {
	count, err := DeleteByCondCols(t, condCols, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("Fail to delete record=[%+v] by condCols=[%+v]! error=[%v].", t, condCols, err)
		return 0, err
	}
	log.Log.Debug("Delete record=[%+v] by condCols=[%+v] successfully! affected rows=[%v]!",
		t, condCols, count)
	return count, err
}

//遍历整张数据库表
//*RETURNS:
//*       return instances []DbInstance, err error
//*               if err == nil ; result is instances
//*				  if err != nil; result is [] or part data of the table
//*EXAMPLE:
//*        t := new(DbInstance)
//*        instanceSlice, err := t.ReadAllInstance()
//
func (t *DbInstance) ReadAllInstance() (result []DbInstance, err error) {
	err = ReadAllRecords(t, &result, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("fail to get all records from table=[%v]. error=[%v].", t.TableName(), err)
		return result, err
	}
	log.Log.Debug("Get all records from table=[%v] successfully.", t.TableName())
	return result, err
}

//读取批量数据库表
//*RETURNS:
//*       return instances []DbInstance, err error
//*               if err == nil ; result is instances
//*				  if err != nil; result is [] or part data of the table
//*EXAMPLE:
//*        t := new(DbInstance)
//*        t.ClusterId = 1
//*        cols := []string{"ClusterId"}
//*        instanceSlice, err := t.ReadInstancesByCols(cols)
//
func (t *DbInstance) ReadInstancesByCols(cols []string) (result []DbInstance, err error) {
	err = ReadRecordsByCols(t, cols, &result, t.ptrOrmer)
	if err != nil {
		log.Log.Warn("fail to get records from table=[%v] by cols=[%+v]. error=[%v].",
			t.TableName(), cols, err)
		return result, err
	}
	log.Log.Debug("Get records from table=[%v] by cols=[%+v] successfully.", t.TableName(), cols)
	return result, err
}

//单表多条件查询
func (t *DbInstance) ReadDbInstanceByMultiCons(whereConds []WhereConds) ([]*DbInstance, error) {
	if t.ptrOrmer == nil {
		t.ptrOrmer = orm.NewOrm()
	}
	//初始化自定义条件表达式
	conds := orm.NewCondition()
	for _, whereCond := range whereConds {
		switch whereCond.Expr {
		case Expr_Equal:
			conds = conds.And(whereCond.Column, whereCond.Value[0])
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
			return nil, errors.New(errmsg)
		}

	}

	qs := t.ptrOrmer.QueryTable(t)
	var resSet []*DbInstance
	_, err := qs.SetCond(conds).All(&resSet)
	if err != nil {
		log.Log.Warn("Failed to get records from db, reason=[%v]", err)
		return nil, err
	}
	return resSet, nil

}
