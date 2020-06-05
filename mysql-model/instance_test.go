package dao

import (
	"github.com/astaxie/beego/orm"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func init() {
	//注册数据库驱动
	orm.RegisterDriver("mysql", orm.DRMySQL)

	//注册数据库
	orm.RegisterDataBase("default", "mysql",
		"gotest:gotest123@tcp(127.0.0.1:3366)/tinker?charset=utf8mb4", 30, 100)

	//需要在init中注册定义的model
	orm.RegisterModel(new(DbInstance))
}

//测试db_instances的读接口
func TestDbInstanceRead(t *testing.T) {

	Convey("TestDbInstanceReadByIndexs return nil when read successfuly.", t, func() {
		instance := new(DbInstance)
		instance.ClusterId = 3308
		instance.NodeId = 1
		instance.InstanceId = 1
		So(instance.ReadByInstanceId(), ShouldBeEmpty)
	})

	Convey("TestDbInstanceReadByIndexs return error when read failed.", t, func() {
		instance := new(DbInstance)
		instance.ClusterId = 3310
		instance.NodeId = 3310
		instance.InstanceId = 3310
		So(instance.ReadByInstanceId(), ShouldBeError)
	})

	Convey("TestDbInstanceReadById return nil when read successfuly.", t, func() {
		instance := new(DbInstance)
		instance.Id = 5
		So(instance.ReadById(), ShouldBeEmpty)
	})

	Convey("TestDbInstanceReadByIndexs return error when read failed.", t, func() {
		instance := new(DbInstance)
		instance.Id = 10
		So(instance.ReadById(), ShouldBeError)
	})

	Convey("TestDbInstanceReadAll return nil when read successfuly.", t, func() {
		instance := new(DbInstance)
		result, err := instance.ReadAllInstance()
		So(err, ShouldBeEmpty)
		So(result, ShouldNotBeNil)
	})

	Convey("TestDbInstanceReadAll return nil when read successfuly.", t, func() {
		instance := new(DbInstance)
		instance.ClusterId = 3308
		result, err := instance.ReadInstancesByCols([]string{"ClusterId"})
		So(err, ShouldBeEmpty)
		So(result, ShouldNotBeNil)
	})
}

//
//测试db_instances的新增和删除函数
func TestDbInstanceInsertAndDelete(t *testing.T) {
	instance := &DbInstance{
		ClusterId:  3310,
		NodeId:     1,
		InstanceId: 1,
		Ip:         "127.0.0.1",
		Port:       3306,
		Status:     0,
		Role:       1,
	}

	instanceIn := &DbInstance{
		ClusterId:  3308,
		NodeId:     0,
		InstanceId: 6,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}
	instanceIn.InsertOneRecord()

	Convey("TestDbClusterInsert return newId!=0, err==nil when insert successfuly.", t, func() {
		newId, err := instance.InsertOneRecord()
		So(err, ShouldBeEmpty)
		So(newId, ShouldNotEqual, int64(0))
	})

	Convey("TestDbClusterInsert return newId==0, err!=nil when insert a same record.", t, func() {
		newId, err := instance.InsertOneRecord()
		So(err, ShouldBeError)
		So(newId, ShouldBeLessThan, 0)
	})

	Convey("TestDbClusterDeleteByIndexs return nums!=0, err==nil when delete records successfully.", t, func() {
		nums, err := instance.DeleteByCondCols(instance.Indexes())
		So(err, ShouldBeEmpty)
		So(nums, ShouldNotEqual, int64(0))
	})

	Convey("TestDbClusterDeleteByIndexs return nums==0, err==nil when delete a records which doesn't exist.", t, func() {
		nums, err := instance.DeleteByCondCols(instance.Indexes())
		So(err, ShouldBeEmpty)
		So(nums, ShouldBeZeroValue)
	})

	Convey("TestDbClusterDeleteByIndexs return num==0, err!=nil when delete a records with field which doesn't exist.", t, func() {
		var col []string
		col = append(col, "xxx")
		nums, err := instance.DeleteByCondCols(col)
		So(err, ShouldBeError)
		So(nums, ShouldBeZeroValue)
	})
}

//测试db_instances的更新函数
func TestDbInstanceUpdate(t *testing.T) {
	instance := &DbInstance{
		ClusterId:  3308,
		NodeId:     1,
		InstanceId: 2,
		Ip:         "127.0.1.1",
		Port:       3307,
		Status:     1,
		Role:       0,
	}
	col := []string{"Ip", "Port"}

	Convey("TestDbClusterUpdateByIndexs return nums!=0, err==nil when update a record by indexs successfully", t, func() {
		nums, err := instance.UpdateByIndexs(col)
		So(err, ShouldBeEmpty)
		So(nums, ShouldEqual, int64(0))
	})

	Convey("TestDbClusterUpdateByIndexs return num==0, err==nil when update a same record.", t, func() {
		nums, err := instance.UpdateByIndexs(col)
		So(err, ShouldBeEmpty)
		So(nums, ShouldBeZeroValue)
	})

	instanceColumn := &DbInstance{
		ClusterId:  3308,
		NodeId:     1,
		InstanceId: 1,
		Ip:         "127.0.3.1",
		Port:       3308,
		Status:     1,
		Role:       0,
	}

	cond := instance.Indexes()

	Convey("TestDbClusterUpdateByCondCols return num!=0, err==nil when update successfully.", t, func() {
		nums, err := instanceColumn.UpdateByCondCols(cond, col)
		So(err, ShouldBeNil)
		So(nums, ShouldNotEqual, int64(0))
	})

	Convey("TestDbClusterUpdateByCondCols return num==0, err==nil when update the same records.", t, func() {
		nums, err := instanceColumn.UpdateByCondCols(cond, col)
		So(err, ShouldBeEmpty)
		So(nums, ShouldBeZeroValue)
	})

	instanceRecover := &DbInstance{
		ClusterId:  3308,
		NodeId:     1,
		InstanceId: 1,
		Ip:         "127.0.2.1",
		Port:       3308,
		Status:     1,
		Role:       0,
	}
	instanceRecover.UpdateByCondCols(cond, col)
}

func TestInstanceOtherMethod(t *testing.T) {
	instance := &DbInstance{
		Id:         -1,
		ClusterId:  3308,
		NodeId:     0,
		InstanceId: 6,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}

	var ptrOrmer orm.Ormer

	Convey("GetPtrOrmer successfully", t, func() {
		o := instance.GetPtrOrmer()
		So(o, ShouldHaveSameTypeAs, ptrOrmer)
	})

	Convey("ReadById :t.Id <= 0", t, func() {
		err := instance.ReadById()
		So(err, ShouldNotBeNil)
	})

	Convey("UpdateByIndexs、UpdateByCondCols、ReadInstancesByCols : exec fail", t, func() {
		col := []string{"InvildCols1", "InvildCols2"}
		col1 := []string{}
		_, delErr := instance.DeleteByCondCols(col1)
		_, readErr := instance.ReadInstancesByCols(col)
		_, err := instance.UpdateByCondCols(col1, col1)
		_, err1 := instance.UpdateByCondCols(col, col)
		_, err2 := instance.UpdateByIndexs(col)
		_, err3 := instance.ReadInstancesByCols(col)
		So(delErr, ShouldNotBeNil)
		So(readErr, ShouldNotBeNil)
		So(err, ShouldNotBeNil)
		So(err1, ShouldNotBeNil)
		So(err2, ShouldNotBeNil)
		So(err3, ShouldNotBeNil)
	})
}

func TestReadRecordsByMultiConditions(t *testing.T) {

	instance := &DbInstance{
		ClusterId: 1,
		Role:      1,
	}
	Convey("TEST ReadRecordsByMultiConditions...", t, func() {
		whereConds := []WhereConds{
			{Column: "instance_id", Expr: Expr_Equal, Value: []interface{}{2}},
			{Column: "cluster_id", Expr: Expr_In, Value: []interface{}{1, 2, 3}},
		}
		result, err := instance.ReadDbInstanceByMultiCons(whereConds)
		So(err, ShouldBeNil)
		So(len(result), ShouldEqual, 2)

	})

}
