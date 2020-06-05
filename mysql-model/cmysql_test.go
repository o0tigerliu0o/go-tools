package dao

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"go-tools/log"
	"testing"
	"github.com/astaxie/beego/orm"
)

func init() {
	log.ParseConfig("../conf/tinker.yaml")
	InitDao()

}

func TestQueryModelCount(t *testing.T) {
	// 得到集群的承接读流量的从库节点信息
	whereConds := []WhereConds{{
		Column: "proxy_node_json",
		Expr:   Expr_Equal,
		Value:  []interface{}{1},
	}}

	cnt, err := QueryModelCount(nil, "db_clusters", whereConds)

	Convey("proxyNodeJson : exec successful", t, func() {
		fmt.Println("---------------", cnt)
		fmt.Println("---------------", err)
		So(cnt, ShouldEqual, 1)
	})

}

func TestRaw(t *testing.T) {
	var o orm.Ormer

	Convey("Raw :update accounts set , condtione = 2.", t, func() {
		_, err := RawExecSql(nil, "update accounts set ip_white_list = ?,bns_white_list=? "+
			"where cluster_id = ? and proxy_account_name = ?", "a", "b", 1, "c")
		So(err, ShouldBeNil)
	})


	Convey("Raw : update sql, condtione = 3.", t, func() {
		_, err := RawUpdate(o, "update db_clusters set cluster_name = ? where id = ? and status = ?", "cluster01", 1, 0)
		So(err, ShouldBeNil)

	})

	Convey("Raw : insert sql, condtione = 3.", t, func() {
		_, err := RawInsert(o, "insert into db_clusters(cluster_id,cluster_name) values(?,?)", 10, "cluster04")
		So(err, ShouldEqual, "<QuerySeter> no row found")
	})

	Convey("Raw : delete sql, condtione = 1.", t, func() {
		_, err := RawDelete(o, "delete from db_clusters where cluster_id = ? and status = ?", 6, 0)
		So(err, ShouldBeNil)
	})
}

func TestRead(t *testing.T) {

	instance := &DbInstance{
		ClusterId:  3308,
		NodeId:     0,
		InstanceId: 66,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}
	cols1 := []string{}
	cols2 := append(cols1, "ClusterId")
	Convey("Read : len(cols) == 0.", t, func() {
		err := Read(instance, cols1, instance.ptrOrmer)
		So(err, ShouldBeError)
	})
	Convey("Read : len(cols) != 0.", t, func() {
		err := Read(instance, cols2, instance.ptrOrmer)
		So(err, ShouldBeNil)
	})
	var result []DbInstance

	Convey("ReadRecordsByCols : len(cols) == 0.", t, func() {
		err := ReadRecordsByCols(instance, cols1, &result, instance.ptrOrmer)
		So(err, ShouldBeError)
	})
	Convey("ReadRecordsByCols : len(cols) != 0.", t, func() {
		err := ReadRecordsByCols(instance, cols2, &result, instance.ptrOrmer)
		So(err, ShouldBeNil)
	})
}

func TestUpdate(t *testing.T) {
	instance := &DbInstance{
		Id:         7,
		ClusterId:  3308,
		NodeId:     0,
		InstanceId: 66,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}
	cols := []string{}
	Convey("Update : cols len == 0", t, func() {
		_, err := Update(instance, cols, instance.ptrOrmer)
		So(err, ShouldNotBeNil)
	})
	cols2 := []string{"Port", "Status"}

	Convey("Update : len != 0 exec successful", t, func() {
		len, err := Update(instance, cols2, instance.ptrOrmer)
		So(err, ShouldBeNil)
		So(len, ShouldEqual, 1)
	})

	Convey("Update : len(cols) == 0", t, func() {
		_, err := Update(instance, cols, instance.ptrOrmer)
		So(err, ShouldBeError)
	})
}

func TestInsert(t *testing.T) {
	instance := &DbInstance{
		Id:         10,
		ClusterId:  3309,
		NodeId:     1,
		InstanceId: 7,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}

	Convey("TEST Insert successful...", t, func() {
		_, err := Insert(instance, instance.ptrOrmer)
		So(err, ShouldBeNil)
	})
	Convey("TEST Insert failed...", t, func() {
		_, err := Insert(instance, instance.ptrOrmer)
		So(err, ShouldBeError)
	})

}

func TestDeleteByCondCols(t *testing.T) {
	instance := &DbInstance{
		Id:         10,
		ClusterId:  6,
		NodeId:     1,
		InstanceId: 6,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}
	cons := []string{}

	Convey("TESE DELETE FAILED LEN(cons) = 0...", t, func() {
		_, err := DeleteByCondCols(instance, cons, instance.ptrOrmer)
		So(err, ShouldBeError)

	})
	cons = append(cons, "ClusterId")
	Convey("TESE DELETE SUCCESSFULY...", t, func() {
		_, err := DeleteByCondCols(instance, cons, instance.ptrOrmer)
		So(err, ShouldBeNil)

	})
}

func TestReadAllRecords(t *testing.T) {

	instance := &DbInstance{
		Id:         7,
		ClusterId:  6,
		NodeId:     1,
		InstanceId: 6,
		Ip:         "127.0.1.1",
		Port:       3308,
		Status:     4,
		Role:       1,
	}
	var resSet []*DbInstance

	Convey("TEST READ DATA ...", t, func() {
		err := ReadAllRecords(instance, resSet, instance.ptrOrmer)
		So(err, ShouldBeNil)
		So(resSet, ShouldBeNil)

	})
}
