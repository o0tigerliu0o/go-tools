package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// mysql连接池
type DBPool struct {
	*sql.DB
}

// 数据库查询返回值
type QueryResult struct {
	Rows      *sql.Rows
	Result    sql.Result
	Error     error
	Warning   []QueryWarning
	QueryCost float64
}

// SQL语句Warn/Error解析
type QueryWarning struct {
	Level   string
	Code    int32
	Message string
}

// SQL语句showVariables解析
type queryShow struct {
	VariableName string
	Value        string
}

// SQL语句show master status解析
type QueryMasterStatus struct {
	File              string
	Position          int64
	Binlog_Do_DB      string
	Binlog_Ignore_DB  string
	Executed_Gtid_Set string
}

// SQL语句show slave status解析
type QuerySlaveStatus struct {
	Slave_IO_State                string
	Master_Host                   string
	Master_User                   string
	Master_Port                   int32
	Connect_Retry                 string
	Master_Log_File               string
	Read_Master_Log_Pos           int64
	Relay_Log_File                string
	Relay_Log_Pos                 int64
	Relay_Master_Log_File         string
	Slave_IO_Running              string
	Slave_SQL_Running             string
	Replicate_Do_DB               string
	Replicate_Ignore_DB           string
	Replicate_Do_Table            string
	Replicate_Ignore_Table        string
	Replicate_Wild_Do_Table       string
	Replicate_Wild_Ignore_Table   string
	Last_Errno                    string
	Last_Error                    string
	Skip_Counter                  string
	Exec_Master_Log_Pos           int64
	Relay_Log_Space               string
	Until_Condition               string
	Until_Log_File                string
	Until_Log_Pos                 string
	Master_SSL_Allowed            string
	Master_SSL_CA_File            string
	Master_SSL_CA_Path            string
	Master_SSL_Cert               string
	Master_SSL_Cipher             string
	Master_SSL_Key                string
	Seconds_Behind_Master         int32
	Master_SSL_Verify_Server_Cert string
	Last_IO_Errno                 string
	Last_IO_Error                 string
	Last_SQL_Errno                string
	Last_SQL_Error                string
	Replicate_Ignore_Server_Ids   string
	Master_Server_Id              string
	Master_UUID                   string
	Master_Info_File              string
	SQL_Delay                     string
	SQL_Remaining_Delay           string
	Slave_SQL_Running_State       string
	Master_Retry_Count            string
	Master_Bind                   string
	Last_IO_Error_Timestamp       string
	Last_SQL_Error_Timestamp      string
	Master_SSL_Crl                string
	Master_SSL_Crlpath            string
	Retrieved_Gtid_Set            string
	Executed_Gtid_Set             string
	Auto_Position                 string
	Replicate_Rewrite_DB          string
	Channel_Name                  string
	Master_TLS_Version            string
}

// database/sql中可不返回error的报错
const (
	ROW_PART_COLUMN_SCAN_ERROR string = "Scan error on column index"
)
