package log

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

const TIME_FORMAT = "2006-01-02 15:04:05"

var BaseDir string

type Configuration struct {
	OnlineDSN        *dsn `yaml:"online-dsn"`     // 线上环境数据库配置
	TestDSN          *dsn `yaml:"test-dsn"`       // 测试环境数据库配置
	MysqlConnTimeOut int  `yaml:"conn-time-out"`  // 数据库连接超时时间，单位秒
	QueryTimeOut     int  `yaml:"query-time-out"` // 数据库SQL执行超时时间，单位秒

	// +++++++++++++++日志相关+++++++++++++++++
	// 日志级别，这里使用了 beego 的 log 包
	// [0:Emergency, 1:Alert, 2:Critical, 3:Error, 4:Warning, 5:Notice, 6:Informational, 7:Debug]
	LogLevel int `yaml:"log-level"`
	// 日志输出位置，默认日志输出到控制台
	// 目前只支持['console', 'file']两种形式，如非console形式这里需要指定文件的路径，可以是相对路径
	LogOutput string `yaml:"log-output"`
	// 日志最大保留天数
	LogMaxDays int `yaml:"log-maxdays"`
	// +++++++++++++++日志相关结束+++++++++++++++++
                       // CS_id
	// +++++++++++++++dao相关+++++++++++++++++
	RegisterDatabase string `yaml:"register-database"`
	MaxIdleConns     int    `yaml:"max-idle-conns"`
	MaxOpenConns     int    `yaml:"max-open-conns"`
	OrmDebugSwitch   bool   `yaml:"orm-debug-switch"`
	// +++++++++++++++worker相关+++++++++++++++++
	WorkerNumber      int `yaml:"worker-number"`
	WorkerChanTimeOut int `yaml:"worker-chan-timeout"`
}

var Config = &Configuration{
	OnlineDSN: &dsn{
		Schema:  "information_schema",
		Charset: "utf8mb4",
		Disable: true,
		Version: 99999,
	},
	TestDSN: &dsn{
		Schema:  "information_schema",
		Charset: "utf8mb4",
		Disable: true,
		Version: 99999,
	},


	MysqlConnTimeOut:     3,
	QueryTimeOut:         30,
	LogLevel:             3,
	LogOutput:            "tinker.log",
	LogMaxDays:           30,

	RegisterDatabase:              "mysql",
	MaxIdleConns:                  30,
	MaxOpenConns:                  3000,
	OrmDebugSwitch:                false,
	WorkerNumber:                  60, //Worker协程数
	WorkerChanTimeOut:             10, //Worker单个协程空闲超时时间
}

//keep alive配置
type keepaliveclientparam struct {
	Time                int  `yaml:"time"`
	TimeOut             int  `yaml:"time-out"`
	PermitWithOutStream bool `yaml:"permit-without-stream"`
}
type keepaliveserverparam struct {
	MaxConnectionIdle     int `yaml:"max-connection-idle"`
	MaxConnectionAge      int `yaml:"max-connection-age"`
	MaxConnectionAgeGrace int `yaml:"max-connection-age-grace"`
	Time                  int `yaml:"time"`
	Timeout               int `yaml:"time-out"`
}
type keepaliveenforcementpolicy struct {
	MinimumTime         int  `yaml:"minimum-time"`
	PermitWithoutStream bool `yaml:"permit-without-stream"`
}
type serverrpckeepalive struct {
	ClientParam       *keepaliveclientparam       `yaml:"client-param"`
	ServerParam       *keepaliveserverparam       `yaml:"server-param"`
	EnforcementPolicy *keepaliveenforcementpolicy `yaml:"enforcement-policy"`
}
type rpcdsn struct {
	Addr              string `yaml:"addr"`
	Port              int    `yaml:"port"`
	Hostname          string `yaml:"hostname"`
	ConnectionTimeout int    `yaml:"connection-timeout"`
}
type dsn struct {
	Addr     string `yaml:"addr"`
	Schema   string `yaml:"schema"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Charset  string `yaml:"charset"`
	Disable  bool   `yaml:"disable"`
	//版本自动检查，不可配置
	Version int `yaml:"-"`
}

// 加载配置文件
func (conf *Configuration) readConfigFile(path string) error {
	configFile, err := os.Open(path)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("readConfigFile(%s) os.Open failed: %v", path, err))
		return err
	}
	defer configFile.Close()
	content, err := ioutil.ReadAll(configFile)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("readConfigFile(%s) ioutil.ReadAll failed: %v", path, err))
		return err
	}
	err = yaml.Unmarshal(content, Config)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("readConfigFile(%s) yaml.Unmarshal failed: %v", path, err))
	}
	return err
}

// 配置初始化
func ParseConfig(configFile string) error {
	var err error
	// 如果未传入配置文件，则返回报错
	if "" == configFile {
		return errors.New("No config file input")
	}
	// 配置文件状态检查
	if _, err = os.Stat(configFile); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v [tinker start failed] Check config file failed."+
			" err=%v ConfFile=%v \n", time.Now().Format(TIME_FORMAT), err, configFile))
		return err
	}
	// 配置文件解析
	if err = Config.readConfigFile(configFile); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("%v [tinker start failed]"+
			" Parse config file failed. ConfFile=%v err=%v \n", time.Now().Format(TIME_FORMAT), err, configFile))
		return err
	}
	return LoggerInit()
}
