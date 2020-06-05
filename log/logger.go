/*
 * 日志打印规范：
 * 参考百度习惯，统一使用Debug、Notice、Warn、Critical四级日志格式
 * Debug：程序调试信息
 * Notice：程序逻辑正常未出错，对有价值信息进行日志记录
 * Warn：程序逻辑出现错误，但不影响继续正常运行，日志记录错误
 * Critical：程序出现严重错误无法正常运行，需要日志记录错误并退出
 */
package log

import (
	"encoding/json"
	"regexp"
	"runtime"
	"strings"

	"github.com/astaxie/beego/logs"
)

// Log 使用 beego 的 log 库
var Log *logs.BeeLogger

func init() {
	Log = logs.NewLogger(0)
	Log.EnableFuncCallDepth(true)
}

// Log配置初始化
func LoggerInit() error {
	Log.SetLevel(Config.LogLevel)

	logConfig := map[string]interface{}{
		"filename": Config.LogOutput,
		"level":    Config.LogLevel,
		"maxlines": 0,
		"maxsize":  0,
		"daily":    true,
		"maxdays":  Config.LogMaxDays,
	}

	logConfigJSON, _ := json.Marshal(logConfig)
	err := Log.SetLogger(logs.AdapterFile, string(logConfigJSON))
	return err
}

// 返回调用者函数名称
// 参考https://stackoverflow.com/questions/35212985/is-it-possible-get-information-about-caller-function-in-golang
func Caller() string {
	// we get the callers as uintptrs - but we just need 1
	fpcs := make([]uintptr, 1)

	// skip 3 levels to get to the caller of whoever called Caller()
	n := runtime.Callers(3, fpcs)
	if n == 0 {
		return "n/a" // proper error her would be better
	}

	// get the info of the actual function that's in the pointer
	fun := runtime.FuncForPC(fpcs[0] - 1)
	if fun == nil {
		return "n/a"
	}

	// return its name
	return fun.Name()
}

// 获取调当前函数名
func GetFunctionName() string {
	// Skip this function, and fetch the PC and file for its parent
	pc, _, _, _ := runtime.Caller(1)
	// Retrieve a Function object this functions parent
	functionObject := runtime.FuncForPC(pc)
	// Regex to extract just the function name (and not the module path)
	extractFnName := regexp.MustCompile(`^.*\.(.*)$`)
	fnName := extractFnName.ReplaceAllString(functionObject.Name(), "$1")
	return fnName
}

// get filename from path
func fileName(original string) string {
	i := strings.LastIndex(original, "/")
	if i == -1 {
		return original
	}
	return original[i+1:]
}

// LogIfError 简化if err != nil 打 Error 日志代码长度
func LogIfError(err error, format string, v ...interface{}) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		if format == "" {
			format = "[%s:%d] %s"
			Log.Critical(format, fileName(fn), line, err.Error())
		} else {
			format = "[%s:%d] " + format + " Error: %s"
			Log.Critical(format, fileName(fn), line, v, err.Error())
		}
	}
}

// LogIfWarn 简化if err != nil 打 Warn 日志代码长度
func LogIfWarn(err error, format string, v ...interface{}) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		if format == "" {
			format = "[%s:%d] %s"
			Log.Warn(format, fileName(fn), line, err.Error())
		} else {
			format = "[%s:%d] " + format + " Error: %s"
			Log.Warn(format, fileName(fn), line, v, err.Error())
		}
	}
}
