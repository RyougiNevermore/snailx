package snailx

import (
	"fmt"
	"github.com/mattn/go-colorable"
	"os"
	"runtime"
	"time"
)

const (
	LoggerLevelDebug = iota
	LoggerLevelInfo
	LoggerLevelWarn
	LoggerLevelError
	LoggerLevelFatal
)

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

var logger Logger = Newlogger(LoggerLevelWarn)

func SetLogger(log Logger)  {
	logger = log
}

func Newlogger(level int) Logger {
	colorable.NewColorableStdout()
	return &slog{Level:level}
}

type slog struct {
	Level int
}

func getFileLine(calldepth int) (loc string) {


	_, file, line, ok := runtime.Caller(calldepth)
	if !ok {
		file = "???"
		line = 0
	}
	loc = fmt.Sprintf("[ %s:%d ]", file, line)
	return
}

func (s *slog) Debugf(format string, args ...interface{}) {
	if s.Level > LoggerLevelDebug {
		return
	}
	fmt.Println("[snailx] [\x1b[32mDEBUG\x1b[0m]", fmt.Sprintf("[%-29s]", time.Now().Format("2006-01-02 15:04:05.999999999")), getFileLine(2), fmt.Sprintf(format, args...))
}

func (s *slog) Infof(format string, args ...interface{}) {
	if s.Level > LoggerLevelInfo {
		return
	}
	fmt.Println("[snailx] [\x1b[36mINFO\x1b[0m ]", fmt.Sprintf("[%-29s]", time.Now().Format("2006-01-02 15:04:05.999999999")), getFileLine(2), fmt.Sprintf(format, args...))
}

func (s *slog) Warnf(format string, args ...interface{}) {
	if s.Level > LoggerLevelWarn {
		return
	}
	fmt.Println("[snailx] [\x1b[33mWARN\x1b[0m ]", fmt.Sprintf("[%-29s]", time.Now().Format("2006-01-02 15:04:05.999999999")), getFileLine(2), fmt.Sprintf(format, args...))
}

func (s *slog) Errorf(format string, args ...interface{}) {
	if s.Level > LoggerLevelError {
		return
	}
	fmt.Println("[snailx] [\x1b[31mERROR\x1b[0m]", fmt.Sprintf("[%-29s]", time.Now().Format("2006-01-02 15:04:05.999999999")), getFileLine(2), fmt.Sprintf(format, args...))
}

func (s *slog) Fatalf(format string, args ...interface{}) {
	if s.Level > LoggerLevelFatal {
		return
	}
	fmt.Println("[snailx] [\x1b[31mFATAL\x1b[0m]", fmt.Sprintf("[%-29s]", time.Now().Format("2006-01-02 15:04:05.999999999")), getFileLine(2), fmt.Sprintf(format, args...))
	os.Exit(2)
}
