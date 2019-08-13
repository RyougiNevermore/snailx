package snailx

import (
	"fmt"
	"github.com/mattn/go-colorable"
	"io"
	"os"
	"runtime"
	"sync"
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

var logger Logger = Newlogger(LoggerLevelWarn, os.Stdout)

func SetLogger(log Logger)  {
	logger = log
}

func Log() Logger {
	return logger
}

func Newlogger(level int, w io.Writer) Logger {
	colorable.NewColorableStdout()
	return &slog{Level:level, locker:&sync.Mutex{}, writer:w}
}

type slog struct {
	locker sync.Locker
	Level int
	writer io.Writer
}

func getRunLocation(calldepth int) (loc string) {
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
	loc := getRunLocation(2)
	s.locker.Lock()
	defer s.locker.Unlock()
	_, _ = s.writer.Write([]byte(fmt.Sprintf("[snailx] [\x1b[32mDEBUG\x1b[0m] [%-29s] %s %s" , time.Now().Format("2006-01-02 15:04:05.999999999"), loc, fmt.Sprintf(format, args...))))

}

func (s *slog) Infof(format string, args ...interface{}) {
	if s.Level > LoggerLevelInfo {
		return
	}
	loc := getRunLocation(2)
	s.locker.Lock()
	defer s.locker.Unlock()
	_, _ = s.writer.Write([]byte(fmt.Sprintf("[snailx] [\x1b[36mINFO\x1b[0m ] [%-29s] %s %s" , time.Now().Format("2006-01-02 15:04:05.999999999"), loc, fmt.Sprintf(format, args...))))
}

func (s *slog) Warnf(format string, args ...interface{}) {
	if s.Level > LoggerLevelWarn {
		return
	}
	loc := getRunLocation(2)
	s.locker.Lock()
	defer s.locker.Unlock()
	_, _ = s.writer.Write([]byte(fmt.Sprintf("[snailx] [\x1b[33mWARN\x1b[0m ] [%-29s] %s %s" , time.Now().Format("2006-01-02 15:04:05.999999999"), loc, fmt.Sprintf(format, args...))))
}

func (s *slog) Errorf(format string, args ...interface{}) {
	if s.Level > LoggerLevelError {
		return
	}
	loc := getRunLocation(2)
	s.locker.Lock()
	defer s.locker.Unlock()
	_, _ = s.writer.Write([]byte(fmt.Sprintf("[snailx] [\x1b[31mERROR\x1b[0m] [%-29s] %s %s" , time.Now().Format("2006-01-02 15:04:05.999999999"), loc, fmt.Sprintf(format, args...))))
}

func (s *slog) Fatalf(format string, args ...interface{}) {
	if s.Level > LoggerLevelFatal {
		return
	}
	loc := getRunLocation(2)
	s.locker.Lock()
	defer s.locker.Unlock()
	_, _ = s.writer.Write([]byte(fmt.Sprintf("[snailx] [\x1b[31mFATAL\x1b[0m] [%-29s] %s %s" , time.Now().Format("2006-01-02 15:04:05.999999999"), loc, fmt.Sprintf(format, args...))))
	os.Exit(2)
}
