package snailx

import (
	"fmt"
	"os"
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

var logger Logger = nil

func SetLogger(log Logger)  {
	logger = log
}

func Newlogger(level int) Logger {
	return &slog{Level:level}
}

type slog struct {
	Level int
}

func (s *slog) Debugf(format string, args ...interface{}) {
	if s.Level > LoggerLevelDebug {
		return
	}
	fmt.Println(fmt.Sprintf(
		`[snailx][\x1b[%dm%s\x1b[0m ] %s`,
		32,
		"DEBUG",
		fmt.Sprintf(format, args...),
	))
}

func (s *slog) Infof(format string, args ...interface{}) {
	if s.Level > LoggerLevelInfo {
		return
	}
	fmt.Println(fmt.Sprintf(
		`[snailx][\x1b[%dm%s\x1b[0m ] %s`,
		36,
		"INFO",
		fmt.Sprintf(format, args...),
	))
}

func (s *slog) Warnf(format string, args ...interface{}) {
	if s.Level > LoggerLevelWarn {
		return
	}
	fmt.Println(fmt.Sprintf(
		`[snailx][\x1b[%dm%s\x1b[0m ] %s`,
		33,
		"WARN",
		fmt.Sprintf(format, args...),
	))
}

func (s *slog) Errorf(format string, args ...interface{}) {
	if s.Level > LoggerLevelError {
		return
	}
	fmt.Println(fmt.Sprintf(
		`[snailx][\x1b[%dm%s\x1b[0m ] %s`,
		31,
		"ERROR",
		fmt.Sprintf(format, args...),
	))
}

func (s *slog) Fatalf(format string, args ...interface{}) {
	if s.Level > LoggerLevelFatal {
		return
	}
	fmt.Println(fmt.Sprintf(
		`[snailx][\x1b[%dm%s\x1b[0m ] %s`,
		31,
		"ERROR",
		fmt.Sprintf(format, args...),
	))
	os.Exit(2)
}
