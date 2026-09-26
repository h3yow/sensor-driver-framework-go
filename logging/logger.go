// Package logging 提供框架统一使用的轻量结构化日志接口。
//
// 设计目标：仅依赖标准库；输出 key=value 形式的单行文本，
// 包含统一时间戳与级别，便于在本地测试中稳定断言关键事件。
package logging

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Logger 是框架内部使用的日志接口。keyvals 为成对出现的键值。
type Logger interface {
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
	Debug(msg string, keyvals ...any)
}

// Options 控制日志输出。
type Options struct {
	Writer io.Writer
	Prefix string
	Debug  bool
}

type textLogger struct {
	mu     sync.Mutex
	w      io.Writer
	prefix string
	debug  bool
}

// New 创建一个写入 opts.Writer 的文本日志器；Writer 为 nil 时使用标准错误。
func New(opts Options) Logger {
	w := opts.Writer
	if w == nil {
		w = os.Stderr
	}
	return &textLogger{w: w, prefix: opts.Prefix, debug: opts.Debug}
}

// Nop 返回一个丢弃所有输出的日志器。
func Nop() Logger {
	return &textLogger{w: io.Discard}
}

func (l *textLogger) log(level, msg string, keyvals []any) {
	if level == "DEBUG" && !l.debug {
		return
	}
	// 收集键值并按键排序，保证重复运行输出稳定。
	type kv struct{ k, v string }
	pairs := make([]kv, 0, len(keyvals)/2+1)
	for i := 0; i+1 < len(keyvals); i += 2 {
		pairs = append(pairs, kv{k: fmt.Sprint(keyvals[i]), v: fmt.Sprint(keyvals[i+1])})
	}
	if len(keyvals)%2 == 1 {
		pairs = append(pairs, kv{k: "ORPHAN", v: fmt.Sprint(keyvals[len(keyvals)-1])})
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })

	var b strings.Builder
	b.WriteString(time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	b.WriteString(" ")
	b.WriteString(level)
	if l.prefix != "" {
		b.WriteString(" [")
		b.WriteString(l.prefix)
		b.WriteString("]")
	}
	b.WriteString(" ")
	b.WriteString(msg)
	for _, p := range pairs {
		b.WriteString(" ")
		b.WriteString(p.k)
		b.WriteString("=")
		b.WriteString(p.v)
	}
	b.WriteString("\n")

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = io.WriteString(l.w, b.String())
}

func (l *textLogger) Info(msg string, keyvals ...any)  { l.log("INFO", msg, keyvals) }
func (l *textLogger) Warn(msg string, keyvals ...any)  { l.log("WARN", msg, keyvals) }
func (l *textLogger) Error(msg string, keyvals ...any) { l.log("ERROR", msg, keyvals) }
func (l *textLogger) Debug(msg string, keyvals ...any) { l.log("DEBUG", msg, keyvals) }
