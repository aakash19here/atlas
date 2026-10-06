package logger

import (
	"context"
	"log/slog"
)

type slogger struct {
	logger *slog.Logger
}

func NewSlog(l *slog.Logger) Logger {
	return &slogger{
		logger: l,
	}
}

func (l *slogger) Debug(ctx context.Context, msg string, args ...any) {
	l.logger.DebugContext(ctx, msg, args...)
}

func (l *slogger) Info(ctx context.Context, msg string, args ...any) {
	l.logger.InfoContext(ctx, msg, args...)
}

func (l *slogger) Warn(ctx context.Context, msg string, args ...any) {
	l.logger.WarnContext(ctx, msg, args...)
}

func (l *slogger) Error(ctx context.Context, msg string, args ...any) {
	l.logger.ErrorContext(ctx, msg, args...)
}

func (l *slogger) With(args ...any) Logger {
	return &slogger{
		logger: l.logger.With(args...),
	}
}
