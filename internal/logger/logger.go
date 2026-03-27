package logger

import (
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	log     *zap.Logger
	logOnce sync.Once
)

// Init initializes the global logger. Call this once at startup.
func Init(env, level string) error {
	var cfg zap.Config
	if env == "production" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}
	cfg.Level = zap.NewAtomicLevelAt(zapLevel)

	// Write to both file and stdout
	ensureLogDir()
	logPath := filepath.Join(projectRoot(), "logs", "app.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// Fallback to stdout only
		zapLogger, err2 := cfg.Build()
		if err2 != nil {
			return err2
		}
		logOnce.Do(func() { log = zapLogger })
		return nil
	}

	writer := zapcore.NewMultiWriteSyncer(
		zapcore.AddSync(file),
		zapcore.AddSync(os.Stdout),
	)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg.EncoderConfig),
		writer,
		zapLevel,
	)
	zapLogger := zap.New(core)
	logOnce.Do(func() { log = zapLogger })
	return nil
}

// Get returns the global logger.
func Get() *zap.Logger {
	if log == nil {
		log, _ = zap.NewDevelopment()
	}
	return log
}

// GetLogger is exported so external packages can call it.
func GetLogger() *zap.Logger {
	return Get()
}

func projectRoot() string {
	exe, _ := os.Executable()
	return filepath.Dir(exe)
}

func ensureLogDir() {
	dir := filepath.Join(projectRoot(), "logs")
	os.MkdirAll(dir, 0755)
}
