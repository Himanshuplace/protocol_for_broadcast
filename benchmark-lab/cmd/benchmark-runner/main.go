// benchmark-runner is the master CLI for orchestrating protocol benchmark scenarios.
// Usage: benchmark-runner run --protocol udp --scenario B --duration 60s
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/reporter"
)

var (
	cfgFile  string
	logLevel string
)

func main() {
	root := &cobra.Command{
		Use:   "benchmark-runner",
		Short: "Protocol benchmark platform for real-time market data distribution",
	}

	root.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: benchmark.yaml)")
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug|info|warn|error)")

	root.AddCommand(runCmd(), compareCmd(), reportCmd())

	cobra.OnInitialize(func() {
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
		} else {
			viper.AddConfigPath(".")
			viper.SetConfigName("benchmark")
		}
		viper.AutomaticEnv()
		_ = viper.ReadInConfig()
	})

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildLogger() *zap.Logger {
	level := zapcore.InfoLevel
	switch strings.ToLower(logLevel) {
	case "debug":
		level = zapcore.DebugLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := cfg.Build()
	return logger
}

// runCmd runs a single scenario.
func runCmd() *cobra.Command {
	var (
		protocol    string
		scenario    string
		msgSize     int
		duration    time.Duration
		warmup      time.Duration
		receivers   int
		senders     int
		rateLimit   int
		netProfile  string
		bcastStrat  string
		genType     string
		serverAddr  string
		serverPort  int
		output      string
		pgDSN       string
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a single benchmark scenario",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := buildLogger()
			defer logger.Sync() //nolint:errcheck

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			cfg := scenarios.ScenarioConfig{
				Protocol:       protocol,
				Scenario:       scenario,
				MsgSize:        msgSize,
				Duration:       duration,
				WarmupDuration: warmup,
				ReceiverCount:  receivers,
				SenderCount:    senders,
				RateLimit:      rateLimit,
				NetworkProfile: netProfile,
				BroadcastStrat: bcastStrat,
				GeneratorType:  genType,
				ServerAddr:     serverAddr,
				ServerPort:     serverPort,
			}

			runner := scenarios.NewRunner(cfg, logger)
			result, err := runner.Run(ctx)
			if err != nil {
				return fmt.Errorf("run: %w", err)
			}

			// Persist if DSN provided
			if pgDSN != "" {
				c, err := collector.NewPostgresCollector(ctx, pgDSN)
				if err != nil {
					logger.Warn("postgres collector unavailable", zap.Error(err))
				} else {
					defer c.Close()
					if err := c.Store(ctx, result); err != nil {
						logger.Warn("store result failed", zap.Error(err))
					}
				}
			}

			return outputResult(result, output)
		},
	}

	cmd.Flags().StringVarP(&protocol, "protocol", "p", "udp", "transport protocol")
	cmd.Flags().StringVarP(&scenario, "scenario", "s", "A", "scenario (A|B|C|D|E)")
	cmd.Flags().IntVar(&msgSize, "msg-size", 1024, "message payload size in bytes")
	cmd.Flags().DurationVar(&duration, "duration", 60*time.Second, "measurement duration")
	cmd.Flags().DurationVar(&warmup, "warmup", 5*time.Second, "warmup duration (discarded)")
	cmd.Flags().IntVar(&receivers, "receivers", 1, "number of subscriber connections")
	cmd.Flags().IntVar(&senders, "senders", 1, "number of publisher goroutines")
	cmd.Flags().IntVar(&rateLimit, "rate-limit", 0, "max messages/sec (0=flood)")
	cmd.Flags().StringVar(&netProfile, "network-profile", "clean", "network impairment profile")
	cmd.Flags().StringVar(&bcastStrat, "broadcast-strat", "naive", "broadcast strategy")
	cmd.Flags().StringVar(&genType, "generator", "random", "payload generator type")
	cmd.Flags().StringVar(&serverAddr, "addr", "127.0.0.1", "server bind address")
	cmd.Flags().IntVar(&serverPort, "port", 9000, "server base port")
	cmd.Flags().StringVar(&output, "output", "json", "output format (json|markdown|html)")
	cmd.Flags().StringVar(&pgDSN, "store-postgres", "", "PostgreSQL DSN to store results")

	return cmd
}

// compareCmd runs all protocols with the same config and outputs a comparison.
func compareCmd() *cobra.Command {
	var (
		scenario   string
		msgSize    int
		duration   time.Duration
		warmup     time.Duration
		receivers  int
		netProfile string
		output     string
	)

	protocols := []string{
		"udp", "tcp",
		"websocket-gorilla", "websocket-gobwas", "websocket-coder",
		"http1", "http2", "http3",
		"sse", "webtransport",
	}

	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Run all protocols and output a comparison table",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := buildLogger()
			defer logger.Sync() //nolint:errcheck

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			var results []*collector.RunResult
			port := 9000
			for _, proto := range protocols {
				logger.Info("comparing protocol", zap.String("protocol", proto))
				cfg := scenarios.ScenarioConfig{
					Protocol:       proto,
					Scenario:       scenario,
					MsgSize:        msgSize,
					Duration:       duration,
					WarmupDuration: warmup,
					ReceiverCount:  receivers,
					NetworkProfile: netProfile,
					BroadcastStrat: "naive",
					GeneratorType:  "random",
					ServerAddr:     "127.0.0.1",
					ServerPort:     port,
				}
				port += 10

				runner := scenarios.NewRunner(cfg, logger)
				result, err := runner.Run(ctx)
				if err != nil {
					logger.Warn("protocol failed", zap.String("protocol", proto), zap.Error(err))
					continue
				}
				results = append(results, result)

				if ctx.Err() != nil {
					break
				}
			}

			return outputResults(results, output)
		},
	}

	cmd.Flags().StringVarP(&scenario, "scenario", "s", "A", "scenario (A|B|C|D|E)")
	cmd.Flags().IntVar(&msgSize, "msg-size", 1024, "message payload size in bytes")
	cmd.Flags().DurationVar(&duration, "duration", 30*time.Second, "measurement duration per protocol")
	cmd.Flags().DurationVar(&warmup, "warmup", 5*time.Second, "warmup duration")
	cmd.Flags().IntVar(&receivers, "receivers", 10, "number of subscribers")
	cmd.Flags().StringVar(&netProfile, "network-profile", "clean", "network profile")
	cmd.Flags().StringVar(&output, "output", "markdown", "output format (json|markdown|html)")

	return cmd
}

// reportCmd generates a report from stored results.
func reportCmd() *cobra.Command {
	var (
		pgDSN    string
		protocol string
		scenario string
		limit    int
		output   string
		outFile  string
	)

	cmd := &cobra.Command{
		Use:   "report",
		Short: "Generate a report from stored benchmark results",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			c, err := collector.NewPostgresCollector(ctx, pgDSN)
			if err != nil {
				return fmt.Errorf("report: connect postgres: %w", err)
			}
			defer c.Close()

			results, err := c.List(ctx, protocol, scenario, limit)
			if err != nil {
				return fmt.Errorf("report: list: %w", err)
			}

			w := os.Stdout
			if outFile != "" {
				f, err := os.Create(outFile)
				if err != nil {
					return err
				}
				defer f.Close()
				w = f
			}

			var rep reporter.Reporter
			switch output {
			case "markdown":
				rep = reporter.NewMarkdownReporter(w)
			case "html":
				rep = reporter.NewHTMLReporter(w)
			default:
				rep = reporter.NewJSONReporter(w)
			}
			return rep.Report(results)
		},
	}

	cmd.Flags().StringVar(&pgDSN, "dsn", "", "PostgreSQL DSN (required)")
	cmd.Flags().StringVar(&protocol, "protocol", "", "filter by protocol")
	cmd.Flags().StringVar(&scenario, "scenario", "", "filter by scenario")
	cmd.Flags().IntVar(&limit, "limit", 100, "max results")
	cmd.Flags().StringVar(&output, "format", "markdown", "output format (json|markdown|html)")
	cmd.Flags().StringVar(&outFile, "out", "", "output file (default: stdout)")
	cmd.MarkFlagRequired("dsn") //nolint:errcheck

	return cmd
}

func outputResult(result *collector.RunResult, format string) error {
	return outputResults([]*collector.RunResult{result}, format)
}

func outputResults(results []*collector.RunResult, format string) error {
	var rep reporter.Reporter
	switch format {
	case "markdown":
		rep = reporter.NewMarkdownReporter(os.Stdout)
	case "html":
		rep = reporter.NewHTMLReporter(os.Stdout)
	default:
		rep = reporter.NewJSONReporter(os.Stdout)
	}

	if err := rep.Report(results); err != nil {
		// Fallback: print raw JSON to stderr
		enc := json.NewEncoder(os.Stderr)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
		return err
	}
	return nil
}
