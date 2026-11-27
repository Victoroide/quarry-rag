// Command postgres-durability inspects PostgreSQL writer durability settings.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	usage             = "usage: postgres-durability -mode <local|ha-session|ha-topology> [-standby <name>] (DATABASE_URL required)"
	inspectionTimeout = 10 * time.Second
)

type report struct {
	Mode   preflightMode `json:"mode"`
	Status string        `json:"status"`
	Reason reason        `json:"reason"`
}

func main() {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	os.Exit(command())
}

func command() int {
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(root, inspectionTimeout)
	defer cancel()
	return run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr)
}

func run(ctx context.Context, args []string, lookupEnv func(string) (string, bool), stdout, stderr io.Writer) int {
	return runWithInspection(ctx, args, lookupEnv, stdout, stderr, inspect)
}

func runWithInspection(ctx context.Context, args []string, lookupEnv func(string) (string, bool), stdout, stderr io.Writer, sample func(context.Context, string, preflightMode, string) reason) int {
	mode, standby, valid := parseArgs(args)
	dsn, present := lookupEnv("DATABASE_URL")
	if !valid || !present || strings.TrimSpace(dsn) == "" {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	result := deadlineOrCancellation
	if ctx.Err() == nil {
		// pgx reads service, password and TLS files before its transport
		// hooks run. Keep the output path independent of those reads.
		completed := make(chan reason, 1)
		go func() { completed <- sample(ctx, dsn, mode, standby) }()
		select {
		case <-ctx.Done():
		case observed := <-completed:
			if ctx.Err() == nil {
				result = observed
			}
		}
	}
	status, code := result.status()
	if err := json.NewEncoder(stdout).Encode(report{mode, status, result}); err != nil {
		return 1
	}
	return code
}

func parseArgs(args []string) (preflightMode, string, bool) {
	if len(args) != 2 && len(args) != 4 {
		return "", "", false
	}
	var mode preflightMode
	var standby string
	var modeSeen, standbySeen bool
	for i := 0; i < len(args); i += 2 {
		switch args[i] {
		case "-mode":
			if modeSeen {
				return "", "", false
			}
			modeSeen, mode = true, preflightMode(args[i+1])
		case "-standby":
			if standbySeen {
				return "", "", false
			}
			standbySeen, standby = true, args[i+1]
		default:
			return "", "", false
		}
	}
	switch mode {
	case modeLocal, modeHASession:
		return mode, standby, !standbySeen
	case modeHATopology:
		return mode, standby, standbySeen && validStandbyName(standby)
	default:
		return "", "", false
	}
}

func inspect(ctx context.Context, dsn string, mode preflightMode, standby string) reason {
	if ctx.Err() != nil {
		return deadlineOrCancellation
	}
	client, err := postgres.NewClient(ctx, dsn, configureInspectionPool(ctx))
	if err != nil {
		return inspectionError(ctx, err, connectionUnavailable)
	}
	defer func() { _ = client.Close(ctx) }()
	conn, err := client.Pool.Acquire(ctx)
	if err != nil {
		return inspectionError(ctx, err, connectionUnavailable)
	}
	defer conn.Release()
	result, err := sampleConnection(ctx, conn.Conn(), mode, standby)
	if err != nil || ctx.Err() != nil {
		return inspectionError(ctx, err, inspectionUnavailable)
	}
	return result
}

// Use one on-demand connection, with its DNS and sockets bound to the
// inspection context. pgxpool otherwise lets connection startup outlive Acquire.
func configureInspectionPool(inspection context.Context) postgres.Option {
	return func(config *pgxpool.Config) {
		config.MaxConns = 1
		config.MinConns = 0
		config.MinIdleConns = 0
		config.HealthCheckPeriod = inspectionTimeout
		config.MaxConnLifetime = inspectionTimeout
		config.MaxConnLifetimeJitter = 0
		config.MaxConnIdleTime = inspectionTimeout
		config.PingTimeout = 0
		config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
			// DSN parsing may have finished after the command was canceled.
			return inspection.Err()
		}

		lookup, dial := config.ConnConfig.LookupFunc, config.ConnConfig.DialFunc
		config.ConnConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			ctx, cancel := inspectionOperation(ctx, inspection)
			defer cancel()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return lookup(ctx, host)
		}
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			ctx, cancel := inspectionOperation(ctx, inspection)
			defer cancel()
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			stop := context.AfterFunc(inspection, func() { _ = conn.Close() })
			return &inspectionSocket{Conn: conn, stop: stop}, nil
		}
	}
}

func inspectionOperation(ctx, inspection context.Context) (context.Context, context.CancelFunc) {
	operation, stop := context.WithCancel(ctx)
	stopInspection := context.AfterFunc(inspection, stop)
	if inspection.Err() != nil {
		stop()
	}
	return operation, func() { stopInspection(); stop() }
}

type inspectionSocket struct {
	net.Conn
	stop func() bool
}

func (s *inspectionSocket) Close() error {
	s.stop()
	return s.Conn.Close()
}

func inspectionError(ctx context.Context, err error, fallback reason) reason {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return deadlineOrCancellation
	}
	return fallback
}
