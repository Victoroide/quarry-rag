package main

import (
	"context"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

type preflightMode string

const (
	modeLocal      preflightMode = "local"
	modeHASession  preflightMode = "ha-session"
	modeHATopology preflightMode = "ha-topology"
)

type reason string

const (
	primarySettingsValid            reason = "primary_settings_valid"
	haSessionSettingsValid          reason = "ha_session_settings_valid"
	standbySnapshotValid            reason = "standby_snapshot_valid"
	notPrimary                      reason = "not_primary"
	unsupportedServer               reason = "unsupported_server"
	unsafeWALSettings               reason = "unsafe_wal_settings"
	unsafeCommitMode                reason = "unsafe_commit_mode"
	nonzeroStatementTimeout         reason = "nonzero_statement_timeout"
	unsupportedStandbyConfiguration reason = "unsupported_standby_configuration"
	standbyNotReady                 reason = "standby_not_ready"
	connectionUnavailable           reason = "connection_unavailable"
	inspectionUnavailable           reason = "inspection_unavailable"
	deadlineOrCancellation          reason = "deadline_or_cancellation"
)

func (r reason) status() (status string, exitCode int) {
	switch r {
	case primarySettingsValid, haSessionSettingsValid, standbySnapshotValid:
		return "pass", 0
	case connectionUnavailable, inspectionUnavailable, deadlineOrCancellation:
		return "unavailable", 1
	default:
		return "fail", 1
	}
}

type sessionSettings struct {
	inRecovery                    bool
	version                       int
	fsync, fullPageWrites         bool
	commit, timeout, standbyNames string
}

type standbyState struct {
	name             string
	state, syncState *string
	hasFlush         bool
	slotType         *string
	temporary        *bool
}

func checkSession(mode preflightMode, s sessionSettings) reason {
	switch {
	case s.inRecovery:
		return notPrimary
	case s.version < 140000:
		return unsupportedServer
	case !s.fsync || !s.fullPageWrites:
		return unsafeWALSettings
	case mode == modeHATopology:
		return ""
	}
	durableRemote := s.commit == "on" || s.commit == "remote_apply"
	if mode == modeLocal {
		if durableRemote || s.commit == "local" || s.commit == "remote_write" {
			return primarySettingsValid
		}
		return unsafeCommitMode
	}
	switch {
	case !durableRemote:
		return unsafeCommitMode
	case s.timeout != "0":
		return nonzeroStatementTimeout
	case strings.TrimSpace(s.standbyNames) == "":
		return unsupportedStandbyConfiguration
	default:
		return haSessionSettingsValid
	}
}

const (
	standbyName = `[A-Za-z_][A-Za-z0-9_]{0,62}`
	asciiSpace  = `[ \t\r\n\f\v]`
)

var (
	standbyNamePattern   = regexp.MustCompile(`^` + standbyName + `$`)
	standbyConfiguration = regexp.MustCompile(`^` + asciiSpace + `*(?:(` + standbyName + `)|(?:[Ff][Ii][Rr][Ss][Tt]` + asciiSpace + `+)?1` + asciiSpace + `*\(` + asciiSpace + `*(` + standbyName + `)` + asciiSpace + `*\))` + asciiSpace + `*$`)
)

func validStandbyName(name string) bool {
	return len(name) <= 63 && standbyNamePattern.MatchString(name) && !strings.EqualFold(name, "first") && !strings.EqualFold(name, "any")
}

func matchesStandby(config, name string) bool {
	if len(config) > 256 || !validStandbyName(name) {
		return false
	}
	fields := standbyConfiguration.FindStringSubmatch(config)
	return len(fields) == 3 && (strings.EqualFold(fields[1], name) || strings.EqualFold(fields[2], name))
}

func checkTopology(name string, rows []standbyState) reason {
	if len(rows) != 1 {
		return standbyNotReady
	}
	row := rows[0]
	if row.state == nil || row.syncState == nil {
		return inspectionUnavailable
	}
	if row.name != name || *row.state != "streaming" || *row.syncState != "sync" || !row.hasFlush ||
		row.slotType == nil || *row.slotType != "physical" || row.temporary == nil || *row.temporary {
		return standbyNotReady
	}
	return standbySnapshotValid
}

func sampleConnection(ctx context.Context, conn *pgx.Conn, mode preflightMode, name string) (reason, error) {
	var s sessionSettings
	err := conn.QueryRow(ctx, `SELECT pg_catalog.pg_is_in_recovery(),
		pg_catalog.current_setting('server_version_num')::integer,
		pg_catalog.current_setting('fsync')::boolean,
		pg_catalog.current_setting('full_page_writes')::boolean,
		pg_catalog.current_setting('synchronous_commit'),
		pg_catalog.current_setting('statement_timeout'),
		pg_catalog.current_setting('synchronous_standby_names')`).Scan(
		&s.inRecovery, &s.version, &s.fsync, &s.fullPageWrites, &s.commit, &s.timeout, &s.standbyNames)
	if err != nil {
		return "", err
	}
	if result := checkSession(mode, s); result != "" {
		return result, nil
	}
	var canInspect bool
	err = conn.QueryRow(ctx, `SELECT pg_catalog.pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')`).Scan(&canInspect)
	if err != nil || !canInspect {
		return inspectionUnavailable, err
	}
	if !matchesStandby(s.standbyNames, name) {
		return unsupportedStandbyConfiguration, nil
	}
	rows, err := conn.Query(ctx, `SELECT r.application_name, r.state, r.sync_state,
		COALESCE(r.flush_lsn > '0/0'::pg_catalog.pg_lsn, FALSE), s.slot_type, s.temporary
		FROM pg_catalog.pg_stat_replication r
		LEFT JOIN pg_catalog.pg_replication_slots s ON s.active_pid = r.pid
		WHERE pg_catalog.lower(r.application_name COLLATE "C") = $1
		LIMIT 2`, strings.ToLower(name))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var states []standbyState
	for rows.Next() {
		var row standbyState
		if err := rows.Scan(&row.name, &row.state, &row.syncState, &row.hasFlush, &row.slotType, &row.temporary); err != nil {
			return "", err
		}
		states = append(states, row)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return checkTopology(name, states), nil
}
