package main

import (
	"strings"
	"testing"
)

func TestDurabilityPreflightDecisions(t *testing.T) {
	valid := sessionSettings{version: 140000, fsync: true, fullPageWrites: true, commit: "on", timeout: "0", standbyNames: "node"}
	for _, commit := range []string{"off", "local", "remote_write", "on", "remote_apply", "unknown"} {
		for _, mode := range []preflightMode{modeLocal, modeHASession, modeHATopology} {
			s := valid
			s.commit = commit
			want := reason("unsafe_commit_mode")
			switch {
			case mode == modeHATopology:
				want = ""
			case mode == modeLocal && (commit == "local" || commit == "remote_write" || commit == "on" || commit == "remote_apply"):
				want = "primary_settings_valid"
			case mode == modeHASession && (commit == "on" || commit == "remote_apply"):
				want = "ha_session_settings_valid"
			}
			if got := checkSession(mode, s); got != want {
				t.Errorf("%s with %s: %s, want %s", mode, commit, got, want)
			}
		}
	}
	for _, tc := range []struct {
		change func(*sessionSettings)
		want   reason
	}{
		{func(s *sessionSettings) { s.inRecovery = true }, "not_primary"},
		{func(s *sessionSettings) { s.version = 139999 }, "unsupported_server"},
		{func(s *sessionSettings) { s.fsync = false }, "unsafe_wal_settings"},
		{func(s *sessionSettings) { s.fullPageWrites = false }, "unsafe_wal_settings"},
	} {
		s := valid
		tc.change(&s)
		for _, mode := range []preflightMode{modeLocal, modeHASession, modeHATopology} {
			if got := checkSession(mode, s); got != tc.want {
				t.Errorf("%s: %s, want %s", mode, got, tc.want)
			}
		}
	}
	for _, timeout := range []string{"30s", "1ms", "", "00"} {
		s := valid
		s.timeout = timeout
		if got := checkSession(modeHASession, s); got != "nonzero_statement_timeout" {
			t.Errorf("timeout %q: %s", timeout, got)
		}
	}
	s := valid
	s.standbyNames = " \t\r"
	if got := checkSession(modeHASession, s); got != "unsupported_standby_configuration" {
		t.Errorf("empty standby configuration: %s", got)
	}

	physical, logical, streaming, sync, async, backup := "physical", "logical", "streaming", "sync", "async", "backup"
	permanent, temporary := false, true
	good := standbyState{"node", &streaming, &sync, true, &physical, &permanent}
	duplicate := good
	duplicate.name = "NODE"
	for _, tc := range []struct {
		name string
		rows []standbyState
		want reason
	}{
		{"ready", []standbyState{good}, "standby_snapshot_valid"},
		{"absent", nil, "standby_not_ready"},
		{"duplicate", []standbyState{good, good}, "standby_not_ready"},
		{"case variant duplicate", []standbyState{good, duplicate}, "standby_not_ready"},
		{"wrong application spelling", []standbyState{duplicate}, "standby_not_ready"},
		{"masked state", []standbyState{{"node", nil, &sync, true, &physical, &permanent}}, "inspection_unavailable"},
		{"masked sync", []standbyState{{"node", &streaming, nil, true, &physical, &permanent}}, "inspection_unavailable"},
		{"no slot", []standbyState{{"node", &streaming, &sync, true, nil, nil}}, "standby_not_ready"},
		{"logical", []standbyState{{"node", &streaming, &sync, true, &logical, &permanent}}, "standby_not_ready"},
		{"temporary", []standbyState{{"node", &streaming, &sync, true, &physical, &temporary}}, "standby_not_ready"},
		{"backup", []standbyState{{"node", &backup, &sync, true, &physical, &permanent}}, "standby_not_ready"},
		{"async", []standbyState{{"node", &streaming, &async, true, &physical, &permanent}}, "standby_not_ready"},
		{"no flush", []standbyState{{"node", &streaming, &sync, false, &physical, &permanent}}, "standby_not_ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkTopology("node", tc.rows); got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func FuzzDurabilityStandbyConfiguration(f *testing.F) {
	for _, tc := range []struct{ config, expected string }{
		{"node", "node"}, {" nOdE ", "NODE"}, {" \tFiRsT 1 ( node )\n", "NODE"},
		{"1(node)", "node"}, {"_node_1", "_node_1"}, {strings.Repeat("x", 63), strings.Repeat("x", 63)},
		{strings.Repeat(" ", 252) + "node", "node"},
		{strings.Repeat(" ", 253) + "node", ""}, {strings.Repeat("x", 4096), ""},
		{"node\x00", ""}, {"\u00a0node", ""}, {"Kode", ""}, {"*", ""}, {"FIRST 1 (*)", ""},
		{"node, node", ""}, {"FIRST 1 (node, other)", ""}, {"ANY 1 (node)", ""},
		{"\"node\"", ""}, {`"n\"ode"`, ""}, {"FIRST 01 (node)", ""}, {"FIRST1 (node)", ""},
		{"FIRST +1 (node)", ""}, {"2 (node)", ""}, {"first", ""}, {"First 1 (ANY)", ""},
	} {
		if tc.expected != "" {
			if !matchesStandby(tc.config, tc.expected) || matchesStandby(tc.config, "different") {
				f.Fatalf("unexpected result for %q", tc.config)
			}
		} else if matchesStandby(tc.config, "node") || matchesStandby(tc.config, tc.config) {
			f.Fatalf("accepted invalid configuration %q", tc.config)
		}
		f.Add(tc.config, tc.expected)
	}
	f.Fuzz(func(t *testing.T, config, standby string) {
		if !matchesStandby(config, standby) {
			return
		}
		if len(config) > 256 || len(standby) < 1 || len(standby) > 63 || strings.EqualFold(standby, "first") || strings.EqualFold(standby, "any") {
			t.Fatalf("accepted invalid input %q %q", config, standby)
		}
		for i, c := range strings.ToLower(standby) {
			letter := c >= 'a' && c <= 'z'
			digit := c >= '0' && c <= '9'
			if !letter && c != '_' && (i == 0 || !digit) {
				t.Fatalf("accepted invalid name %q", standby)
			}
		}
		if !strings.Contains(strings.ToLower(config), strings.ToLower(standby)) || matchesStandby(config, standby+",other") {
			t.Fatalf("accepted an unverified name %q %q", config, standby)
		}
	})
}
