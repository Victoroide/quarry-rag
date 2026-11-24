package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/google/uuid"
)

type ledgerEntry struct {
	State       string `json:"state"`
	Incarnation string `json:"incarnation,omitempty"`
}

type ledger struct {
	Version        string                 `json:"fixture_version"`
	Target         string                 `json:"target"`
	EndpointDigest string                 `json:"endpoint_sha256"`
	Prefix         string                 `json:"prefix"`
	NamespaceID    string                 `json:"namespace"`
	Receipt        string                 `json:"receipt,omitempty"`
	Entries        map[string]ledgerEntry `json:"entries"`
	path           string
	namespace      evaluation.Namespace
	lock           *os.File
}

func endpointDigest(base string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimRight(base, "/"))))
}

func acquireLedger(path string) (*os.File, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	f, err := root.OpenFile(filepath.Base(path)+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockLedgerFile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquire fixture ledger lock: %w", err)
	}
	return f, nil
}

func (l *ledger) Close() error {
	return l.lock.Close()
}

func createLedger(path string, target targetConfig, ns evaluation.Namespace) (_ *ledger, err error) {
	lock, err := acquireLedger(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	l := &ledger{Version: evaluation.Version, Target: target.Name, EndpointDigest: endpointDigest(target.BaseURL),
		Prefix: ns.Prefix, NamespaceID: ns.ID.String(), Receipt: uuid.NewString(), Entries: make(map[string]ledgerEntry),
		path: path, namespace: ns, lock: lock}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	f, err := root.OpenFile(filepath.Base(path), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := l.save(); err != nil {
		return nil, err
	}
	return l, nil
}

func openLedger(path string, target targetConfig) (_ *ledger, err error) {
	lock, err := acquireLedger(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	l := &ledger{path: path, lock: lock}
	if err := readJSON(path, l); err != nil {
		return nil, err
	}
	ns, ok := evaluation.ParseNamespace(l.Prefix, l.NamespaceID)
	if !ok || l.Version != evaluation.Version || l.Target != target.Name ||
		l.EndpointDigest != endpointDigest(target.BaseURL) || l.Entries == nil ||
		l.Receipt != "" && !evaluation.ValidReceipt(l.Receipt) {
		return nil, errors.New("ledger fixture or target does not match")
	}
	for key, entry := range l.Entries {
		if !slices.ContainsFunc(evaluation.Documents(), func(d evaluation.Document) bool { return d.Key == key }) ||
			entry.State != "pending" && entry.State != "accepted" && entry.State != "deleting" {
			return nil, errors.New("invalid fixture entry in ledger")
		}
		if entry.Incarnation != "" {
			if !evaluation.ValidIncarnation(entry.Incarnation) {
				return nil, errors.New("invalid document incarnation in ledger")
			}
		}
	}
	l.namespace = ns
	return l, nil
}

func (l *ledger) save() error {
	root, err := os.OpenRoot(filepath.Dir(l.path))
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	name := ".evidence-ledger-" + uuid.NewString()
	tmp, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = root.Remove(name)
	}()
	if err := json.NewEncoder(tmp).Encode(l); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return replaceLedgerFile(root, name, filepath.Base(l.path))
}

func (l *ledger) set(key string, entry ledgerEntry) error {
	previous, existed := l.Entries[key]
	if entry.State == "" {
		delete(l.Entries, key)
	} else {
		l.Entries[key] = entry
	}
	if err := l.save(); err != nil {
		if existed {
			l.Entries[key] = previous
		} else {
			delete(l.Entries, key)
		}
		return err
	}
	return nil
}

func (l *ledger) remainingIDs() []string {
	ids := make([]string, 0, len(l.Entries))
	for _, doc := range evaluation.Documents() {
		if _, ok := l.Entries[doc.Key]; ok {
			ids = append(ids, l.namespace.DocumentID(doc.Key))
		}
	}
	return ids
}
