package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeGrantBatch(t *testing.T) {
	valid := `{"batch_id":"10000000-0000-4000-8000-000000000001","expected_revision":0,"tenants":[{"tenant_id":"10000000-0000-4000-8000-000000000002"}]}`
	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing revision", strings.Replace(valid, `"expected_revision":0,`, "", 1)},
		{"negative revision", strings.Replace(valid, `"expected_revision":0`, `"expected_revision":-1`, 1)},
		{"null revision", strings.Replace(valid, `"expected_revision":0`, `"expected_revision":null`, 1)},
		{"duplicate revision", strings.Replace(valid, `"expected_revision":0`, `"expected_revision":0,"expected_revision":1`, 1)},
		{"duplicate nested field", strings.Replace(valid, `"tenant_id":`, `"tenant_id":null,"tenant_id":`, 1)},
		{"unknown nested field", strings.Replace(valid, `"tenant_id":`, `"owner":`, 1)},
		{"missing tenant id", strings.Replace(valid, `"tenant_id":"10000000-0000-4000-8000-000000000002"`, "", 1)},
		{"wrong case field", strings.Replace(valid, `"tenant_id":`, `"Tenant_id":`, 1)},
		{"empty batch", strings.Replace(valid, `[{"tenant_id":"10000000-0000-4000-8000-000000000002"}]`, `[]`, 1)},
		{"null entry", strings.Replace(valid, `{"tenant_id":"10000000-0000-4000-8000-000000000002"}`, `null`, 1)},
		{"duplicate entry", strings.Replace(valid, `[{"tenant_id":"10000000-0000-4000-8000-000000000002"}]`, `[{"tenant_id":"10000000-0000-4000-8000-000000000002"},{"tenant_id":"10000000-0000-4000-8000-000000000002"}]`, 1)},
		{"zero UUID", strings.Replace(valid, "10000000-0000-4000-8000-000000000002", "00000000-0000-0000-0000-000000000000", 1)},
		{"trailing JSON", valid + "{}"},
		{"invalid UTF-8", strings.Replace(valid, "tenant_id", "tenant_\xffid", 1)},
		{"byte limit", valid + strings.Repeat(" ", maxGrantBatchBytes)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeGrantBatch(strings.NewReader(tc.body))
			require.Error(t, err)
		})
	}
	_, err := decodeGrantBatch(strings.NewReader(valid))
	require.NoError(t, err)
}

func TestDecodeGrantBatchEntryLimit(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, total := range []int{500, 501} {
			t.Run(fmt.Sprintf("split_%t_total_%d", split, total), func(t *testing.T) {
				batch := map[string]any{"batch_id": "20000000-0000-4000-8000-000000000001", "expected_revision": 0}
				tenantCount := total
				if split {
					tenantCount--
					batch["collections"] = []map[string]string{{"tenant_id": "10000000-0000-4000-8000-000000000001", "collection": ""}}
				}
				tenants := make([]map[string]string, 0, tenantCount)
				for index := 1; index <= tenantCount; index++ {
					tenants = append(tenants, map[string]string{"tenant_id": fmt.Sprintf("10000000-0000-4000-8000-%012d", index)})
				}
				batch["tenants"] = tenants
				raw, err := json.Marshal(batch)
				require.NoError(t, err)
				_, err = decodeGrantBatch(bytes.NewReader(raw))
				if total == 500 {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "exceeds 500 entries")
				}
			})
		}
	}
}

func FuzzDecodeGrantBatch(f *testing.F) {
	f.Add([]byte(`{"batch_id":"10000000-0000-4000-8000-000000000001","expected_revision":0,"tenants":[{"tenant_id":"10000000-0000-4000-8000-000000000002"}]}`))
	f.Add([]byte(`{"batch_id":null,"assignments":[{"document_id":[]}]}`))
	f.Add([]byte(`{"expected_revision":0,"expected_revision":1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, err := decodeGrantBatch(bytes.NewReader(data))
		if err != nil {
			return
		}
		var original map[string]any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&original))
		encoded, err := json.Marshal(original)
		require.NoError(t, err)
		reencoded, err := decodeGrantBatch(bytes.NewReader(encoded))
		if len(encoded) > maxGrantBatchBytes {
			require.ErrorContains(t, err, "exceeds 262144 bytes")
			return
		}
		require.NoError(t, err)
		var copyBatch map[string]any
		decoder = json.NewDecoder(bytes.NewReader(reencoded))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&copyBatch))
		require.Equal(t, original, copyBatch)
	})
}
