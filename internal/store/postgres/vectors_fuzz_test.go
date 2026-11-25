package postgres

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func FuzzVectorQueryInputs(f *testing.F) {
	validID := "00000000-0000-4000-8000-000000000001"
	otherID := "00000000-0000-4000-8000-000000000002"
	f.Add("report", "archive", validID+","+otherID, int32(10), int32(50), []byte{0, 0, 128, 63, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add("", "", "", int32(0), int32(0), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add("report", "", "invalid,"+validID+","+validID, int32(5), int32(1), []byte{0, 0, 192, 127, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add("", "collection' OR 1=1--", strings.ToUpper(validID), int32(-1), int32(100), []byte{0, 0, 128, 127, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add("note", "rare", "invalid-only", int32(10001), int32(5), []byte{0, 0, 128, 63, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, docType, collection, rawIDs string, topKRaw, scopeMaxRaw int32, vecBytes []byte) {
		if len(docType) > 256 || len(collection) > 256 || len(rawIDs) > 4096 || len(vecBytes) > 512 {
			t.Skip()
		}

		// Reconstruct float32 slice from fuzzed bytes.
		vecLen := len(vecBytes) / 4
		embedding := make([]float32, vecLen)
		for i := range vecLen {
			embedding[i] = math.Float32frombits(binary.LittleEndian.Uint32(vecBytes[i*4 : (i+1)*4]))
		}

		const dims = 3
		literal, vecErr := vectorLiteral(embedding, dims)
		validVec := len(embedding) == dims
		nonzero := false
		for _, v := range embedding {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				validVec = false
			}
			if v != 0 {
				nonzero = true
			}
		}
		if !validVec || !nonzero {
			require.Error(t, vecErr)
			return
		}
		require.NoError(t, vecErr)
		require.True(t, strings.HasPrefix(literal, "[") && strings.HasSuffix(literal, "]"))

		var docIDs []string
		if rawIDs != "<nil>" {
			if rawIDs == "" {
				docIDs = []string{}
			} else {
				docIDs = strings.Split(rawIDs, ",")
			}
		}
		filter := domain.VectorFilter{
			DocType:     docType,
			Collection:  collection,
			DocumentIDs: docIDs,
		}
		topK := int(topKRaw % 10002)
		scopeMax := int(scopeMaxRaw%1000) + 1
		if scopeMax < 1 {
			scopeMax = 1
		}

		probeQ, probeArgs, probeOK := probeSQL("chunk_vectors", scopeMax+1, filter)
		exactQ, exactArgs, exactOK := exactSQL("chunk_vectors", literal, topK, scopeMax+1, filter)
		searchQ, searchArgs, searchOK := searchSQL("chunk_vectors", literal, topK, filter, true)
		legacyQ, legacyArgs, legacyOK := searchSQL("chunk_vectors", literal, topK, filter, false)

		hasValidUUID := false
		if filter.DocumentIDs != nil {
			for _, id := range filter.DocumentIDs {
				if parsed, err := uuid.Parse(id); err == nil && parsed.String() == id {
					hasValidUUID = true
					break
				}
			}
		}
		wantMatch := filter.DocumentIDs == nil || hasValidUUID
		require.Equal(t, wantMatch, probeOK)
		require.Equal(t, wantMatch, exactOK)
		require.Equal(t, wantMatch, searchOK)
		require.Equal(t, wantMatch, legacyOK)

		if !wantMatch {
			return
		}

		// Verify all filter predicates are parameterized consistently across paths.
		for _, q := range []string{probeQ, exactQ, searchQ, legacyQ} {
			if filter.DocType != "" {
				require.Contains(t, q, "v.doc_type=$")
			}
			if filter.Collection != "" {
				require.Contains(t, q, "v.collection=$")
			}
			if filter.DocumentIDs != nil {
				require.Contains(t, q, "v.document_id=ANY($")
			}
			require.Contains(t, q, "d.status='completed'")
		}
		for _, q := range []string{probeQ, exactQ, searchQ} {
			if filter.DocType != "" {
				require.Contains(t, q, "d.doc_type=$")
			}
			if filter.Collection != "" {
				require.Contains(t, q, "d.collection=$")
			}
		}
		require.NotContains(t, legacyQ, "d.doc_type=$")
		require.NotContains(t, legacyQ, "d.collection=$")
		require.Equal(t, searchArgs, legacyArgs)
		// Exact query must isolate candidate selection inside a MATERIALIZED CTE
		// without approximate nearest-neighbor ordering inside candidates.
		require.Contains(t, exactQ, "WITH candidates AS MATERIALIZED")
		require.Contains(t, exactQ, "ORDER BY distance,chunk_id")
		require.Contains(t, exactQ, "ORDER BY r.distance,v.chunk_id")
		require.Equal(t, scopeMax+1, probeArgs[len(probeArgs)-1])
		for i := range probeArgs {
			require.Contains(t, probeQ, fmt.Sprintf("$%d", i+1))
		}
		for i := range exactArgs {
			require.Contains(t, exactQ, fmt.Sprintf("$%d", i+1))
		}
		for i := range searchArgs {
			require.Contains(t, searchQ, fmt.Sprintf("$%d", i+1))
		}
		if topK > 0 {
			require.Equal(t, topK, exactArgs[len(exactArgs)-1])
			require.Equal(t, topK, searchArgs[len(searchArgs)-1])
		}
	})
}
