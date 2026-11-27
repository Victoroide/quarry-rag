package textutil

import (
	"context"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestNormalizeForIngestLineBoundaries(t *testing.T) {
	line := func(offset int) domain.SourceLineBoundary {
		return domain.SourceLineBoundary{Offset: offset, Kind: domain.SourceLineBreak}
	}
	paragraph := func(offset int) domain.SourceLineBoundary {
		return domain.SourceLineBoundary{Offset: offset, Kind: domain.SourceParagraphBreak}
	}
	for _, tc := range []struct {
		raw, text string
		want      []domain.SourceLineBoundary
	}{
		{"Page 3\nOf 48 pallet positions", "Page 3 Of 48 pallet positions", []domain.SourceLineBoundary{line(6)}},
		{"Page 3 Of 48 pallet positions", "Page 3 Of 48 pallet positions", nil},
		{"\n \tＰａge\r\n\t3\rOf\u202848\u2029  cafe\u0301\n \t\nﬁ\u0085\x00P\x00中文\n\t", "Page 3 Of 48\n\ncafé\n\nfi P 中文", []domain.SourceLineBoundary{line(4), line(6), line(9), paragraph(12), paragraph(19)}},
		{"documenta-\n  tion\nwrap", "documentation wrap", []domain.SourceLineBoundary{line(13)}},
		{"\u2000\nalpha\u2000 \n\t\u2001beta\n\n\u2000\n\ngamma\n\u2001", "alpha\u2002 \u2003beta\n\n\n\ngamma", []domain.SourceLineBoundary{line(8), paragraph(16), paragraph(18)}},
		{"\n\t\u2000\n\n", "", nil},
	} {
		got, marks, err := NormalizeForIngestWithLineBoundaries(t.Context(), tc.raw)
		require.NoError(t, err)
		require.Equal(t, tc.text, got, "%q", tc.raw)
		require.Equal(t, NormalizeForIngest(tc.raw), got)
		require.NotNil(t, marks)
		require.Equal(t, len(tc.want), len(marks))
		for i, m := range tc.want {
			require.Equal(t, m, marks[i])
		}
		require.True(t, domain.ValidateSourceLineBoundaries(got, marks))
	}
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelDuringLineCapture{Context: base, cancel: cancel, remaining: 5}
	got, marks, err := NormalizeForIngestWithLineBoundaries(ctx, "word"+strings.Repeat(" ", 1<<20)+"\nnext")
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, got)
	require.Nil(t, marks)
}

type cancelDuringLineCapture struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelDuringLineCapture) Err() error {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func FuzzNormalizeForIngestLineBoundaries(f *testing.F) {
	for _, seed := range []string{"", "a\r\nb\u2000\n\nc", "ﬁ\x00P\x00\u2029", "\xff\xfe\ntext", "docu-\nment\u0085text", "a\n\n\u2001\n\nb"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		got, marks, err := NormalizeForIngestWithLineBoundaries(t.Context(), raw)
		require.NoError(t, err)
		require.Equal(t, NormalizeForIngest(raw), got)
		require.NotNil(t, marks)
		require.True(t, domain.ValidateSourceLineBoundaries(got, marks))
	})
}
