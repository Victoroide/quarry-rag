package ocr

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/stretchr/testify/require"
)

func blk(id string, bt types.BlockType, page int32) types.Block {
	return types.Block{Id: aws.String(id), BlockType: bt, Page: aws.Int32(page)}
}

func blkText(id string, bt types.BlockType, page int32, text string) types.Block {
	b := blk(id, bt, page)
	b.Text = aws.String(text)
	return b
}

func withRels(b types.Block, rels ...types.Relationship) types.Block {
	b.Relationships = append(b.Relationships, rels...)
	return b
}

func rel(rt types.RelationshipType, ids ...string) types.Relationship {
	return types.Relationship{Type: rt, Ids: ids}
}

func bbox(b types.Block, top, left, width, height float32) types.Block {
	b.Geometry = &types.Geometry{BoundingBox: &types.BoundingBox{Top: top, Left: left, Width: width, Height: height}}
	return b
}

func cellBlk(id string, page, row, col, rs, cs int32, entities []types.EntityType, wordIDs ...string) types.Block {
	b := blk(id, types.BlockTypeCell, page)
	b.RowIndex = aws.Int32(row)
	b.ColumnIndex = aws.Int32(col)
	b.RowSpan = aws.Int32(rs)
	b.ColumnSpan = aws.Int32(cs)
	b.EntityTypes = entities
	if len(wordIDs) > 0 {
		b.Relationships = []types.Relationship{rel(types.RelationshipTypeChild, wordIDs...)}
	}
	return b
}

func decodeTimeLimit() time.Duration {
	if raceDetectorEnabled {
		return 2 * time.Second
	}
	return 100 * time.Millisecond
}

// decodeMeasured loads blocks into a fresh graph and decodes it, reporting the
// decode time and the bytes allocated while decoding.
func decodeMeasured(t *testing.T, blocks []types.Block) (LayoutResults, time.Duration, uint64, error) {
	t.Helper()
	g := newAnalysisGraph(1, defaultLayoutBounds())
	require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: blocks}))
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	res, err := g.decode(t.Context())
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	return res, elapsed, after.TotalAlloc - before.TotalAlloc, err
}

// ingestAndDecodeMeasured measures addResponse and decode together, reporting
// total elapsed time and bytes allocated across ingestion and decoding.
func ingestAndDecodeMeasured(t *testing.T, blocks []types.Block) (LayoutResults, time.Duration, uint64, error) {
	t.Helper()
	out := &textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: blocks}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	g := newAnalysisGraph(1, defaultLayoutBounds())
	if err := g.addResponse(out); err != nil {
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		return LayoutResults{}, elapsed, after.TotalAlloc - before.TotalAlloc, err
	}
	res, err := g.decode(t.Context())
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	return res, elapsed, after.TotalAlloc - before.TotalAlloc, err
}

func TestAnalyzePagesIdempotencyZeroDetectionAndPagination(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var detectionStarts, analysisStarts int
		var tokens []string
		acceptedCalls := 0
		getCalls := 0

		api := fakeTextract{
			start: func(context.Context, *textract.StartDocumentTextDetectionInput) (*textract.StartDocumentTextDetectionOutput, error) {
				detectionStarts++
				return nil, errors.New("unexpected detection start")
			},
			startAnalysis: func(_ context.Context, in *textract.StartDocumentAnalysisInput) (*textract.StartDocumentAnalysisOutput, error) {
				analysisStarts++
				require.Equal(t, []types.FeatureType{types.FeatureTypeLayout, types.FeatureTypeTables}, in.FeatureTypes)
				require.Equal(t, "synthetic-bucket", aws.ToString(in.DocumentLocation.S3Object.Bucket))
				require.Equal(t, "doc/original.pdf", aws.ToString(in.DocumentLocation.S3Object.Name))
				tokens = append(tokens, aws.ToString(in.ClientRequestToken))
				return &textract.StartDocumentAnalysisOutput{JobId: aws.String("analysis-job-1")}, nil
			},
			getAnalysis: func(_ context.Context, in *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
				require.Equal(t, "analysis-job-1", aws.ToString(in.JobId))
				getCalls++
				switch getCalls % 4 {
				case 1:
					require.Nil(t, in.NextToken)
					return &textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusInProgress}, nil
				case 2:
					// Split LAYOUT_LIST and TABLE across paginated responses.
					return &textract.GetDocumentAnalysisOutput{
						JobStatus:        types.JobStatusSucceeded,
						DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
						NextToken:        aws.String("page-2"),
						Blocks: []types.Block{
							withRels(blk("p1", types.BlockTypePage, 1), rel(types.RelationshipTypeChild, "lt", "ll", "lp")),
							withRels(blk("lp", types.BlockTypeLayoutPageNumber, 1), rel(types.RelationshipTypeChild, "lpn")),
							blkText("lpn", types.BlockTypeLine, 1, "Page 1 of 1"),
							withRels(bbox(blk("lt", types.BlockTypeLayoutTable, 1), 0.1, 0.1, 0.8, 0.3), rel(types.RelationshipTypeChild, "tl1", "tl2")),
							withRels(bbox(blkText("tl1", types.BlockTypeLine, 1, "Duplicate header line"), 0.1, 0.1, 0.8, 0.05), rel(types.RelationshipTypeChild, "w1")),
							withRels(bbox(blkText("tl2", types.BlockTypeLine, 1, "Duplicate body line"), 0.2, 0.1, 0.8, 0.05), rel(types.RelationshipTypeChild, "w2")),
							withRels(blk("ll", types.BlockTypeLayoutList, 1), rel(types.RelationshipTypeChild, "li1", "li2")),
							withRels(blk("li1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "lil1")),
						},
					}, nil
				case 3:
					require.Equal(t, "page-2", aws.ToString(in.NextToken))
					return &textract.GetDocumentAnalysisOutput{
						JobStatus:        types.JobStatusSucceeded,
						DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
						NextToken:        aws.String("page-3"),
						Blocks: []types.Block{
							blkText("lil1", types.BlockTypeLine, 1, "First item"),
							withRels(blk("li2", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "lil2")),
							blkText("lil2", types.BlockTypeLine, 1, "Second item"),
							withRels(bbox(blk("t1", types.BlockTypeTable, 1), 0.1, 0.1, 0.8, 0.3),
								rel(types.RelationshipTypeTableTitle, "tt1"),
								rel(types.RelationshipTypeChild, "c1", "c2", "c3", "c4"),
								rel(types.RelationshipTypeTableFooter, "tf1")),
							withRels(blk("tt1", types.BlockTypeTableTitle, 1), rel(types.RelationshipTypeChild, "wtitle")),
							withRels(blk("tf1", types.BlockTypeTableFooter, 1), rel(types.RelationshipTypeChild, "wfooter")),
						},
					}, nil
				default:
					require.Equal(t, "page-3", aws.ToString(in.NextToken))
					return &textract.GetDocumentAnalysisOutput{
						JobStatus:        types.JobStatusSucceeded,
						DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
						Blocks: []types.Block{
							blkText("wtitle", types.BlockTypeWord, 1, "Budget Schedule"),
							blkText("wfooter", types.BlockTypeWord, 1, "All amounts in EUR"),
							cellBlk("c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w1"),
							cellBlk("c2", 1, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "w1b"),
							cellBlk("c3", 1, 2, 1, 1, 1, nil, "w2"),
							cellBlk("c4", 1, 2, 2, 1, 1, nil, "w2b"),
							blkText("w1", types.BlockTypeWord, 1, "Item"),
							blkText("w1b", types.BlockTypeWord, 1, "Amount"),
							blkText("w2", types.BlockTypeWord, 1, "Storage"),
							blkText("w2b", types.BlockTypeWord, 1, "420"),
						},
					}, nil
				}
			},
		}

		c := &Client{bucket: "synthetic-bucket", timeout: time.Minute, api: api}
		res, err := c.AnalyzePages(t.Context(), "doc/original.pdf", domain.LayoutPDFProfile, "window-1", 1, func(context.Context) error {
			acceptedCalls++
			return nil
		})
		require.NoError(t, err)
		require.Zero(t, detectionStarts)
		require.Equal(t, 1, analysisStarts)
		require.Equal(t, 1, acceptedCalls)
		require.False(t, res.Partial)

		page := res.Pages[1]
		require.True(t, page.Present)
		require.False(t, page.Failed)
		require.Equal(t, []domain.LayoutSection{
			{Page: 1, TableID: "p1-t1", Text: "Budget Schedule"},
			{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Item: Storage | Amount: 420"},
			{Page: 1, TableID: "p1-t1", Text: "All amounts in EUR"},
			{Page: 1, Text: "First item"},
			{Page: 1, Text: "Second item"},
		}, page.Sections)
		require.NotContains(t, page.Text, "Duplicate header line")
		require.NotContains(t, page.Text, "Page 1 of 1")

		// Simulated restart / retry within the idempotency window reuses the exact token.
		_, err = c.AnalyzePages(t.Context(), "doc/original.pdf", domain.LayoutPDFProfile, "window-1", 1, nil)
		require.NoError(t, err)
		require.Len(t, tokens, 2)
		require.Equal(t, tokens[0], tokens[1])

		// Different window generates a fresh token.
		_, err = c.AnalyzePages(t.Context(), "doc/original.pdf", domain.LayoutPDFProfile, "window-2", 1, nil)
		require.NoError(t, err)
		require.NotEqual(t, tokens[0], tokens[2])
		require.Zero(t, detectionStarts)
	})
}

func TestAnalyzePagesFaultsAndGraphSafety(t *testing.T) {
	t.Run("partial success with warned page", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusPartialSuccess,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(2)},
				Warnings:         []types.Warning{{Pages: []int32{2}}},
				Blocks: []types.Block{
					withRels(blk("p1", types.BlockTypePage, 1), rel(types.RelationshipTypeChild, "lt1")),
					withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l1")),
					blkText("l1", types.BlockTypeLine, 1, "Readable page one text"),
				},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		res, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 2, nil)
		require.NoError(t, err)
		require.True(t, res.Partial)
		require.True(t, res.Pages[1].Present)
		require.False(t, res.Pages[1].Failed)
		require.True(t, res.Pages[2].Failed)
		require.Equal(t, "provider_warning", res.Pages[2].Reason)
	})

	t.Run("missing relationship target is rejected", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				Blocks: []types.Block{
					withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "missing-line")),
				},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		_, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 1, nil)
		require.ErrorIs(t, err, domain.ErrPermanent)
		require.ErrorIs(t, err, ErrDetectionFailed)
	})

	t.Run("missing block ID is rejected", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				Blocks:           []types.Block{{BlockType: types.BlockTypeLine, Page: aws.Int32(1), Text: aws.String("No ID")}},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		_, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 1, nil)
		require.ErrorIs(t, err, ErrDetectionFailed)
	})

	t.Run("cycle in relationships is rejected", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				Blocks: []types.Block{
					withRels(blk("a", types.BlockTypeLayoutList, 1), rel(types.RelationshipTypeChild, "b")),
					withRels(blk("b", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "a")),
				},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		_, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 1, nil)
		require.ErrorIs(t, err, ErrDetectionFailed)
	})

	t.Run("repeated pagination token is rejected", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				NextToken:        aws.String("same-token"),
			}, nil
		}}
		calls := 0
		api.getAnalysis = func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			calls++
			if calls > 3 {
				t.Fatal("pagination loop did not stop")
			}
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				NextToken:        aws.String("same-token"),
			}, nil
		}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		_, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 1, nil)
		require.ErrorIs(t, err, ErrDetectionFailed)
	})

	t.Run("unlinked lines and missing table fallback mark ambiguous order partial", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(2)},
				Blocks: []types.Block{
					bbox(blkText("orphan1", types.BlockTypeLine, 1, "Second"), 0.5, 0.1, 0.2, 0.05),
					bbox(blkText("orphan2", types.BlockTypeLine, 1, "First"), 0.1, 0.1, 0.2, 0.05),
					withRels(blk("lt2", types.BlockTypeLayoutTable, 2), rel(types.RelationshipTypeChild, "tl")),
					blkText("tl", types.BlockTypeLine, 2, "Fallback table text"),
				},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		res, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 2, nil)
		require.NoError(t, err)
		require.True(t, res.Partial)
		require.True(t, res.Pages[1].Partial)
		require.Equal(t, "ambiguous_order", res.Pages[1].Reason)
		require.Equal(t, "First\nSecond", res.Pages[1].Text)
		require.True(t, res.Pages[2].Partial)
		require.Equal(t, "unstructured_table", res.Pages[2].Reason)
		require.Equal(t, "Fallback table text", res.Pages[2].Text)
	})
}

func TestAnalyzePagesBoundsBeforeExpansion(t *testing.T) {
	t.Run("over-limit cell span rejected before grid allocation", func(t *testing.T) {
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				Blocks: []types.Block{
					withRels(blk("t", types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "c")),
					cellBlk("c", 1, 1, 1, 500, 1, nil),
				},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		_, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 1, nil)
		require.ErrorIs(t, err, ErrTextLimit)
	})

	t.Run("over-limit table grid cells rejected before allocation", func(t *testing.T) {
		bounds := defaultLayoutBounds()
		bounds.maxTableCells = 4
		g := newAnalysisGraph(1, bounds)
		err := g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks: []types.Block{
				withRels(blk("t", types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "c")),
				cellBlk("c", 1, 3, 3, 1, 1, nil),
			},
		})
		require.NoError(t, err)
		_, err = g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
	})

	t.Run("over-limit blocks and relationships rejected during collection", func(t *testing.T) {
		bounds := defaultLayoutBounds()
		bounds.maxBlocks = 2
		g := newAnalysisGraph(1, bounds)
		err := g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks: []types.Block{
				blkText("1", types.BlockTypeLine, 1, "a"),
				blkText("2", types.BlockTypeLine, 1, "b"),
				blkText("3", types.BlockTypeLine, 1, "c"),
			},
		})
		require.ErrorIs(t, err, ErrTextLimit)

		bounds = defaultLayoutBounds()
		bounds.maxRelationshipsPerBlock = 2
		g = newAnalysisGraph(1, bounds)
		err = g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks: []types.Block{
				withRels(blk("1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "a", "b", "c")),
			},
		})
		require.ErrorIs(t, err, ErrTextLimit)
	})

	t.Run("over-limit output bytes rejected", func(t *testing.T) {
		bounds := defaultLayoutBounds()
		bounds.maxOutputBytes = 20
		g := newAnalysisGraph(1, bounds)
		err := g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks: []types.Block{
				withRels(blk("lt", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l")),
				blkText("l", types.BlockTypeLine, 1, strings.Repeat("x", 25)),
			},
		})
		require.NoError(t, err)
		_, err = g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
	})

	t.Run("diamond lattice does not expand exponentially", func(t *testing.T) {
		// 31 layers of shared children stays within maxGraphDepth (32).
		const layers = 31
		blocks := make([]types.Block, 0, 2*layers+3)
		blocks = append(blocks, withRels(blk("root", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "n0a", "n0b")))
		for layer := range layers {
			nextA := fmt.Sprintf("n%da", layer+1)
			nextB := fmt.Sprintf("n%db", layer+1)
			if layer == layers-1 {
				nextA, nextB = "leafA", "leafB"
			}
			blocks = append(blocks,
				withRels(blk(fmt.Sprintf("n%da", layer), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, nextA, nextB)),
				withRels(blk(fmt.Sprintf("n%db", layer), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, nextA, nextB)),
			)
		}
		blocks = append(blocks,
			blkText("leafA", types.BlockTypeLine, 1, "a"),
			blkText("leafB", types.BlockTypeLine, 1, "b"),
		)

		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		start := time.Now()
		res, err := g.decode(t.Context())
		require.NoError(t, err)
		require.Less(t, time.Since(start), 100*time.Millisecond)
		require.Equal(t, "a\nb", res.Pages[1].Text)

		// When the deduplicated leaves exceed maxOutputBytes, decode stops with ErrTextLimit.
		tight := defaultLayoutBounds()
		tight.maxOutputBytes = 2
		gTight := newAnalysisGraph(1, tight)
		require.NoError(t, gTight.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		start = time.Now()
		_, err = gTight.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), 100*time.Millisecond)
	})

	t.Run("many-parents fan-in stops at decode-wide output budget", func(t *testing.T) {
		const (
			parents = 244
			lines   = 4096
		)
		lineIDs := make([]string, lines)
		blocks := make([]types.Block, 0, parents+lines)
		lineText := strings.Repeat("x", 64)
		for i := range lines {
			id := fmt.Sprintf("l-%d", i)
			lineIDs[i] = id
			blocks = append(blocks, blkText(id, types.BlockTypeLine, 1, lineText))
		}
		for i := range parents {
			blocks = append(blocks, withRels(blk(fmt.Sprintf("p-%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, lineIDs...)))
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		limit := 100 * time.Millisecond
		if raceDetectorEnabled {
			limit = 2 * time.Second
		}
		start := time.Now()
		_, err := g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), limit)
	})

	t.Run("two-level empty-word fan-in stops at decode-wide visit budget", func(t *testing.T) {
		const (
			lineCount = 122
			wordCount = 4096
		)
		for _, parentCount := range []int{512, 4096} {
			t.Run(fmt.Sprintf("P=%d", parentCount), func(t *testing.T) {
				wordIDs := make([]string, wordCount)
				blocks := make([]types.Block, 0, wordCount+lineCount+parentCount)
				for i := range wordCount {
					id := fmt.Sprintf("w%d", i)
					wordIDs[i] = id
					blocks = append(blocks, blkText(id, types.BlockTypeWord, 1, ""))
				}
				lineIDs := make([]string, lineCount)
				for i := range lineCount {
					id := fmt.Sprintf("l%d", i)
					lineIDs[i] = id
					blocks = append(blocks, withRels(blkText(id, types.BlockTypeLine, 1, ""), rel(types.RelationshipTypeChild, wordIDs...)))
				}
				for i := range parentCount {
					blocks = append(blocks, withRels(blk(fmt.Sprintf("p%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, lineIDs...)))
				}
				g := newAnalysisGraph(1, defaultLayoutBounds())
				require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
					JobStatus: types.JobStatusSucceeded,
					Blocks:    blocks,
				}))
				limit := 100 * time.Millisecond
				if raceDetectorEnabled {
					limit = 2 * time.Second
				}
				start := time.Now()
				_, err := g.decode(t.Context())
				require.ErrorIs(t, err, ErrTextLimit)
				require.Less(t, time.Since(start), limit)
			})
		}
	})

	t.Run("shared merged-cell children hit decode-wide visit budget", func(t *testing.T) {
		const (
			tables      = 100
			mergedCells = 100
			children    = 4096
		)
		childIDs := make([]string, children)
		blocks := make([]types.Block, 0, children+mergedCells+tables)
		for i := range children {
			id := fmt.Sprintf("w-%d", i)
			childIDs[i] = id
			blocks = append(blocks, blkText(id, types.BlockTypeWord, 1, ""))
		}
		mergedIDs := make([]string, mergedCells)
		for i := range mergedCells {
			id := fmt.Sprintf("mc-%d", i)
			mergedIDs[i] = id
			blocks = append(blocks, types.Block{
				Id:            aws.String(id),
				BlockType:     types.BlockTypeMergedCell,
				Page:          aws.Int32(1),
				RowIndex:      aws.Int32(1),
				ColumnIndex:   aws.Int32(1),
				RowSpan:       aws.Int32(1),
				ColumnSpan:    aws.Int32(1),
				Relationships: []types.Relationship{rel(types.RelationshipTypeChild, childIDs...)},
			})
		}
		for i := range tables {
			blocks = append(blocks, withRels(blk(fmt.Sprintf("t-%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeMergedCell, mergedIDs...)))
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		limit := 100 * time.Millisecond
		if raceDetectorEnabled {
			limit = 2 * time.Second
		}
		start := time.Now()
		_, err := g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), limit)
	})

	t.Run("shared list and block-text fan-in hit decode-wide visit budget", func(t *testing.T) {
		const (
			parents   = 200
			lineCount = 100
			wordCount = 2000
		)
		wordIDs := make([]string, wordCount)
		blocks := make([]types.Block, 0, wordCount+lineCount+parents)
		for i := range wordCount {
			id := fmt.Sprintf("lw-%d", i)
			wordIDs[i] = id
			blocks = append(blocks, blkText(id, types.BlockTypeWord, 1, ""))
		}
		lineIDs := make([]string, lineCount)
		for i := range lineCount {
			id := fmt.Sprintf("ll-%d", i)
			lineIDs[i] = id
			blocks = append(blocks, withRels(blkText(id, types.BlockTypeLine, 1, ""), rel(types.RelationshipTypeChild, wordIDs...)))
		}
		for i := range parents {
			blocks = append(blocks, withRels(blk(fmt.Sprintf("list-%d", i), types.BlockTypeLayoutList, 1), rel(types.RelationshipTypeChild, lineIDs...)))
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		limit := 100 * time.Millisecond
		if raceDetectorEnabled {
			limit = 2 * time.Second
		}
		start := time.Now()
		_, err := g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), limit)
	})

	t.Run("shared table subtree hits decode-wide visit budget", func(t *testing.T) {
		const (
			tables = 100
			words  = 1000
		)
		wordIDs := make([]string, words)
		blocks := make([]types.Block, 0, tables+words+1)
		for i := range words {
			id := fmt.Sprintf("w-%d", i)
			wordIDs[i] = id
			blocks = append(blocks, blkText(id, types.BlockTypeWord, 1, "x"))
		}
		blocks = append(blocks, cellBlk("shared-cell", 1, 1, 1, 1, 1, nil, wordIDs...))
		for i := range tables {
			blocks = append(blocks, withRels(blk(fmt.Sprintf("t-%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "shared-cell")))
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		limit := 100 * time.Millisecond
		if raceDetectorEnabled {
			limit = 2 * time.Second
		}
		start := time.Now()
		_, err := g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), limit)
	})

	t.Run("many tables on one page charge document-wide cell budget", func(t *testing.T) {
		blocks := make([]types.Block, 0, 6)
		for i := range 3 {
			tid := fmt.Sprintf("t-%d", i)
			cid := fmt.Sprintf("c-%d", i)
			blocks = append(blocks,
				withRels(blk(tid, types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, cid)),
				cellBlk(cid, 1, 100, 100, 1, 1, nil),
			)
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		limit := 100 * time.Millisecond
		if raceDetectorEnabled {
			limit = 2 * time.Second
		}
		start := time.Now()
		_, err := g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), limit)
	})

	t.Run("table header expansion respects output budget", func(t *testing.T) {
		// 40 cols x 100 rows = 4,000 cells (within maxRelationshipsPerBlock and maxTableCells), repeated headers > 1 MB.
		blocks := []types.Block{blk("tbl", types.BlockTypeTable, 1)}
		var cellIDs []string
		hdrText := strings.Repeat("H", 350)
		for c := int32(1); c <= 40; c++ {
			cid := fmt.Sprintf("h-%d", c)
			wid := fmt.Sprintf("hw-%d", c)
			cellIDs = append(cellIDs, cid)
			blocks = append(blocks,
				cellBlk(cid, 1, 1, c, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, wid),
				blkText(wid, types.BlockTypeWord, 1, hdrText),
			)
		}
		for r := int32(2); r <= 100; r++ {
			for c := int32(1); c <= 40; c++ {
				cid := fmt.Sprintf("d-%d-%d", r, c)
				wid := fmt.Sprintf("dw-%d-%d", r, c)
				cellIDs = append(cellIDs, cid)
				blocks = append(blocks,
					cellBlk(cid, 1, r, c, 1, 1, nil, wid),
					blkText(wid, types.BlockTypeWord, 1, "v"),
				)
			}
		}
		blocks[0] = withRels(blocks[0], rel(types.RelationshipTypeChild, cellIDs...))

		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		limit := 100 * time.Millisecond
		if raceDetectorEnabled {
			limit = 2 * time.Second
		}
		start := time.Now()
		_, err := g.decode(t.Context())
		require.ErrorIs(t, err, ErrTextLimit)
		require.Less(t, time.Since(start), limit)
	})

	t.Run("many tables IoU fallback stays bounded and linear", func(t *testing.T) {
		const count = 200
		blocks := make([]types.Block, 0, count*4)
		for i := range count {
			lt := fmt.Sprintf("lt-%d", i)
			tb := fmt.Sprintf("tb-%d", i)
			c := fmt.Sprintf("c-%d", i)
			w := fmt.Sprintf("w-%d", i)
			top := float32(i) * 0.004
			blocks = append(blocks,
				bbox(blk(lt, types.BlockTypeLayoutTable, 1), top, 0.1, 0.8, 0.003),
				withRels(bbox(blk(tb, types.BlockTypeTable, 1), top, 0.1, 0.8, 0.003), rel(types.RelationshipTypeChild, c)),
				cellBlk(c, 1, 1, 1, 1, 1, nil, w),
				blkText(w, types.BlockTypeWord, 1, fmt.Sprintf("cell-%d", i)),
			)
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    blocks,
		}))
		start := time.Now()
		res, err := g.decode(t.Context())
		require.NoError(t, err)
		require.Less(t, time.Since(start), 100*time.Millisecond)
		require.Len(t, res.Pages[1].Sections, count)
	})

	t.Run("non-followed relationship entries are never rescanned per walk", func(t *testing.T) {
		const (
			parents = 10_000
			lines   = 4
			entries = 4_095
		)
		for _, parentType := range []types.BlockType{types.BlockTypeLayoutTable, types.BlockTypeLayoutText} {
			t.Run(string(parentType), func(t *testing.T) {
				blocks := make([]types.Block, 0, parents+lines+1)
				blocks = append(blocks, blkText("y", types.BlockTypeWord, 1, ""))
				lineIDs := make([]string, lines)
				for k := range lines {
					id := fmt.Sprintf("x%d", k)
					lineIDs[k] = id
					line := blkText(id, types.BlockTypeLine, 1, "")
					for range entries {
						line.Relationships = append(line.Relationships, rel(types.RelationshipTypeValue, "y"))
					}
					blocks = append(blocks, line)
				}
				for i := range parents {
					blocks = append(blocks, withRels(blk(fmt.Sprintf("p%d", i), parentType, 1), rel(types.RelationshipTypeChild, lineIDs...)))
				}
				res, elapsed, _, err := decodeMeasured(t, blocks)
				require.NoError(t, err)
				require.Empty(t, res.Pages[1].Sections)
				require.Less(t, elapsed, decodeTimeLimit())
			})
		}
	})

	t.Run("shared header text is charged across tables", func(t *testing.T) {
		const length = 499_000
		t.Run("header spanning many columns is built once and bounded", func(t *testing.T) {
			blocks := []types.Block{
				blkText("hw", types.BlockTypeWord, 1, strings.Repeat("|", length)),
				cellBlk("h1", 1, 1, 1, 1, 100, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
				cellBlk("h2", 1, 1, 101, 1, 100, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
			}
			for i := range 100 {
				blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "h1", "h2")))
			}
			_, elapsed, allocated, err := decodeMeasured(t, blocks)
			require.ErrorIs(t, err, ErrTextLimit)
			require.Less(t, elapsed, decodeTimeLimit())
			require.Less(t, allocated, uint64(16<<20))
		})
		t.Run("header-only tables that fit the budgets succeed without data rows", func(t *testing.T) {
			blocks := []types.Block{
				blkText("hw", types.BlockTypeWord, 1, strings.Repeat("|", 1_000)),
				cellBlk("h1", 1, 1, 1, 1, 100, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
				cellBlk("h2", 1, 1, 101, 1, 100, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
			}
			for i := range 100 {
				blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "h1", "h2")))
			}
			res, elapsed, allocated, err := decodeMeasured(t, blocks)
			require.NoError(t, err)
			require.Empty(t, res.Pages[1].Sections)
			require.Less(t, elapsed, decodeTimeLimit())
			require.Less(t, allocated, uint64(16<<20))
		})
		t.Run("one header cell shared by many 1x1 tables", func(t *testing.T) {
			blocks := []types.Block{
				blkText("hw", types.BlockTypeWord, 1, strings.Repeat("a", length)),
				cellBlk("h1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "hw"),
			}
			for i := range 1_000 {
				blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "h1")))
			}
			_, elapsed, allocated, err := decodeMeasured(t, blocks)
			require.ErrorIs(t, err, ErrTextLimit)
			require.Less(t, elapsed, decodeTimeLimit())
			require.Less(t, allocated, uint64(16<<20))
		})
		t.Run("one title cell shared by many tables", func(t *testing.T) {
			blocks := []types.Block{
				blkText("tw", types.BlockTypeWord, 1, strings.Repeat("a", length)),
				cellBlk("c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeTableTitle}, "tw"),
			}
			for i := range 1_000 {
				blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "c1")))
			}
			_, elapsed, allocated, err := decodeMeasured(t, blocks)
			require.ErrorIs(t, err, ErrTextLimit)
			require.Less(t, elapsed, decodeTimeLimit())
			require.Less(t, allocated, uint64(16<<20))
		})
		t.Run("one table title target shared by many tables", func(t *testing.T) {
			blocks := []types.Block{blkText("tt", types.BlockTypeLine, 1, strings.Repeat("a", length))}
			for i := range 1_000 {
				blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeTableTitle, "tt")))
			}
			_, elapsed, allocated, err := decodeMeasured(t, blocks)
			require.ErrorIs(t, err, ErrTextLimit)
			require.Less(t, elapsed, decodeTimeLimit())
			require.Less(t, allocated, uint64(16<<20))
		})
	})

	t.Run("overlapping span fills are charged to a decode-wide grid budget", func(t *testing.T) {
		for _, textLen := range []int{0, 64} {
			t.Run(fmt.Sprintf("text=%d", textLen), func(t *testing.T) {
				const cells = 4_096
				blocks := make([]types.Block, 0, 2*cells+2)
				cellIDs := make([]string, cells)
				for i := range cells {
					id := fmt.Sprintf("c%d", i)
					cellIDs[i] = id
					cell := cellBlk(id, 1, 1, 1, 100, 100, nil)
					if textLen > 0 {
						wid := fmt.Sprintf("w%d", i)
						blocks = append(blocks, blkText(wid, types.BlockTypeWord, 1, strings.Repeat("x", textLen)))
						cell.Relationships = []types.Relationship{rel(types.RelationshipTypeChild, wid)}
					}
					blocks = append(blocks, cell)
				}
				for i := range 2 {
					blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, cellIDs...)))
				}
				_, elapsed, _, err := decodeMeasured(t, blocks)
				require.ErrorIs(t, err, ErrTextLimit)
				require.Less(t, elapsed, decodeTimeLimit())
			})
		}
	})

	t.Run("repeated title targets are read once per table", func(t *testing.T) {
		const edges = 4_000
		title := strings.Repeat("t", 200)
		titleIDs := make([]string, edges)
		for i := range titleIDs {
			titleIDs[i] = "tt"
		}
		blocks := []types.Block{
			blkText("tt", types.BlockTypeLine, 1, title),
			withRels(blk("t", types.BlockTypeTable, 1), rel(types.RelationshipTypeTableTitle, titleIDs...)),
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: blocks}))
		budget := newDecodeBudget(t.Context(), g)
		sections, _, err := g.decodeTable(1, "p1-t1", "t", budget)
		require.NoError(t, err)
		require.Len(t, sections, 1)
		require.Equal(t, title, sections[0].Text)
		require.LessOrEqual(t, budget.textWork, 4*len(title))
	})

	t.Run("per-block entity types are bounded", func(t *testing.T) {
		bounds := defaultLayoutBounds()
		cell := cellBlk("c", 1, 1, 1, 1, 1, make([]types.EntityType, bounds.maxEntityTypesPerBlock+1))
		g := newAnalysisGraph(1, bounds)
		err := g.addResponse(&textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: []types.Block{cell}})
		require.ErrorIs(t, err, ErrTextLimit)
	})

	t.Run("decode never reorders provider relationship slices", func(t *testing.T) {
		blocks := []types.Block{
			withRels(blk("t", types.BlockTypeTable, 1), rel(types.RelationshipTypeMergedCell, "m")),
			withRels(func() types.Block {
				m := blk("m", types.BlockTypeMergedCell, 1)
				m.RowIndex, m.ColumnIndex = aws.Int32(1), aws.Int32(1)
				m.RowSpan, m.ColumnSpan = aws.Int32(2), aws.Int32(2)
				return m
			}(), rel(types.RelationshipTypeChild, "c4", "c3", "c2", "c1")),
			withRels(cellBlk("c1", 1, 1, 1, 1, 1, nil), rel(types.RelationshipTypeChild, "w1")),
			withRels(cellBlk("c2", 1, 1, 2, 1, 1, nil), rel(types.RelationshipTypeChild, "w2")),
			withRels(cellBlk("c3", 1, 2, 1, 1, 1, nil), rel(types.RelationshipTypeChild, "w3")),
			withRels(cellBlk("c4", 1, 2, 2, 1, 1, nil), rel(types.RelationshipTypeChild, "w4")),
			blkText("w1", types.BlockTypeWord, 1, "one"),
			blkText("w2", types.BlockTypeWord, 1, "two"),
			blkText("w3", types.BlockTypeWord, 1, "three"),
			blkText("w4", types.BlockTypeWord, 1, "four"),
		}
		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: blocks}))
		res, err := g.decode(t.Context())
		require.NoError(t, err)
		require.Equal(t, "one two three four | one two three four", res.Pages[1].Sections[0].Text)
		require.Equal(t, []string{"c4", "c3", "c2", "c1"}, blocks[1].Relationships[0].Ids)
	})

	t.Run("legitimate large table decodes within every budget", func(t *testing.T) {
		const (
			rows = 200
			cols = 20
		)
		blocks := []types.Block{blk("tbl", types.BlockTypeTable, 1)}
		cellIDs := make([]string, 0, rows*cols)
		for r := int32(1); r <= rows; r++ {
			for c := int32(1); c <= cols; c++ {
				cid, wid := fmt.Sprintf("c-%d-%d", r, c), fmt.Sprintf("w-%d-%d", r, c)
				cellIDs = append(cellIDs, cid)
				entities := []types.EntityType(nil)
				text := fmt.Sprintf("value %d %d", r, c)
				if r == 1 {
					entities = []types.EntityType{types.EntityTypeColumnHeader}
					text = fmt.Sprintf("column %d", c)
				}
				blocks = append(blocks, cellBlk(cid, 1, r, c, 1, 1, entities, wid), blkText(wid, types.BlockTypeWord, 1, text))
			}
		}
		blocks[0] = withRels(blocks[0], rel(types.RelationshipTypeChild, cellIDs...))
		res, _, _, err := decodeMeasured(t, blocks)
		require.NoError(t, err)
		require.Len(t, res.Pages[1].Sections, rows-1)
		require.Equal(t, "column 1: value 2 1 | column 2: value 2 2", res.Pages[1].Sections[0].Text[:len("column 1: value 2 1 | column 2: value 2 2")])
	})

	t.Run("canceled context stops decode promptly without permanent wrap", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
			return &textract.GetDocumentAnalysisOutput{
				JobStatus:        types.JobStatusSucceeded,
				DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
				Blocks:           []types.Block{blkText("l1", types.BlockTypeLine, 1, "hello")},
			}, nil
		}}
		c := &Client{bucket: "b", timeout: time.Minute, api: api}
		res, err := c.collectAnalysis(ctx, aws.String("job-1"), 1, defaultLayoutBounds())
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, domain.ErrPermanent)
		require.Empty(t, res.Pages)

		g := newAnalysisGraph(1, defaultLayoutBounds())
		require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
			JobStatus: types.JobStatusSucceeded,
			Blocks:    []types.Block{blkText("l1", types.BlockTypeLine, 1, "hello")},
		}))
		_, err = g.decode(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("repeated single-ID followed entries scale linearly", func(t *testing.T) {
		const (
			parents = 64
			entries = 4_096
		)
		for _, tc := range []struct {
			name       string
			parentType types.BlockType
			relType    types.RelationshipType
		}{
			{name: "LAYOUT_TEXT/CHILD", parentType: types.BlockTypeLayoutText, relType: types.RelationshipTypeChild},
			{name: "LAYOUT_TABLE/CHILD", parentType: types.BlockTypeLayoutTable, relType: types.RelationshipTypeChild},
			{name: "TABLE/MERGED_CELL", parentType: types.BlockTypeTable, relType: types.RelationshipTypeMergedCell},
		} {
			t.Run(tc.name, func(t *testing.T) {
				blocks := make([]types.Block, 0, parents+1)
				blocks = append(blocks, blkText("w", types.BlockTypeWord, 1, "x"))
				for i := range parents {
					b := blk(fmt.Sprintf("p%d", i), tc.parentType, 1)
					for range entries {
						b.Relationships = append(b.Relationships, rel(tc.relType, "w"))
					}
					blocks = append(blocks, b)
				}
				_, elapsed, allocated, err := ingestAndDecodeMeasured(t, blocks)
				require.NoError(t, err)
				require.Less(t, elapsed, decodeTimeLimit())
				allocLimit := uint64(64 << 20)
				if raceDetectorEnabled {
					allocLimit = 128 << 20
				}
				require.Less(t, allocated, allocLimit)
			})
		}
	})

	t.Run("shared whitespace provider text is trimmed once at ingestion", func(t *testing.T) {
		const length = 499_000
		t.Run("shared whitespace word in cell loop", func(t *testing.T) {
			const (
				tables  = 4
				entries = 4_096
			)
			for _, tc := range []struct {
				name     string
				text     string
				wantSecs int
			}{
				{name: "all_spaces", text: strings.Repeat(" ", length), wantSecs: 0},
				{name: "trailing_spaces", text: "a" + strings.Repeat(" ", length-1), wantSecs: tables},
			} {
				t.Run(tc.name, func(t *testing.T) {
					blocks := []types.Block{
						blkText("ws", types.BlockTypeWord, 1, tc.text),
						cellBlk("c1", 1, 1, 1, 1, 1, nil, "ws"),
					}
					for i := range tables {
						tb := blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1)
						for range entries {
							tb.Relationships = append(tb.Relationships, rel(types.RelationshipTypeChild, "c1"))
						}
						blocks = append(blocks, tb)
					}
					res, elapsed, _, err := ingestAndDecodeMeasured(t, blocks)
					require.NoError(t, err)
					require.Less(t, elapsed, decodeTimeLimit())
					require.Len(t, res.Pages[1].Sections, tc.wantSecs)
				})
			}
		})

		t.Run("shared whitespace line under many layout blocks", func(t *testing.T) {
			const parents = 40_000
			blocks := make([]types.Block, 0, parents+1)
			blocks = append(blocks, blkText("ln", types.BlockTypeLine, 1, strings.Repeat(" ", length)))
			for i := range parents {
				blocks = append(blocks, withRels(blk(fmt.Sprintf("p%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "ln")))
			}
			res, elapsed, _, err := ingestAndDecodeMeasured(t, blocks)
			require.NoError(t, err)
			require.Empty(t, res.Pages[1].Sections)
			require.Less(t, elapsed, decodeTimeLimit())
		})

		t.Run("shared whitespace word under merged cell", func(t *testing.T) {
			const children = 4_096
			for _, tc := range []struct {
				name string
				text string
			}{
				{name: "all_spaces", text: strings.Repeat(" ", length)},
				{name: "trailing_spaces", text: "a" + strings.Repeat(" ", length-1)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					merged := blk("m1", types.BlockTypeMergedCell, 1)
					merged.RowIndex, merged.ColumnIndex = aws.Int32(1), aws.Int32(1)
					for range children {
						merged.Relationships = append(merged.Relationships, rel(types.RelationshipTypeChild, "c1"))
					}
					blocks := []types.Block{
						blkText("ws", types.BlockTypeWord, 1, tc.text),
						cellBlk("c1", 1, 1, 1, 1, 1, nil, "ws"),
						merged,
						withRels(blk("t0", types.BlockTypeTable, 1), rel(types.RelationshipTypeMergedCell, "m1")),
					}
					_, elapsed, _, err := ingestAndDecodeMeasured(t, blocks)
					if err != nil {
						require.ErrorIs(t, err, ErrTextLimit)
					}
					require.Less(t, elapsed, decodeTimeLimit())
				})
			}
		})
	})
}

func TestLayoutDecodeAndIngestionLinearScaling(t *testing.T) {
	measureAllocsAndBytes := func(fn func()) (float64, uint64) {
		allocs := testing.AllocsPerRun(3, fn)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		fn()
		runtime.ReadMemStats(&after)
		return allocs, after.TotalAlloc - before.TotalAlloc
	}

	t.Run("followed relationship indexing preserves order and scales linearly", func(t *testing.T) {
		build := func(entries int, bt types.BlockType, rt types.RelationshipType) []types.Block {
			b := blk("p", bt, 1)
			for i := range entries {
				if bt == types.BlockTypeLayoutTable && i%2 == 1 {
					b.Relationships = append(b.Relationships, rel(types.RelationshipTypeTable, fmt.Sprintf("t%d", i)))
					continue
				}
				b.Relationships = append(b.Relationships, rel(rt, fmt.Sprintf("c%d", i)))
			}
			return []types.Block{b}
		}

		// Verify interleaved direct order and independence from provider slices.
		ltBlocks := build(6, types.BlockTypeLayoutTable, types.RelationshipTypeChild)
		fr := indexFollowedRels(&ltBlocks[0])
		require.NotNil(t, fr)
		require.Equal(t, []string{"c0", "c2", "c4"}, fr.child)
		require.Equal(t, []string{"c0", "t1", "c2", "t3", "c4", "t5"}, fr.direct)
		ltBlocks[0].Relationships[0].Ids[0] = "mutated"
		require.Equal(t, "c0", fr.child[0])
		require.Equal(t, "c0", fr.direct[0])

		for _, tc := range []struct {
			name string
			bt   types.BlockType
			rt   types.RelationshipType
		}{
			{name: "LAYOUT_TEXT/CHILD", bt: types.BlockTypeLayoutText, rt: types.RelationshipTypeChild},
			{name: "LAYOUT_TABLE/CHILD+TABLE", bt: types.BlockTypeLayoutTable, rt: types.RelationshipTypeChild},
			{name: "TABLE/MERGED_CELL", bt: types.BlockTypeTable, rt: types.RelationshipTypeMergedCell},
			{name: "TABLE/TABLE_TITLE", bt: types.BlockTypeTable, rt: types.RelationshipTypeTableTitle},
			{name: "TABLE/TABLE_FOOTER", bt: types.BlockTypeTable, rt: types.RelationshipTypeTableFooter},
		} {
			t.Run(tc.name, func(t *testing.T) {
				const smallN, bigN = 256, 4096 // 16x
				smallBlocks := build(smallN, tc.bt, tc.rt)
				bigBlocks := build(bigN, tc.bt, tc.rt)
				allocsSmall, bytesSmall := measureAllocsAndBytes(func() {
					_ = indexFollowedRels(&smallBlocks[0])
				})
				allocsBig, bytesBig := measureAllocsAndBytes(func() {
					_ = indexFollowedRels(&bigBlocks[0])
				})
				require.LessOrEqual(t, allocsBig, max(10, 4*allocsSmall))
				require.LessOrEqual(t, bytesBig, 32*max(bytesSmall, 1024))
			})
		}
	})

	t.Run("decode accumulators scale linearly across block types", func(t *testing.T) {
		buildGraph := func(n int) []types.Block {
			blocks := make([]types.Block, 0, 5*n+2)
			tableChildren := make([]string, 0, n)
			for i := range n {
				lt := fmt.Sprintf("lt-%d", i)
				ln := fmt.Sprintf("ln-%d", i)
				wd := fmt.Sprintf("wd-%d", i)
				blocks = append(blocks,
					withRels(blk(lt, types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, ln)),
					withRels(blk(ln, types.BlockTypeLine, 1), rel(types.RelationshipTypeChild, wd)),
					blkText(wd, types.BlockTypeWord, 1, fmt.Sprintf("word-%d", i)),
				)
				cid := fmt.Sprintf("c-%d", i)
				cwd := fmt.Sprintf("cw-%d", i)
				tableChildren = append(tableChildren, cid)
				blocks = append(blocks,
					cellBlk(cid, 1, int32(i+1), 1, 1, 1, nil, cwd),
					blkText(cwd, types.BlockTypeWord, 1, fmt.Sprintf("cell-%d", i)),
				)
			}
			blocks = append(blocks, withRels(blk("tbl", types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, tableChildren...)))
			return blocks
		}
		const smallN, bigN = 64, 512 // 8x
		smallBlocks := buildGraph(smallN)
		bigBlocks := buildGraph(bigN)
		run := func(blocks []types.Block) {
			g := newAnalysisGraph(1, defaultLayoutBounds())
			require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: blocks}))
			_, err := g.decode(t.Context())
			require.NoError(t, err)
		}
		allocsSmall, bytesSmall := measureAllocsAndBytes(func() { run(smallBlocks) })
		allocsBig, bytesBig := measureAllocsAndBytes(func() { run(bigBlocks) })
		require.LessOrEqual(t, allocsBig, 16*allocsSmall)
		require.LessOrEqual(t, bytesBig, 16*bytesSmall)
	})

	t.Run("shared whitespace text scans scale linearly with fan-in", func(t *testing.T) {
		const length = 100_000
		for _, tc := range []struct {
			name  string
			build func(parents int) []types.Block
		}{
			{
				name: "cell_word_spaces",
				build: func(parents int) []types.Block {
					blocks := []types.Block{
						blkText("ws", types.BlockTypeWord, 1, strings.Repeat(" ", length)),
						cellBlk("c1", 1, 1, 1, 1, 1, nil, "ws"),
					}
					for i := range parents {
						blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeChild, "c1")))
					}
					return blocks
				},
			},
			{
				name: "line_direct_words_spaces",
				build: func(parents int) []types.Block {
					blocks := []types.Block{
						blkText("ws", types.BlockTypeWord, 1, strings.Repeat(" ", length)),
						withRels(blk("ln", types.BlockTypeLine, 1), rel(types.RelationshipTypeChild, "ws")),
					}
					for i := range parents {
						blocks = append(blocks, withRels(blk(fmt.Sprintf("p%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "ln")))
					}
					return blocks
				},
			},
			{
				name: "table_title_default_block_spaces",
				build: func(parents int) []types.Block {
					blocks := []types.Block{
						blkText("ws", types.BlockTypeWord, 1, strings.Repeat(" ", length)),
						withRels(blk("kv", types.BlockTypeKeyValueSet, 1), rel(types.RelationshipTypeChild, "ws")),
					}
					for i := range parents {
						blocks = append(blocks, withRels(blk(fmt.Sprintf("t%d", i), types.BlockTypeTable, 1), rel(types.RelationshipTypeTableTitle, "kv")))
					}
					return blocks
				},
			},
			{
				name: "layout_text_word_trailing_spaces",
				build: func(parents int) []types.Block {
					blocks := []types.Block{
						blkText("ws", types.BlockTypeWord, 1, "a"+strings.Repeat(" ", length-1)),
					}
					for i := range parents {
						blocks = append(blocks, withRels(blk(fmt.Sprintf("p%d", i), types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "ws")))
					}
					return blocks
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				const smallP, bigP = 256, 4096 // 16x
				smallBlocks := tc.build(smallP)
				bigBlocks := tc.build(bigP)
				run := func(blocks []types.Block) {
					g := newAnalysisGraph(1, defaultLayoutBounds())
					require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{JobStatus: types.JobStatusSucceeded, Blocks: blocks}))
					_, err := g.decode(t.Context())
					require.NoError(t, err)
				}
				allocsSmall, bytesSmall := measureAllocsAndBytes(func() { run(smallBlocks) })
				start := time.Now()
				allocsBig, bytesBig := measureAllocsAndBytes(func() { run(bigBlocks) })
				elapsed := time.Since(start)
				require.Less(t, elapsed, 4*decodeTimeLimit()) // covers 4 runs (3 AllocsPerRun + 1 MemStats)
				require.LessOrEqual(t, allocsBig, 32*allocsSmall)
				require.LessOrEqual(t, bytesBig, 32*bytesSmall)
			})
		}
	})
}

func TestAnalyzePagesRotationThresholdIgnoresMinorSkew(t *testing.T) {
	for _, tc := range []struct {
		name        string
		angle       float32
		wantPartial bool
		wantReason  string
	}{
		{name: "small positive skew", angle: 2.5, wantPartial: false},
		{name: "small negative skew", angle: -1.5, wantPartial: false},
		{name: "wraparound near 360", angle: 358.0, wantPartial: false},
		{name: "tilted beyond 10 degrees", angle: 15.0, wantPartial: true, wantReason: "rotated_text"},
		{name: "upside down 180 degrees", angle: 180.0, wantPartial: true, wantReason: "rotated_text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			angle := tc.angle
			b := blkText("l1", types.BlockTypeLine, 1, "Scanned line")
			b.Geometry = &types.Geometry{
				RotationAngle: &angle,
				BoundingBox:   &types.BoundingBox{Top: 0.1, Left: 0.1, Width: 0.5, Height: 0.05},
			}
			lt := withRels(blk("lt1", types.BlockTypeLayoutText, 1), rel(types.RelationshipTypeChild, "l1"))
			g := newAnalysisGraph(1, defaultLayoutBounds())
			require.NoError(t, g.addResponse(&textract.GetDocumentAnalysisOutput{
				JobStatus: types.JobStatusSucceeded,
				Blocks:    []types.Block{lt, b},
			}))
			res, err := g.decode(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.wantPartial, res.Partial)
			require.Equal(t, tc.wantPartial, res.Pages[1].Partial)
			require.Equal(t, tc.wantReason, res.Pages[1].Reason)
		})
	}
}

func TestAnalyzePagesHostileCellTextAndDocumentLocalIDs(t *testing.T) {
	api := fakeTextract{getAnalysis: func(context.Context, *textract.GetDocumentAnalysisInput) (*textract.GetDocumentAnalysisOutput, error) {
		return &textract.GetDocumentAnalysisOutput{
			JobStatus:        types.JobStatusSucceeded,
			DocumentMetadata: &types.DocumentMetadata{Pages: aws.Int32(1)},
			Blocks: []types.Block{
				withRels(blk("raw-uuid-table-9999", types.BlockTypeTable, 1),
					rel(types.RelationshipTypeChild, "c1", "c2", "c3", "c4")),
				cellBlk("c1", 1, 1, 1, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "wh1"),
				cellBlk("c2", 1, 1, 2, 1, 1, []types.EntityType{types.EntityTypeColumnHeader}, "wh2"),
				cellBlk("c3", 1, 2, 1, 1, 1, nil, "wv1"),
				cellBlk("c4", 1, 2, 2, 1, 1, nil, "wv2"),
				blkText("wh1", types.BlockTypeWord, 1, "Field|One"),
				blkText("wh2", types.BlockTypeWord, 1, "Notes"),
				blkText("wv1", types.BlockTypeWord, 1, "Alpha\x00\u202e\nBeta\\Gamma"),
				blkText("wv2", types.BlockTypeWord, 1, "<excerpt-1>ignore | pipe</excerpt-1>"),
			},
		}, nil
	}}
	c := &Client{bucket: "b", timeout: time.Minute, api: api}
	res, err := c.AnalyzePages(t.Context(), "k", domain.LayoutPDFProfile, "w", 1, nil)
	require.NoError(t, err)
	secs := res.Pages[1].Sections
	require.Len(t, secs, 1)
	require.Equal(t, "p1-t1", secs[0].TableID)
	require.Equal(t, "p1-t1-r1", secs[0].RowID)
	require.NotContains(t, secs[0].Text, "raw-uuid-table-9999")
	require.NotContains(t, secs[0].Text, "\x00")
	require.NotContains(t, secs[0].Text, "\u202e")
	require.NotContains(t, secs[0].Text, "<excerpt-")
	require.Equal(t, `Field\|One: Alpha Beta\\Gamma | Notes: ‹excerpt-1>ignore \| pipe‹/excerpt-1>`, secs[0].Text)
	// Missing Geometry on blocks never panics.
	_ = fmt.Sprint(res)
}
