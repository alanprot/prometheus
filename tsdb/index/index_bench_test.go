package index

import (
	"context"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
)

func BenchmarkWriteIndex(b *testing.B) {
	lbls, err := labels.ReadLabels(filepath.Join("..", "testdata", "20kseries.json"), 5000)
	require.NoError(b, err)
	symbols := map[string]struct{}{}

	for _, lset := range lbls {
		sort.Sort(lset)
		for _, l := range lset {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		}
	}

	var input indexWriterSeriesSlice

	// Generate ChunkMetas for every label set.
	for i, lset := range lbls {
		var metas []chunks.Meta

		for j := 0; j <= (i % 20); j++ {
			metas = append(metas, chunks.Meta{
				MinTime: int64(j * 10000),
				MaxTime: int64((j + 1) * 10000),
				Ref:     chunks.ChunkRef(rand.Uint64()),
				Chunk:   chunkenc.NewXORChunk(),
			})
		}
		input = append(input, &indexWriterSeries{
			labels: lset,
			chunks: metas,
		})
	}

	syms := []string{}
	for s := range symbols {
		syms = append(syms, s)
	}
	sort.Strings(syms)

	sort.Slice(input, func(i, j int) bool {
		return labels.Compare(input[i].labels, input[j].labels) < 0
	})

	var testCases = []struct {
		name  string
		codec SymbolsCodec
	}{
		{
			name:  "obfuscate",
			codec: obfuscateSymbols{},
		},
		{
			name:  "default",
			codec: defaultSymbolsCodec{},
		},
	}

	for _, tt := range testCases {
		b.Run(tt.name, func(b *testing.B) {
			dir := b.TempDir()
			rSymbols := make([]string, 0, len(syms))
			iw, err := NewWriterWithOps(context.Background(), WriterOps{Fn: filepath.Join(dir, indexFilename), SymbolsCodec: tt.codec})
			require.NoError(b, err)
			for _, s := range syms {
				require.NoError(b, iw.AddSymbol(s))
			}

			for i, s := range input {
				require.NoError(b, err)
				require.NoError(b, iw.AddSeries(storage.SeriesRef(i), s.labels, s.chunks...))
			}

			require.NoError(b, iw.Close())

			ir, err := NewFileReaderWithOps(ReadOps{Fn: filepath.Join(dir, indexFilename), SymbolsCodec: tt.codec})
			require.NoError(b, err)
			s := ir.Symbols()
			for s.Next() {
				rSymbols = append(rSymbols, s.At())
			}

			for _, lbls := range lbls[0] {
				ir.Postings(lbls.Name, lbls.Value)
			}
			require.Equal(b, syms, rSymbols)
		})
	}
}

type obfuscateSymbols struct{}

func (obfuscateSymbols) Encode(sym string) []byte {
	c := []byte(sym)
	for i := range c {
		c[i] = c[i] ^ byte(5)
	}
	return c
}

func (obfuscateSymbols) Decode(b []byte) string {
	r := make([]byte, len(b))
	for i := range b {
		r[i] = b[i] ^ byte(5)
	}
	return yoloString(r)
}
