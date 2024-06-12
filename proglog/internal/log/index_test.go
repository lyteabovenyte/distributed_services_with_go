package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err2 := idx.Read(-1)
	require.NoError(t, err2)

	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		pos uint64
	}{
		{Off: 0, pos: 0},
		{Off: 1, pos: 10},
	}
	for _, want := range entries {
		err3 := idx.Write(want.Off, want.pos)
		require.NoError(t, err3)
		_, pos, err4 := idx.Read(int64(want.Off))
		require.NoError(t, err4)
		require.Equal(t, pos, want.pos)
	}

	// index and scanner should error reading pas existing entries.
	_, _, err6 := idx.Read(len(entries))
	require.Equal(t, err6, io.EOF)

	// index should build it's state from the existing file
	file, _ := os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err7 := newIndex(file, c)
	require.NoError(t, err7)
	off, pos, err8 := idx.Read(-1)
	require.NoError(t, err8)
	require.Equal(t, entries[1].pos, pos)
	require.Equal(t, uint32(1), off)
}
