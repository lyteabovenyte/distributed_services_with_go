package log

import (
	api "github.com/lyteabovenyte/distributed_services_with_go/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "segment-test")
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 3 * entWidth

	want := &api.Record{Value: []byte("hello world")}

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, got.Value, want.Value)
	}

	_, err2 := s.Append(want)
	require.Equal(t, io.EOF, err2)

	// maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err3 := newSegment(dir, 16, c)
	require.NoError(t, err3)

	// maxed store
	require.True(t, s.IsMaxed())

	err4 := s.Remove()
	require.NoError(t, err4)

	s, err5 := newSegment(dir, 16, c)
	require.NoError(t, err5)
	require.False(t, s.IsMaxed())
}
