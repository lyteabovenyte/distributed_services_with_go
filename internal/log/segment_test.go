package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/lyteabovenyte/distributed_services_with_go/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxIndexBytes = 3 * entWidth
	c.Segment.MaxStoreBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, off, 16+i)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxIndexBytes = 1024
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// maxed store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
