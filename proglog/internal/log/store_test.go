package log

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(s, t)
	testRead(s, t)
	testReadAt(s, t)

	s, err2 := newStore(f)
	require.NoError(t, err2)

	testRead(s, t)
}

func testAppend(s *store, t *testing.T) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write) // n is the size of []byte of write
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(s *store, t *testing.T) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, read, write)
		pos += width
	}
}

func testReadAt(s *store, t *testing.T) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, n, lenWidth)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err2 := s.ReadAt(b, off)
		require.NoError(t, err2)
		require.Equal(t, b, write)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err2 := newStore(f)
	require.NoError(t, err2)
	_, _, err3 := s.Append(write)
	require.NoError(t, err3)

	f, beforesize, err4 := openFile(f.Name())
	require.NoError(t, err4)

	err5 := s.Close() // it flushes everything from the buffer to the file
	require.NoError(t, err5)
	_, aftersize, err6 := openFile(f.Name())
	require.NoError(t, err6)

	require.True(t, beforesize < aftersize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_APPEND|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err2 := f.Stat()
	if err2 != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
