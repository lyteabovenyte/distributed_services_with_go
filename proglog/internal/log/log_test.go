package log

import (
	api "github.com/lyteabovenyte/distributed_services_with_go/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

// defining a table to test the log.
func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempFile("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir.Name())
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err2 := NewLog(dir.Name(), c)
			require.NoError(t, err2)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err2 := log.Read(off)
	require.NoError(t, err2)
	require.Equal(t, append.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off2, err2 := log.HighestOffset()
	require.NoError(t, err2)
	require.Equal(t, uint64(2), off2)

	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, off, uint64(0))

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err2 := proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err2)
	require.Equal(t, read.Value, append.Value)
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
