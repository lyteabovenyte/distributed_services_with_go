package log

import (
	"github.com/stretchr/testify/require"
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
