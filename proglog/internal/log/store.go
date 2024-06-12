package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	errBinWrite := binary.Write(&s.buf, enc, uint64(len(p)))
	if errBinWrite != nil {
		return nil, nil, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return nil, nil, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	errFlush := s.buf.Flush()
	if errFlush != nil {
		return nil, errFlush
	}

	size := make([]byte, lenWidth)
	_, errFileRead := s.File.ReadAt(size, int64(pos))
	if errFileRead != nil {
		return nil, errFileRead
	}

	b := make([]byte, enc.Uint64(size))
	_, errRecRead := s.File.ReadAt(b, int64(pos+lenWidth))
	if errRecRead != nil {
		return nil, errRecRead
	}
	return b, nil
}

// ReadAt implementing the io.ReaderAt to the store
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off) // reads len(p) bytes` into p, beginning at the offset.
}

// Close persist all the buffered data before it close the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
