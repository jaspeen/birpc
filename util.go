package birpc

import (
	"encoding/binary"
	"errors"
	"io"
)

// Used to propagate writer errors as RpcError to distinguish encoding errors from connection errors
type ErrorPropagationWriter struct {
	Writer io.Writer
}

func (w *ErrorPropagationWriter) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	if err != nil {
		return n, &RpcConnectionError{Cause: err}
	}
	return n, err
}

type ErrorPropagationReader struct {
	Reader io.Reader
}

func (r *ErrorPropagationReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err != nil && err != io.EOF {
		return n, &RpcConnectionError{Cause: err}
	}
	return n, err
}

// Max string size - 2^16-1
func readString(r io.Reader) (string, error) {
	// read path
	buf := make([]byte, 2)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	size := binary.BigEndian.Uint16(buf)

	if size != 0 {
		path := make([]byte, size)
		_, err = io.ReadFull(r, path)
		if err != nil {
			return "", err
		}
		return string(path), nil
	}
	return "", nil
}

// Max string size - 2^16-1
func writeString(val string, w io.Writer) error {
	size := len(val)
	if size > 0xFFFF {
		return errors.New("Len exeeds 0xFFFF")
	}
	buf := make([]byte, 2) // TODO: allocate array for both size and string
	binary.BigEndian.PutUint16(buf, uint16(size))
	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	_, err = w.Write([]byte(val))
	return err
}
