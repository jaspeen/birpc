package encodingtest

import (
	"bytes"
	"errors"
	"github.com/cavaliercoder/badio"
	"github.com/jaspeen/birpc"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

//TODO: benchmarks

type EncodingTestSuite struct {
	Encoding *birpc.Encoding
}

func (ets *EncodingTestSuite) Run(t *testing.T) {
	t.Run("ShouldReturnConnectionError", ets.ShouldReturnConnectionError)
	t.Run("ShouldReturnConnectionErrorEncode", ets.ShouldReturnConnectionErrorEncode)
	t.Run("ShouldReturnWrongEncoding", ets.ShouldReturnWrongEncoding)
	t.Run("ReadWriteStrings", ets.ReadWriteStrings)
}

func (ets *EncodingTestSuite) ShouldReturnConnectionError(t *testing.T) {
	buf := &bytes.Buffer{}
	err := ets.Encoding.Encoder(buf).Encode("TestString")
	assert.NoError(t, err)
	badReader := badio.NewBreakReader(buf, 5)
	var res string
	err = ets.Encoding.Decoder(badReader).Decode(&res)
	assert.Error(t, err)
	conErr, ok := err.(*birpc.RpcConnectionError)
	assert.True(t, ok, "Expected *RpcConnectionError, actual %+v", err)
	if !ok {
		return
	}
	assert.True(t, badio.IsBadIOError(conErr.Cause))
}

var somethingWrongErr = errors.New("Something wrong")

type BadWriter struct {
}

func (bw *BadWriter) Write(data []byte) (int, error) {
	return 0, somethingWrongErr
}

func (ets *EncodingTestSuite) ShouldReturnConnectionErrorEncode(t *testing.T) {
	err := ets.Encoding.Encoder(&BadWriter{}).Encode("TestString")
	assert.Error(t, err)
	conErr, ok := err.(*birpc.RpcConnectionError)
	assert.True(t, ok, "Expected *RpcConnectionError, actual %+v", err)
	if !ok {
		return
	}
	assert.Equal(t, somethingWrongErr, conErr.Cause)
}

func (ets *EncodingTestSuite) ShouldReturnWrongEncoding(t *testing.T) {
	buf := &bytes.Buffer{}
	err := ets.Encoding.Encoder(buf).Encode("TestString")
	assert.NoError(t, err)
	badReader := badio.NewRandomReader()
	var res string
	err = ets.Encoding.Decoder(badReader).Decode(&res)
	assert.Error(t, err)
	rpcErr, ok := err.(*birpc.RpcError)
	assert.True(t, ok, "Expected *RpcError, actual %+v", err)
	if !ok {
		return
	}
	assert.Equal(t, birpc.RPC_WRONG_ENCODING, rpcErr.Code)
}

func (ets *EncodingTestSuite) ReadWriteStrings(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder := ets.Encoding.Encoder(buf)
	decoder := ets.Encoding.Decoder(buf)
	for i := 0; i < 1000; i++ {
		err := encoder.Encode("TestString" + strconv.Itoa(i))
		if err != nil {
			t.Errorf("Unexpected error: %s", err.Error())
			return
		}
		var res string
		err = decoder.Decode(&res)
		if err != nil {
			t.Errorf("Unexpected error: %s", err.Error())
			return
		}
		assert.Equal(t, "TestString"+strconv.Itoa(i), res)
	}
}
