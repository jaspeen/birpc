package birpc

import (
	"context"
	"github.com/jaspeen/birpc/log"
	mockCon "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"
)

var logOnError = func(e error) { log.Debug(e.Error()) }

func TestTransport(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		assert.Equal(t, "127.0.0.1", serverTransport.LocalAddr().String())
		assert.Equal(t, "127.0.0.1", serverTransport.RemoteAddr().String())
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		err = stream.Close()
		assert.NoError(t, err)
		err = serverTransport.Close()
		assert.NoError(t, err)
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1", clientTransport.LocalAddr().String())
	assert.Equal(t, "127.0.0.1", clientTransport.RemoteAddr().String())

	stream, err := clientTransport.OpenStream(context.TODO(), "testpath")
	assert.NoError(t, err)
	assert.Equal(t, "testpath", stream.Path())

	wg.Wait()
}

func TestTransportErrors(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)

		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		stream.sendErrorAndClose(RPC_NOT_FOUND, "path not found")
		time.Sleep(1 * time.Second)
		err = serverTransport.Close()
		assert.NoError(t, err)
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	stream, err := clientTransport.OpenStream(context.TODO(), "testpath")
	assert.NoError(t, err)
	assert.Equal(t, "testpath", stream.Path())

	var something string
	err = stream.Read(&something)

	assert.Error(t, err)
	rpcErr, ok := err.(*RpcError)
	assert.True(t, ok, "Expected *RpcError, actual %+v", err)
	assert.Equal(t, RPC_NOT_FOUND, rpcErr.Code)
	assert.Equal(t, "path not found", rpcErr.Msg)

	err = stream.Read(&something)
	assert.Equal(t, io.EOF, err)

	wg.Wait() // wait until server gorouting finished
}

func TestTransportConnectionCloses(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)

		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.Nil(t, stream)
		assert.Nil(t, err)
		wg2.Wait()
		serverTransport.Close()
	}()
	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)
	wg2.Done()
	clientTransport.Close()

	wg.Wait() // wait until server gorouting finished
}

func TestStreamHeader(t *testing.T) {
	mockC := mockCon.NewConn()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		header := streamHeader{}
		err := header.write(mockC.Client)
		assert.NoError(t, err)
	}()
	anotherHeader := streamHeader{}
	err := anotherHeader.read(mockC.Server)
	assert.Equal(t, conClosed, err)
	wg.Wait()
}

func genString(size int) string {
	res := ""
	for i := 0; i < size; i++ {
		res += "1"
	}
	return res
}

func TestStreamHeaderLongPath(t *testing.T) {
	mockC := mockCon.NewConn()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		header := streamHeader{path: genString(65536)}
		err := header.write(mockC.Client)
		assert.Error(t, err)
		rpcErr, ok := err.(*RpcError)
		assert.True(t, ok, "Expected *RpcError, actual %+v", err)
		assert.Equal(t, RPC_PROTOCOL_ERROR, rpcErr.Code)
	}()

	wg.Wait()
}

func TestTransportCall(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		var req string
		err = stream.Read(&req)
		assert.NoError(t, err)
		assert.Equal(t, "testrequest", req)
		err = stream.Write("testresponse")
		assert.NoError(t, err)
		stream.Close()
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	req := "testrequest"
	var res string

	err = clientTransport.Call(context.TODO(), "testpath", req, &res)
	assert.NoError(t, err)
	assert.Equal(t, "testresponse", res)
	wg.Wait() // wait until server gorouting finished
}

func TestTransportCallCloseCon(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		stream.Close()
	}()
	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)
	req := "testrequest"
	err = clientTransport.Call(context.TODO(), "testpath", req, nil)
	assert.NoError(t, err)
	wg.Wait() // wait until server gorouting finished
}

func TestTransportCallError(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		stream.sendErrorAndClose(RPC_NOT_FOUND, "path not found")
		time.Sleep(1 * time.Second)
		err = serverTransport.Close()
		assert.NoError(t, err)
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	err = clientTransport.Call(context.TODO(), "testpath", nil, nil)
	assert.Error(t, err)
	wg.Wait() // wait until server gorouting finished
}

func TestTransportCallErrorSequence(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		stream.Close()
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	req := "testrequest"
	var res string

	err = clientTransport.Call(context.TODO(), "testpath", req, &res)
	assert.Error(t, err)
	rpcErr, ok := err.(*RpcError)
	if !ok {
		t.Errorf("Expected *RpcError, actual: %+v", err)
		return
	}
	assert.Equal(t, RPC_PROTOCOL_ERROR, rpcErr.Code)
	wg.Wait() // wait until server gorouting finished
}

func TestTransportCallErrorSequenceNoResponse(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		var req string
		err = stream.Read(&req)
		assert.NoError(t, err)
		assert.Equal(t, "testrequest", req)
		stream.Close()
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	req := "testrequest"
	var res string

	err = clientTransport.Call(context.TODO(), "testpath", req, &res)
	assert.Error(t, err)
	rpcErr, ok := err.(*RpcError)
	if !ok {
		t.Errorf("Expected *RpcError, actual: %+v", err)
		return
	}
	assert.Equal(t, RPC_PROTOCOL_ERROR, rpcErr.Code)
	wg.Wait() // wait until server gorouting finished
}

func TestTransportCallNoResponse(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		var req string
		err = stream.Read(&req)
		assert.NoError(t, err)
		assert.Equal(t, "testrequest", req)
		stream.Close()
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	req := "testrequest"

	err = clientTransport.Call(context.TODO(), "testpath", req, nil)
	assert.NoError(t, err)

	wg.Wait() // wait until server gorouting finished
}

func TestTransportCallStream(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := logOnError
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverTransport, err := newServerTransport(rpc, onE, mockC.Server, "test")
		assert.NoError(t, err)
		stream, err := serverTransport.AcceptStream(context.TODO())
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "testpath", stream.Path())
		var req string
		err = stream.Read(&req)
		assert.NoError(t, err)
		assert.Equal(t, "testrequest", req)
		for i := 0; i < 100; i++ {
			err = stream.Write("testresponse" + strconv.Itoa(i))
			if err != nil {
				t.Error(err)
				break
			}
		}
		stream.Close()
	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)

	req := "testrequest"

	s, err := clientTransport.CallStream(context.TODO(), "testpath", req)
	assert.NoError(t, err)

	for i := 0; i < 101; i++ {
		var res string
		err = s.Read(&res)
		if err == io.EOF {
			break
		}
		if i == 100 && err != io.EOF {
			t.Error("EOF expected")
			break
		}
		assert.NoError(t, err)
		assert.Equal(t, "testresponse"+strconv.Itoa(i), res)
	}

	wg.Wait() // wait until server gorouting finished
}
