package birpc

import (
	"birpc/log"
	"context"
	mockCon "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestStickyConnection(t *testing.T) {
	//Init()
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := func(e error) { log.Debug(e.Error()) }
	mockC := mockCon.NewConn()

	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg1.Add(1)
	go func() {

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
		wg1.Done()
		wg2.Add(1)
		wg2.Wait()
		log.Debug("Closing transport")
		serverTransport.Close()

	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)
	assert.Equal(t, "test", clientTransport.id)
	log.Debugf("Create new con")
	sc := newStickyConnection("test", clientTransport, func(bool) {})
	sc.onConnect(clientTransport, func(bool) {})
	log.Debugf("New con created")

	assert.Equal(t, SCON_ONLINE, sc.State())
	assert.Equal(t, "test", sc.Id())
	assert.Equal(t, "127.0.0.1", sc.LocalAddr().String())
	assert.Equal(t, "127.0.0.1", sc.RemoteAddr().String())
	var res string
	err = sc.Call(context.TODO(), "testpath", "testrequest", &res)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, "testresponse", res)

	wg1.Wait() // wait until server finishes response
	wg2.Done()
	err = sc.Call(context.TODO(), "testpath", "testrequest", &res)
	assert.Error(t, err)
	if _, ok := err.(*RpcConnectionError); !ok {
		t.Errorf("Expecting *RpcConnectionError, actual: %+v", err)
	}
	assert.Equal(t, SCON_OFFLINE, sc.State())
	err = sc.Call(context.TODO(), "testpath", "testrequest", &res)
	assert.Error(t, err)
	assert.Equal(t, "Not connected", err.Error())
	assert.Equal(t, SCON_OFFLINE, sc.State())
}

func TestStickyConnectionCallStream(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := func(e error) { log.Debug(e.Error()) }
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {

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
		wg.Done()

	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)
	assert.Equal(t, "test", clientTransport.id)

	sc := newStickyConnection("test", clientTransport, func(bool) {})
	sc.onConnect(clientTransport, func(bool) {})
	assert.Equal(t, SCON_ONLINE, sc.State())
	assert.Equal(t, "test", sc.Id())
	assert.Equal(t, "127.0.0.1", sc.LocalAddr().String())
	assert.Equal(t, "127.0.0.1", sc.RemoteAddr().String())

	var res string
	stream, err := sc.CallStream(context.TODO(), "testpath", "testrequest")
	if err != nil {
		t.Error(err)
		return
	}
	err = stream.Read(&res)
	assert.NoError(t, err)
	err = stream.Read(&res)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, "testresponse", res)
	assert.Equal(t, SCON_ONLINE, sc.State())
	wg.Wait()
}

func TestStickyConnectionOpenStream(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := func(e error) { log.Debug(e.Error()) }
	mockC := mockCon.NewConn()

	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg1.Add(1)
	wg2.Add(1)
	go func() {

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
		wg1.Done()
		wg2.Wait()
		serverTransport.Close()

	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	assert.NoError(t, err)
	assert.Equal(t, "test", clientTransport.id)

	disconnectCalled := atomic.Value{}
	disconnectCalled.Store(false)
	onDisconnect := func(bool) {
		disconnectCalled.Store(true)
	}

	sc := newStickyConnection("test", clientTransport, onDisconnect)
	sc.onConnect(clientTransport, onDisconnect)
	assert.Equal(t, SCON_ONLINE, sc.State())
	assert.Equal(t, "test", sc.Id())
	assert.Equal(t, "127.0.0.1", sc.LocalAddr().String())
	assert.Equal(t, "127.0.0.1", sc.RemoteAddr().String())

	var res string
	stream, err := sc.OpenStream(context.TODO(), "testpath")
	if err != nil {
		t.Error(err)
		return
	}
	err = stream.Write("testrequest")
	if err != nil {
		t.Error(err)
		return
	}
	err = stream.Read(&res)
	assert.NoError(t, err)
	err = stream.Read(&res)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, "testresponse", res)
	assert.Equal(t, SCON_ONLINE, sc.State())
	wg1.Wait()
	wg2.Done()
	_, err = sc.OpenStream(context.TODO(), "testpath2")
	assert.Error(t, err)
	assert.Equal(t, SCON_OFFLINE, sc.State())
	assert.True(t, disconnectCalled.Load().(bool), "onDisconnect callback was not called")
}

func TestStickyConnectionProcessStream(t *testing.T) {
	rpc, _ := NewRpc(GobEncoding, DEFAULT_CONFIG)
	onE := func(e error) { log.Debug(e.Error()) }
	mockC := mockCon.NewConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {

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
		err = stream.Write("testresponse1")
		assert.NoError(t, err)
		err = stream.Write("testresponse2")
		assert.NoError(t, err)
		stream.Close()
		wg.Done()

	}()

	clientTransport, err := newClientTransport(rpc, onE, mockC.Client)
	if err != nil {
		t.Error(err)
		return
	}
	disconnectCalled := atomic.Value{}
	disconnectCalled.Store(false)
	terminated := atomic.Value{}
	terminated.Store(false)
	disconnectCallsCount := 0
	onDisconnect := func(term bool) {
		disconnectCalled.Store(true)
		disconnectCallsCount += 1
		if term {
			terminated.Store(true)
		}
	}
	sc := newStickyConnection("test", clientTransport, onDisconnect)
	sc.onConnect(clientTransport, onDisconnect)

	var res string
	calls := 0
	err = sc.ProcessStream(context.TODO(), "testpath", "testrequest",
		func() interface{} { return &res },
		func(target interface{}) error {
			assert.Equal(t, "testresponse"+strconv.Itoa(calls+1), res)
			calls += 1
			return nil
		})
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, 2, calls)

	wg.Wait()
	// check callbacks on terminate
	sc.Terminate()
	assert.True(t, disconnectCalled.Load().(bool), "onDisconnect no called on temination")
	assert.True(t, terminated.Load().(bool), "onDisconnect called without terminate flag")
	assert.Equal(t, 1, disconnectCallsCount)
	assert.Equal(t, SCON_TERMINATED, sc.State())
	sc.Terminate()
	assert.Equal(t, 1, disconnectCallsCount)
	assert.Equal(t, SCON_TERMINATED, sc.State())
}
