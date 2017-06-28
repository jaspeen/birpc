package birpc

import (
	"birpc/log"
	"context"
	mockCon "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type MockNetwork struct {
	conn net.Conn
}

func (mocknet *MockNetwork) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return mocknet.conn, nil
}

type MockListener struct {
	conn net.Conn
	wg   sync.WaitGroup
}

func (ml *MockListener) Accept() (net.Conn, error) {
	ml.wg.Wait()
	defer ml.wg.Add(1)
	return ml.conn, nil
}
func (ml *MockListener) Close() error { return nil }
func (ml *MockListener) Addr() net.Addr {
	return ml.conn.LocalAddr()
}

func (mocknet *MockNetwork) Listen(network, address string) (net.Listener, error) {
	return &MockListener{mocknet.conn, sync.WaitGroup{}}, nil
}

// create client and server mock networks which should connect eachother via pipes
func newMockNetworks() (*MockNetwork, *MockNetwork) {
	c := mockCon.NewConn()
	return &MockNetwork{c.Client}, &MockNetwork{c.Server}
}

func TestServerServe(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
	}
	clientNetwork, serverNetwork := newMockNetworks()
	srv := newServer(rpc, "test", serverNetwork)
	srv.RegisterHandler("testpath", func(ctx context.Context, stream Stream) error {
		log.Debug("Handler called")
		var req string
		err := stream.Read(&req)
		assert.NoError(t, err)
		log.Debug("Start writing response")
		err = stream.Write("response")
		assert.NoError(t, err)
		if err == nil {
			log.Debug("Response writing succesfully")
		}
		return nil
	})
	cancelled := atomic.Value{}
	cancelled.Store(false)
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		srv.Serve(ctx, "tcp", "ServerAddress")
		cancelled.Store(true)
	}()
	transport, err := newClientTransport(rpc, func(error) {}, clientNetwork.conn)
	if err != nil {
		t.Error(err)
	}
	var res string
	err = transport.Call(context.TODO(), "testpath", "testreq", &res)
	assert.NoError(t, err)
	cancel()
	time.Sleep(3 * time.Second)
	assert.True(t, cancelled.Load().(bool), "Not properly cancelled")
	//time.Sleep(3*time.Second)
}

func TestServerConnect(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
	}
	clientNetwork, serverNetwork := newMockNetworks()
	srv := newServer(rpc, "test", serverNetwork)
	srv.RegisterHandler("testpath", func(ctx context.Context, stream Stream) error {
		log.Debug("Handler called")
		var req string
		err := stream.Read(&req)
		assert.NoError(t, err)
		log.Debug("Start writing response")
		err = stream.Write("response")
		assert.NoError(t, err)
		if err == nil {
			log.Debug("Response writing succesfully")
		}
		return nil
	})
	cancelled := atomic.Value{}
	cancelled.Store(false)
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		srv.Connect(ctx, "tcp", "testaddress")
		cancelled.Store(true)
	}()
	transport, err := newClientTransport(rpc, func(error) {}, clientNetwork.conn)
	if err != nil {
		t.Error(err)
	}
	var res string
	err = transport.Call(context.TODO(), "testpath", "testreq", &res)
	assert.NoError(t, err)
	cancel()
	time.Sleep(3 * time.Second)
	assert.True(t, cancelled.Load().(bool), "Not properly cancelled")
	//time.Sleep(3*time.Second)
}

func TestServerFuncHandler(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
	}
	clientNetwork, serverNetwork := newMockNetworks()
	srv := newServer(rpc, "test", serverNetwork)
	srv.RegisterHandler("testpath", FuncHandler(func(req string, res *string) error {
		assert.Equal(t, "testreq", req)
		*res = "testresponse"
		return nil
	}))

	go func() {
		srv.Connect(context.TODO(), "tcp", "testaddress")
	}()
	transport, err := newClientTransport(rpc, func(error) {}, clientNetwork.conn)
	if err != nil {
		t.Error(err)
	}
	var res string
	err = transport.Call(context.TODO(), "testpath", "testreq", &res)
	assert.NoError(t, err)
	assert.Equal(t, "testresponse", res)
}
