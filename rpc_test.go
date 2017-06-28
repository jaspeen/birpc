package birpc

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRpcCreation(t *testing.T) {
	rpc, err := NewRpc(nil, DEFAULT_CONFIG)
	assert.Nil(t, rpc)
	assert.Error(t, err)

	rpc, err = NewRpc(GobEncoding, nil)
	assert.Nil(t, rpc)
	assert.Error(t, err)

	rpc, err = NewRpc(nil, nil)
	assert.Nil(t, rpc)
	assert.Error(t, err)

	rpc, err = NewRpc(GobEncoding, &Config{KeepAliveInterval: 2 * time.Second, KeepAliveTimeout: 1 * time.Second})
	assert.Nil(t, rpc)
	assert.Error(t, err)

	rpc, err = NewRpc(GobEncoding, &Config{KeepAliveInterval: 1 * time.Second, KeepAliveTimeout: 1 * time.Second})
	assert.NotNil(t, rpc)
	assert.NoError(t, err)
}

func TestRpcNewClient(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, &Config{KeepAliveInterval: 1 * time.Second, KeepAliveTimeout: 1 * time.Second})
	assert.NotNil(t, rpc)
	assert.NoError(t, err)

	c := rpc.NewClient()
	assert.NotNil(t, c)
}

func TestRpcNewServer(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, &Config{KeepAliveInterval: 1 * time.Second, KeepAliveTimeout: 1 * time.Second})
	assert.NotNil(t, rpc)
	assert.NoError(t, err)

	s := rpc.NewServer("testid")
	assert.NotNil(t, s)
	assert.Equal(t, "testid", s.stickyToken)
}
