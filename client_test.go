package birpc

import (
	"context"
	"github.com/jaspeen/birpc/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClientServe(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
	}
	clientNetwork, serverNetwork := newMockNetworks()
	client := newClient(rpc, clientNetwork)
	go func() {
		serverTransport, err := newServerTransport(rpc, func(error) {}, serverNetwork.conn, "testid")
		assert.NoError(t, err)

		stream, err := serverTransport.AcceptStream(context.TODO())
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, "testpath", stream.Path())

		var req string
		err = stream.Read(&req)
		assert.NoError(t, err)
		assert.Equal(t, "testrequest", req)
		log.Debug("Start writing response")
		err = stream.Write("response")
		assert.NoError(t, err)
		if err == nil {
			log.Debug("Response writing succesfully")
		}
	}()

	conChan := make(chan *StickyConnection)
	go func() {
		err = client.Serve(context.TODO(), "tcp", "ServerAddress", conChan)

		if err != nil {
			t.Error(err)
		}
	}()
	log.Debug("Waiting for sticky connection from client")
	sc := <-conChan
	log.Debugf("Got sticky connection with id %s", sc.Id())
	var res string
	err = sc.Call(context.TODO(), "testpath", "testrequest", &res)
	assert.NoError(t, err)
}

func TestClientConnect(t *testing.T) {
	rpc, err := NewRpc(GobEncoding, DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
	}
	clientNetwork, serverNetwork := newMockNetworks()
	client := newClient(rpc, clientNetwork)
	go func() {
		serverTransport, err := newServerTransport(rpc, func(error) {}, serverNetwork.conn, "testid")
		assert.NoError(t, err)

		stream, err := serverTransport.AcceptStream(context.TODO())
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, "testpath", stream.Path())

		var req string
		err = stream.Read(&req)
		assert.NoError(t, err)
		assert.Equal(t, "testrequest", req)
		log.Debug("Start writing response")
		err = stream.Write("response")
		assert.NoError(t, err)
		if err == nil {
			log.Debug("Response writing succesfully")
		}
	}()

	sc, err := client.Connect(context.TODO(), "tcp", "ServerAddress")
	if err != nil {
		t.Error(err)
		return
	}
	log.Debugf("Got sticky connection with id %s", sc.Id())
	var res string
	err = sc.Call(context.TODO(), "testpath", "testrequest", &res)
	assert.NoError(t, err)
}
