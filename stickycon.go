package birpc

import (
	"context"
	"errors"
	"github.com/ventu-io/slf"
	"io"
	"net"
	"sync/atomic"
)

/*
   Connection object what exists until explicitly terminated and can be stored somewhere.
   ** ID **
     To allow sticky connection maintained for incoming TCP/TLS connections *id* field used.
     ID must be generated unique for each connection and should be same between reconnects with same thirdparty(host, process)
*/

type StickyConnectionState uint8

const (
	SCON_OFFLINE    StickyConnectionState = iota // all calls will return error in this state, can become online when underlying transport reconnected
	SCON_ONLINE                                  // underlying transport is connected and can be used
	SCON_TERMINATED                              // connection terminated and can't be used anymore
)

type StickyConnection struct {
	id                  string
	state               atomic.Value
	transport           *Transport
	onDisconnect        func(terminate bool) // called when connection indicate connection failure, terminate=true request no more reconnect attempts
	StateChangeListener func(oldState StickyConnectionState, newState StickyConnectionState)
	log                 slf.Logger // logger=StickyConnection, token=$token
}

func newStickyConnection(id string, transport *Transport, onDisconnect func(bool)) *StickyConnection {
	sc := &StickyConnection{id: id,
		log: slf.WithContext("birpc.StickyConnection").WithField("id", id),
	}
	sc.onConnect(transport, onDisconnect)
	return sc
}

func (sc *StickyConnection) State() StickyConnectionState {
	return sc.state.Load().(StickyConnectionState)
}

func (sc *StickyConnection) Id() string {
	return sc.id
}

func (sc *StickyConnection) RemoteAddr() net.Addr {
	return sc.transport.RemoteAddr()
}

func (sc *StickyConnection) LocalAddr() net.Addr {
	return sc.transport.LocalAddr()
}

func (sc *StickyConnection) handleConError(err error) error {
	if _, ok := err.(*RpcConnectionError); ok {
		sc.goDisconnect()
	}
	return err
}

func (sc *StickyConnection) OpenStream(ctx context.Context, path string) (Stream, error) {
	sc.log.Debugf("Open stream %s", path)
	if sc.State() != SCON_ONLINE {
		sc.log.Debug("Not connected")
		return nil, errors.New("Not connected")
	}
	s, err := sc.transport.OpenStream(ctx, path)
	return s, sc.handleConError(err)
}

func (sc *StickyConnection) Call(ctx context.Context, path string, req interface{}, res interface{}) error {
	sc.log.Debugf("Call %s", path)
	if sc.State() != SCON_ONLINE {
		return errors.New("Not connected")
	}
	return sc.handleConError(sc.transport.Call(ctx, path, req, res))
}

func (sc *StickyConnection) CallStream(ctx context.Context, path string, req interface{}) (StreamReader, error) {
	sc.log.Debugf("CallStream %s", path)
	if sc.State() != SCON_ONLINE {
		return nil, errors.New("Not connected")
	}
	s, err := sc.transport.CallStream(ctx, path, req)
	return s, sc.handleConError(err)
}

func (sc *StickyConnection) ProcessStream(ctx context.Context,
	path string,
	req interface{},
	targetMaker func() interface{},
	processor func(target interface{}) error) error {
	stream, err := sc.CallStream(ctx, path, req)
	if stream != nil {
		defer stream.Close()
	}
	if err != nil {
		return sc.handleConError(err)
	}
	for {
		target := targetMaker()
		err := stream.Read(target)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = processor(target)
		if err != nil {
			return err
		}
	}
}

func (sc *StickyConnection) onConError(err error) {
	sc.log.Errorf("Connection error %s", err)
	if _, ok := err.(*RpcConnectionError); ok {
		sc.goDisconnect()
		return
	}
}

func (sc *StickyConnection) onConnect(transport *Transport, onDisconnect func(bool)) {
	sc.log.Debug("Connected")
	sc.transport = transport
	sc.onDisconnect = onDisconnect
	transport.onError = sc.onConError
	sc.state.Store(SCON_ONLINE)
	if sc.StateChangeListener != nil {
		sc.StateChangeListener(SCON_OFFLINE, SCON_ONLINE)
	}
}

func (sc *StickyConnection) goDisconnect() {
	sc.log.Debug("Disconnected")
	sc.state.Store(SCON_OFFLINE)
	sc.onDisconnect(false)
	if sc.StateChangeListener != nil {
		sc.StateChangeListener(SCON_ONLINE, SCON_OFFLINE)
	}
}

// Terminate connection. It can't be used after termination and can be freely deleted
func (sc *StickyConnection) Terminate() {
	if sc.State() == SCON_TERMINATED {
		sc.log.Warn("Trying to terminate already terminated connection")
		return
	}
	sc.log.Info("Terminating")
	prevState := sc.State()
	sc.state.Store(SCON_TERMINATED)
	sc.onDisconnect(true)
	if sc.StateChangeListener != nil {
		sc.StateChangeListener(prevState, SCON_TERMINATED)
	}
}
