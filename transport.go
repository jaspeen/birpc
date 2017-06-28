package birpc

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/jaspeen/birpc/log"
	"github.com/ventu-io/slf"
	"github.com/xtaci/smux"
	"io"
	"net"
	"time"
)

/*
 Transport allow open and accept streams. Transport is not statefull and can fail.
 Client/Server implementation support creating and maintaining transports(including reconnects)
 This particular implementation is around multiplexed TCP/TLS connection
*/
type Transport struct {
	session    *smux.Session
	onError    func(error)
	rpc        *Rpc
	id         string
	localAddr  net.Addr
	remoteAddr net.Addr
	log        slf.Logger
}

func newClientTransport(rpc *Rpc, onError func(error), rawCon net.Conn) (*Transport, error) {
	id, err := readString(rawCon)
	if err != nil {
		return nil, err
	}
	smuxConf := &smux.Config{KeepAliveInterval: rpc.Config.KeepAliveInterval,
		KeepAliveTimeout: rpc.Config.KeepAliveTimeout,
		MaxFrameSize:     4096, MaxReceiveBuffer: 4194304}

	ses, err := smux.Client(rawCon, smuxConf)

	if err != nil {
		return nil, err
	}

	return &Transport{id: id, session: ses, rpc: rpc, onError: onError, localAddr: rawCon.LocalAddr(), remoteAddr: rawCon.RemoteAddr(),
		log: log.WithContext("birpc.Transport").WithField("localAddr", rawCon.LocalAddr()).WithField("remoteAddr", rawCon.RemoteAddr())}, nil
}

func newServerTransport(rpc *Rpc, onError func(error), rawCon net.Conn, id string) (*Transport, error) {
	err := writeString(id, rawCon)

	if err != nil {
		return nil, err
	}

	smuxConf := &smux.Config{KeepAliveInterval: rpc.Config.KeepAliveInterval,
		KeepAliveTimeout: rpc.Config.KeepAliveTimeout,
		MaxFrameSize:     4096, MaxReceiveBuffer: 4194304}

	ses, err := smux.Server(rawCon, smuxConf)

	if err != nil {
		return nil, err
	}

	return &Transport{id: id, session: ses, rpc: rpc, onError: onError, localAddr: rawCon.LocalAddr(), remoteAddr: rawCon.RemoteAddr(),
		log: slf.WithContext("birpc.Transport").WithField("localAddr", rawCon.LocalAddr()).WithField("remoteAddr", rawCon.RemoteAddr())}, nil
}

type streamHeader struct {
	path string
}

var conClosed = errors.New("Connection closed")

func (sh *streamHeader) write(s net.Conn) error {
	var pathSize uint16
	if len(sh.path) > 65535 {
		err := &RpcError{Code: RPC_PROTOCOL_ERROR, Msg: "Path is too big"}
		return err
	}
	pathSize = uint16(len(sh.path))

	pathSizeBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(pathSizeBuf, pathSize)
	_, err := s.Write(pathSizeBuf)
	if err != nil {
		return err
	}
	if pathSize > 0 {
		_, err = s.Write([]byte(sh.path))
		if err != nil {
			return err
		}
	}
	return nil
}

func (sh *streamHeader) read(s net.Conn) error {
	// read path
	buf := make([]byte, 2)
	_, err := io.ReadFull(s, buf)
	if err != nil {
		return err
	}
	pathSize := binary.BigEndian.Uint16(buf)

	if pathSize != 0 {
		path := make([]byte, pathSize)
		_, err = io.ReadFull(s, path)
		if err != nil {
			return err
		}
		sh.path = string(path)
		return nil
	} else {
		// special case indicating what connection closed by other side as correct behaviour
		return conClosed
	}
}

/*
   Block until other side opens stream.
   Client can validate path and send error RPC_NOT_FOUND if path is bad
*/
func (transport *Transport) AcceptStream(ctx context.Context) (Stream, error) {
	s, err := transport.session.AcceptStream()
	if err != nil {
		return nil, &RpcConnectionError{Cause: err}
	}
	transport.log.Debug("Stream accepted, reading header")
	// read path
	header := &streamHeader{}
	err = header.read(s)
	if err != nil {
		if err == conClosed {
			return nil, nil
		}
		transport.log.Debugf("Error reading header: %s", err)
		return nil, &RpcConnectionError{Cause: err}
	}
	return &StreamImpl{path: header.path, con: s, encoder: transport.rpc.encoding.Encoder(s), decoder: transport.rpc.encoding.Decoder(s), onError: transport.onError, config: transport.rpc.Config}, nil
}

/*
   Open stream for specified path.
   Stream operations can be canceled via ctx
*/
func (transport *Transport) OpenStream(ctx context.Context, path string) (Stream, error) {
	transport.log.Debugf("OpenStream %s", path)
	//con.session.SetDeadline(time.Now().Add(con.rpc.config.Timeout))
	s, err := transport.session.OpenStream()
	if err != nil {
		return nil, &RpcConnectionError{Cause: err}
	}
	transport.log.Debug("Stream opened, writing header")
	header := &streamHeader{path: path}
	err = header.write(s)
	if err != nil {
		transport.log.Debugf("Error writing header %s", err)
		s.Close()
		return nil, &RpcConnectionError{Cause: err}
	}
	return &StreamImpl{con: s, encoder: transport.rpc.encoding.Encoder(s), decoder: transport.rpc.encoding.Decoder(s), path: path, onError: transport.onError, config: transport.Config()}, nil
}

func (transport *Transport) Config() *Config {
	return transport.rpc.Config
}

/*
  Closes transport and all underlying connections
*/
func (transport *Transport) Close() error {
	// send close signal
	transport.session.SetDeadline(time.Now().Add(10 * time.Second))
	stream, err := transport.session.OpenStream()
	if err != nil {
		return err
	}
	stream.SetDeadline(time.Now().Add(10 * time.Second))
	header := &streamHeader{}
	header.write(stream)

	stream.Close()
	return transport.session.Close()
}

/*
  Return local address of the transport connection
*/
func (transport *Transport) LocalAddr() net.Addr {
	return transport.localAddr
}

/*
  Return remote address of the transport connection
*/
func (transport *Transport) RemoteAddr() net.Addr {
	return transport.remoteAddr
}

/*
  Simplified method with call semantic. Uses openStream, write request and read response after what closes the stream.
  Should always have request and response. If nil passed to req or res bool placeholder will be used instead.
*/
func (transport *Transport) Call(ctx context.Context, path string, req interface{}, response interface{}) error {
	s, err := transport.OpenStream(ctx, path)
	if s != nil {
		defer s.Close()
	}
	if err != nil {
		return err
	}
	transport.log.Debug("Writing request")
	if req != nil {
		err = s.Write(req)
		if err != nil {
			if err == io.EOF {
				return &RpcError{RPC_PROTOCOL_ERROR, "Unexpected stream close"}
			}
			return err
		}
	}
	transport.log.Debug("Reading response")
	resIsNil := response == nil
	if response == nil {
		var res bool
		response = &res
	}
	err = s.Read(response)
	if err == io.EOF {
		if resIsNil {
			return nil
		}
		return &RpcError{RPC_PROTOCOL_ERROR, "Unexpected stream close"}
	}
	return err
}

/*
  Simplified method with call semantic. Uses openStream, write request and return reader to read sequence of responses.
  User must close reader after completion.
*/
func (transport *Transport) CallStream(ctx context.Context, path string, req interface{}) (StreamReader, error) {
	s, err := transport.OpenStream(ctx, path)
	if err != nil {
		s.Close()
		return nil, err
	}
	if req != nil {
		err := s.Write(req)
		if err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

/*type ConnectionStateListener interface {
	// called when base connection established; should do handshake and return sticky ticket
	OnConnect(con net.Conn) error
	// called when base connection disconnected; ticket generated by OnConnect passed as param
	OnDisconnect(ticket string, err error, con net.Conn) error
}

type ConnectionListener interface {
	handle(ctx context.Context, con net.Conn) error
}*/
