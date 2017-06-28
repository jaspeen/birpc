package birpc

import (
	"birpc/log"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

type RpcConnectionError struct {
	Cause error
}

func (rce *RpcConnectionError) Error() string {
	return "Connection error: " + rce.Cause.Error()
}

type RpcStatusCode byte

// RPC error is part of rpc protocol error reporting. All these errors is fatal for stream - stream will be closed after such error
type RpcError struct {
	Code RpcStatusCode
	Msg  string
}

func (e *RpcError) concatMsg(val string) string {
	if e.Msg != "" {
		return val + ": " + e.Msg
	} else {
		return val
	}
}

func (e *RpcError) Error() string {
	val, ok := rpcErrorMap[e.Code]
	if !ok {
		return e.concatMsg("Error #" + strconv.Itoa(int(e.Code)))
	}
	return e.concatMsg(val)
}

const (
	RPC_OK             RpcStatusCode = iota
	RPC_PROTOCOL_ERROR               // wrong protocol; followed by error message and stream close
	RPC_NOT_FOUND                    // received by client if not such path; like Http 404; followed by stream close
	RPC_WRONG_ENCODING               // returned to client when server unable to decode stream message; followed by encoded string with error description and stream close
	RPC_APP_ERROR                    // application specific error; followed by encoded app error object and stream close
)

var rpcErrorMap = map[RpcStatusCode]string{
	RPC_OK:             "Ok",
	RPC_PROTOCOL_ERROR: "Protocol error",
	RPC_NOT_FOUND:      "Not found",
	RPC_WRONG_ENCODING: "Wrong encoding",
	RPC_APP_ERROR:      "Error",
}

/*type Message struct {
	Code RpcStatusCode
	Data *interface{}
}*/

// restricted interface to read messages from stream
// note what closing reader or writer will cause whole stream to be closed, not only single direction as in TCP
type StreamReader interface {
	io.Closer
	Read(targetMsg interface{}) error
}

// restricted interface to write messages to stream
type StreamWriter interface {
	io.Closer
	Write(msg interface{}) error
}

type Stream interface {
	Path() string
	Read(target interface{}) error
	Write(data interface{}) error
	WriteError(err error) error
	writeMessage(code RpcStatusCode, data interface{}) error
	sendErrorAndClose(code RpcStatusCode, e interface{})
	io.Closer
}

// Stream is analogue of tpc connection but it opened on multiplexed tcp stream
type StreamImpl struct {
	path    string
	con     net.Conn
	onError func(error)
	encoder Encoder
	decoder Decoder
	config  *Config
}

func (s *StreamImpl) Path() string {
	return s.path
}

func (s *StreamImpl) decode(data interface{}) error {
	err := s.decoder.Decode(data)
	if err != nil {
		if _, ok := err.(*RpcConnectionError); ok {
			s.onError(err)
		}
	}
	return err
}

func (s *StreamImpl) Read(target interface{}) error {
	if target == nil {
		return errors.New("Must be non-nil")
	}
	var code RpcStatusCode
	err := s.decode(&code)
	if err != nil {
		return err
	}

	switch code {
	case RPC_OK:
		err := s.decode(target)
		if err != nil {
			return err
		}
		return nil
	case RPC_APP_ERROR:
		var e error
		err := s.decode(&e)
		if err != nil {
			return err
		}
		return e
	default:
		var msgStr string
		err := s.decode(&msgStr)
		if err != nil {
			return err
		}
		return &RpcError{code, msgStr}
	}
}

func (s *StreamImpl) Write(data interface{}) error {
	if data == nil {
		return errors.New("Must be non-nil")
	}
	return s.writeMessage(RPC_OK, data)
}

func (s *StreamImpl) WriteError(err error) error {
	if err == nil {
		return errors.New("Must be non-nil")
	}
	return s.writeMessage(RPC_APP_ERROR, &err)
}

func (s *StreamImpl) writeMessage(code RpcStatusCode, data interface{}) error {
	err := s.encoder.Encode(code)
	if err != nil {
		s.onError(err)
		return err
	}
	err = s.encoder.Encode(data)
	if err != nil {
		s.onError(err)
		return err
	}
	return nil
}

func (s *StreamImpl) Close() error {
	return s.con.Close()
}

func (s *StreamImpl) sendErrorAndClose(code RpcStatusCode, e interface{}) {
	err := s.writeMessage(code, e)
	if err != nil {
		log.Error(err.Error())
	}
	s.Close()
}

type ConnectionState int

const (
	CON_CONNECTING ConnectionState = iota
	CON_READY
	CON_FAILURE
	CON_IDLE
	CON_CLOSED
)

type Connection interface {
	Config() *Config
	State() ConnectionState
	Close() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type ClientConnection interface {
	Connection
	OpenStream(ctx context.Context, path string) (Stream, error)
	Call(ctx context.Context, path string, req interface{}, response interface{}) error
	CallStream(ctx context.Context, path string, req interface{}) (StreamReader, error)
}

type ServerConnection interface {
	Connection
	AcceptStream(ctx context.Context) (Stream, error)
}

type Config struct {
	KeepAliveInterval time.Duration
	KeepAliveTimeout  time.Duration
	Timeout           time.Duration
}

func (c *Config) Validate() error {
	if c.KeepAliveTimeout < c.KeepAliveInterval {
		return errors.New("KeepAliveTimeout should be greater or equal to KeepAliveInterval")
	}
	return nil
}
