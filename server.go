package birpc

import (
	"context"
	//"encoding/binary"
	//"errors"
	"github.com/jpillora/backoff"
	//"io"
	"birpc/log"
	"github.com/ventu-io/slf"
	"net"
	"reflect"
	"time"
	"io"
)

type StreamHandler func(context.Context, Stream) error

func FuncHandler(f interface{}) StreamHandler {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Func {
		panic("argument is not a functions")
	}
	if t.NumIn() != 2 {
		panic("Function should have exact 2 arguments")
	}

	if t.NumOut() != 1 {
		panic("Function should return an error")
	}
	return func(ctx context.Context, stream Stream) error {
		reqVal := reflect.New(t.In(0))
		req := reqVal.Interface()
		resp := reflect.New(t.In(1).Elem())

		err := stream.Read(req)
		if err != nil {
			return err
		}

		callArgs := []reflect.Value{reqVal.Elem(), resp}
		log.Debugf("Call args %+v\n", callArgs)
		retValues := reflect.ValueOf(f).Call(callArgs)
		fErr := retValues[0].Interface()
		if fErr != nil {
			fErrStr, _ := fErr.(error)
			err := stream.WriteError(fErrStr)
			if err != nil {
				return err
			}
		} else {
			err := stream.Write(resp.Interface())
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func StructHandler(s interface{}) map[string]StreamHandler {
	return nil
}

type Server struct {
	handlers    map[string]StreamHandler
	rpc         *Rpc
	log         slf.Logger
	stickyToken string
	network     Network
}

func newServer(rpc *Rpc, id string, network Network) *Server {
	return &Server{rpc: rpc, stickyToken: id, network: network,
		log:      log.WithContext("birpc.Server").WithField("id", id),
		handlers: make(map[string]StreamHandler)}
}

func (s *Server) RegisterHandler(path string, handler StreamHandler) {
	s.handlers[path] = handler
}

func (s *Server) handle(ctx context.Context, con net.Conn) error {

	transport, err := newServerTransport(s.rpc, func(err error) { s.log.Error(err.Error()) }, con, s.stickyToken)
	if err != nil {
		return err
	}
	go func() {
		<- ctx.Done()
		transport.Close()
	}()
	for {

		s.log.Debug("Start accepting stream")
		stream, err := transport.AcceptStream(ctx)

		if err != nil {
			s.log.Debug("Failed accepting stream")
			//connection broken, returing from handle to probably reconnect again
			return err
		}
		if stream == nil && err == nil {
			//TODO: other side ask to close the connection
			s.log.Debug("Closing transport requested")
			transport.Close()
			return io.EOF
		}

		s.log.Debugf("Accepted stream %s", stream.Path())

		handler, ok := s.handlers[stream.Path()]
		if !ok {
			stream.sendErrorAndClose(RPC_NOT_FOUND, rpcErrorMap[RPC_NOT_FOUND])
			continue
		}
		handlerCtx := ctx
		go func() {
			s.log.Debugf("Calling handler %s", stream.Path())
			err := handler(handlerCtx, stream)
			if err != nil {
				s.log.Errorf("Handler error: %s", err.Error())
			}
			s.log.Debug("Closing stream")
			stream.Close()
		}()
	}
}

func (s *Server) Connect(ctx context.Context, network string, addr string) error {
	s.log.Debugf("Connecting to %s", addr)
	b := &backoff.Backoff{
		Min:    1 * time.Second,
		Max:    5 * time.Minute,
		Factor: 2,
		Jitter: false,
	}
	for {
		con, err := s.network.DialContext(ctx, network, addr)
		if err == nil {
			s.log.Debugf("Succesfully connected")
			b.Reset()
			err = s.handle(ctx, con)
			if err != nil {
				s.log.Error(err.Error())
				// reconnecting
			}
		} else {
			s.log.Error(err.Error())

		}
		select {
		case <-ctx.Done():
			s.log.Info("Cancelling server loop")
			return ctx.Err()
		default:
			dur := b.Duration()
			s.log.Warnf("Reconnecting after %s", dur)
			time.Sleep(dur)
		}
	}
}

func (s *Server) Serve(ctx context.Context, network string, addr string) error {
	s.log.Debugf("Start listening on %s", addr)
	listener, err := s.network.Listen(network, addr)
	s.log.Debug("Got listen")
	if err != nil {
		return err
	}

	type acceptRes struct {
		conn net.Conn
		err  error
	}

	acceptChan := make(chan acceptRes)

	go func() {
		for {
			s.log.Debug("Before accept")
			con, err := listener.Accept()
			select {
			case <-ctx.Done():
				s.log.Debug("Need to finish this")
				return
			default:
				//do nothing
			}
			s.log.Debug("Accepted")

			acceptChan <- acceptRes{con, err}
			s.log.Debug("Sent to accept channel")
		}
	}()

	for {
		select {
		case ar := <-acceptChan:

			if ar.err != nil {
				s.log.Errorf("Not accepted, err %+v\n", err)
				return err
			}
			s.log.Debug("Accepted connection")

			go func() {
				e := s.handle(ctx, ar.conn)
				if e != nil {
					if ctx.Err() == nil {
						s.log.Error(e.Error())
					} else {
						s.log.Debug("Handler cancelled")
					}
				}
			}()
		case <-ctx.Done():
			s.log.Info("Cancelling server loop")
			listener.Close()
			close(acceptChan)
			return ctx.Err()
		}
		s.log.Debug("Just for")

	}
}
