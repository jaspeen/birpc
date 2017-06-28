package birpc

import (
	//"sync"
	"birpc/log"
	"errors"
	"github.com/cavaliercoder/badio"
	mockCon "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
	"github.com/ventu-io/slf"
	"github.com/ventu-io/slog"
	"github.com/ventu-io/slog/basic"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestMain(m *testing.M) {
	bh := basic.New()
	// optionally define the format (this here is the default one)
	bh.SetTemplate("{{.Time}} [\033[{{.Color}}m{{.Level}}\033[0m] {{.Context}}{{if .Caller}} ({{.Caller}}){{end}}: {{.Message}}{{if .Error}} (\033[31merror: {{.Error}}\033[0m){{end}} {{.Fields}}")

	// initialise and configure the SLF implementation
	lf := slog.New()
	lf.SetCallerInfo(slf.CallerShort)
	// set common log level to INFO
	lf.SetLevel(slf.LevelDebug)
	// set log level for specific contexts to DEBUG
	//lf.SetLevel(slf.LevelDebug, "app.package1", "app.package2")
	lf.AddEntryHandler(bh)
	lf.SetConcurrent(true)
	// make this into the one used by all the libraries
	slf.Set(lf)
	log.SetLog(slf.WithContext("birpc"))
	os.Exit(m.Run())
}

func TestRpcConnectionError(t *testing.T) {
	err := &RpcConnectionError{Cause: errors.New("Test error")}
	assert.Equal(t, "Connection error: Test error", err.Error())
}

func TestRpcErrorNormal(t *testing.T) {
	err := &RpcError{Code: RPC_PROTOCOL_ERROR}
	assert.Equal(t, "Protocol error", err.Error())
	err = &RpcError{Code: RPC_APP_ERROR, Msg: "Something bad"}
	assert.Equal(t, "Error: Something bad", err.Error())
}

func TestRpcErrorCustomCode(t *testing.T) {
	err := &RpcError{Code: 42}
	assert.Equal(t, "Error #42", err.Error())
}

func createTestStreams(onError1 func(error), onError2 func(error)) (Stream, Stream) {
	mockC := mockCon.NewConn()

	serverStream := &StreamImpl{con: mockC.Client, path: "testpath?param=value", onError: onError1, encoder: GobEncoding.Encoder(mockC.Client), decoder: GobEncoding.Decoder(mockC.Client)}
	clientStream := &StreamImpl{con: mockC.Server, path: "testpath?param=value", onError: onError2, encoder: GobEncoding.Encoder(mockC.Server), decoder: GobEncoding.Decoder(mockC.Server)}
	return serverStream, clientStream
}

func TestStreamClient(t *testing.T) {
	testOnError := func(err error) {
		//not expecting errors here
		t.Errorf("onError: %s", err)
	}

	serverStream, clientStream := createTestStreams(testOnError, testOnError)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		// test stream writer
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := serverStream.Write("Preved namba " + strconv.Itoa(i))
			if err == io.EOF {
				log.Debug("EOF reached")
				break
			} else {
				assert.NoError(t, err)
			}
		}
		serverStream.Close()

	}()

	for i := 0; i < 101; i++ {
		var res string
		err := clientStream.Read(&res)
		if err == io.EOF {
			log.Debug("EOF reached")
			break
		}
		assert.NoError(t, err)

		assert.Equal(t, "Preved namba "+strconv.Itoa(i), res)

		if i == 100 {
			// loop should exit on EOF error
			t.Error("should exit by EOF error")
		}
	}
	err := clientStream.Close()
	assert.NoError(t, err)
	wg.Wait()
}

func TestStreamEncError(t *testing.T) {
	onE := func(err error) {}
	serverStream, clientStream := createTestStreams(onE, onE)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverStream.Write(123)
	}()

	var s string
	err := clientStream.Read(&s)
	assert.Error(t, err)

	rpcErr, ok := err.(*RpcError)
	assert.True(t, ok)
	assert.Equal(t, RPC_WRONG_ENCODING, rpcErr.Code)
	wg.Wait()
}

func TestStreamPathError(t *testing.T) {
	onE := func(err error) {}
	serverStream, clientStream := createTestStreams(onE, onE)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverStream.writeMessage(RPC_NOT_FOUND, "specified path does not exist")
	}()

	var s string
	err := clientStream.Read(&s)
	assert.Error(t, err)

	rpcErr, ok := err.(*RpcError)
	assert.True(t, ok)
	assert.Equal(t, RPC_NOT_FOUND, rpcErr.Code)
	assert.Equal(t, err.Error(), "Not found: specified path does not exist")
	wg.Wait()
}

func TestStreamEof(t *testing.T) {
	onE := func(err error) {}
	serverStream, clientStream := createTestStreams(onE, onE)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverStream.Close()
	}()

	var s string
	err := clientStream.Read(&s)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
	wg.Wait()
}

type myErrorStruct struct {
	Val string
	C   int
}

func (e *myErrorStruct) Error() string {
	return e.Val + strconv.Itoa(e.C)
}

func TestStreamAppError(t *testing.T) {
	var onErrVal error
	onE := func(err error) {
		onErrVal = err
	}
	GobEncoding.Register(&myErrorStruct{}) // register custom error

	serverStream, clientStream := createTestStreams(onE, onE)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverStream.WriteError(&myErrorStruct{Val: "My error message", C: 42})
	}()

	var s string
	err := clientStream.Read(&s)
	assert.Error(t, err)
	assert.NoError(t, onErrVal)
	myErr, ok := err.(*myErrorStruct)
	assert.True(t, ok)
	assert.Equal(t, 42, myErr.C)
	assert.Equal(t, err.Error(), "My error message42")
	wg.Wait()
}

func TestStreamNils(t *testing.T) {
	onE := func(err error) {

	}
	stream, clientStream := createTestStreams(onE, onE)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var s string
			e := clientStream.Read(&s)
			if e == io.EOF {
				break
			}
			assert.NoError(t, e)
		}
	}()
	err := stream.WriteError(nil)
	assert.Error(t, err)
	err = stream.Write(nil)

	assert.Error(t, err)

	err = stream.Read(nil)
	assert.Error(t, err)

	assert.Equal(t, "testpath?param=value", stream.Path())
	stream.Close()
	wg.Wait()
}

func TestStreamConnectionErrors(t *testing.T) {
	mockC := mockCon.NewConn()
	var onErrVal error
	onE := func(err error) {
		onErrVal = err
	}
	serverStream := &StreamImpl{con: mockC.Client, path: "testpath?param=value", onError: onE, encoder: GobEncoding.Encoder(mockC.Client), decoder: GobEncoding.Decoder(mockC.Client)}
	clientStream := &StreamImpl{con: mockC.Server, path: "testpath?param=value", onError: onE, encoder: GobEncoding.Encoder(mockC.Server), decoder: GobEncoding.Decoder(badio.NewBreakReader(mockC.Server, 50))}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			err := serverStream.Write("Preved namba " + strconv.Itoa(i))
			if err == io.EOF {
				log.Debug("EOF reached")
				break
			} else {
				if i == 2 {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			}
		}
	}()

	var res string

	err := clientStream.Read(&res)
	assert.NoError(t, err)
	assert.Equal(t, "Preved namba 0", res)
	err = clientStream.Read(&res)
	assert.NoError(t, err)
	assert.Equal(t, "Preved namba 1", res)

	// here we should exceed 50b reader break limit
	err = clientStream.Read(&res)
	assert.Error(t, err)
	_, ok := err.(*RpcConnectionError)
	assert.True(t, ok)
	if !ok {
		return
	}
	assert.Equal(t, err, onErrVal)

	err = clientStream.Close()
	assert.NoError(t, err)
	wg.Wait()
}
