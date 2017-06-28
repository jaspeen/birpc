package test

/*
 Test connection
*/

import (
	"context"
	"sync"
	//"sync/atomic"
	"testing"
	//"net/http"
	"birpc"
	"birpc/log"
	"net/http"
	_ "net/http/pprof"
)

func SimpleTestHandler(req string, res *string) error {
	log.Debug("Got function request " + req)
	*res = "RESPONSE"
	return nil
}

type TestServerStuff struct {
}

func (ss *TestServerStuff) SingleArgumentCall(ctx context.Context, arg string) error {
	return nil
}

func (ss *TestServerStuff) SingleResultCall(ctx context.Context) (string, error) {
	return "preved", nil
}

func (ss *TestServerStuff) ArgResultCall(ctx context.Context, arg string) (string, error) {
	return arg + "result", nil
}

func (ss *TestServerStuff) StreamResultCall(ctx context.Context, arg string, stream birpc.Stream) error {
	for i := 0; i < 100; i++ {
		stream.Write("Preved")
		stream.Write("Medved")
	}
	return nil
}

func startCS(ctx context.Context, t *testing.T) error {
	r, err := birpc.NewRpc(birpc.GobEncoding, birpc.DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
		return err
	}
	rs := r.NewServer("testtoken")
	rs.RegisterHandler("test", func(ctx context.Context, stream birpc.Stream) error {
		log.Debug("Got request on test")
		var data string
		err := stream.Read(&data)
		if err != nil {
			log.Error("Error reading on server side" + err.Error())
			t.Error(err)
			return err
		}
		log.Debugf("Data comes %d\n", len(data))
		return stream.Write("ok" + data)
	})

	rs.RegisterHandler("test2", func(ctx context.Context, stream birpc.Stream) error {
		log.Debug("Got request on test2")
		var data string
		err := stream.Read(&data)
		if err != nil {
			log.Error("Error reading on server side stream" + err.Error())
			t.Error(err)
			return err
		}
		log.Debugf("Data comes %d\n", len(data))
		return stream.Write("ok" + data)
	})
	rs.RegisterHandler("test3", birpc.FuncHandler(SimpleTestHandler))
	rs.Connect(ctx, "tcp", "localhost:14598")
	log.Info("Connect finished")
	return nil
}

func TestRpcCommunication(t *testing.T) {
	go func() {
		log.Debugf("%v", http.ListenAndServe("localhost:6060", nil))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	r, err := birpc.NewRpc(birpc.GobEncoding, birpc.DEFAULT_CONFIG)
	if err != nil {
		t.Error(err)
		return
	}
	data := ""
	for i := 0; i < 12800; i++ {
		data += "01234567890"
	}
	rc := r.NewClient()
	scchan := make(chan *birpc.StickyConnection)
	log.Error("Before serve")
	go func() {
		err := rc.Serve(ctx, "tcp", ":14598", scchan)
		log.Infof("Serve cancelled %s", err)
	}()
	log.Debug("Serving")
	go startCS(ctx, t)
	sc := <-scchan
	log.Debug("Got sticky connection")
	wg := sync.WaitGroup{}
	/*wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			var res string
			err := sc.Call(ctx, "test", data, &res)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()*/
	wg.Add(1)
	go func() {
		for i := 0; i < 1; i++ {
			var res string
			err := sc.ProcessStream(ctx, "test2", data, func() interface{} { return &res },
				func(target interface{}) error {
					log.Debugf("Print it %s", *(target.(*string)))
					return nil
				})

			if err != nil {
				log.Error(err.Error())
				t.Error(err)
			}
		}
		wg.Done()
	}()
	//time.Sleep(50*time.Second)
	wg.Wait()
	cancel()
	//time.Sleep(60 * time.Second)
}

//
//func TestCancellation1(t *testing.T) {
//	ctx, cancel := context.WithCancel(context.Background())
//	r := &Rpc{}
//	rc := &RpcClient{Handler: func(ctx context.Context, con ClientConnection) error {
//		for i := 0; i < 100; i++ {
//			var res string
//			err := con.Call(ctx, "test", []byte("Hi"), &res)
//			if err != nil {
//				t.Error(err)
//			}
//			select {
//			case <-ctx.Done():
//				log.Println("Cancelled")
//				return ctx.Err()
//			default:
//				time.Sleep(100 * time.Millisecond)
//			}
//		}
//		return nil
//	}}
//	wg := sync.WaitGroup{}
//	cancelled := atomic.Value{}
//	cancelled.Store(false)
//	wg.Add(1)
//	go func() {
//		log.Println("Start server")
//		err := r.Serve(ctx, "tcp", ":14598", rc)
//		log.Println("After serve")
//		if err != nil {
//			//should be cancelled error
//			if err != context.Canceled {
//				t.Errorf("Error returned should be context.Canceled - %+v", err)
//			}
//		}
//		cancelled.Store(true)
//		wg.Done()
//	}()
//	cancel()
//	wg.Wait()
//	if cn := cancelled.Load().(bool); !cn {
//		t.Errorf("Server should be properly cancelled on context cancel")
//	}
//}
//
//func TestReconnect(t *testing.T) {
//	go func() {
//		log.Println(http.ListenAndServe("localhost:6060", nil))
//	}()
//
//	ctx, cancel := context.WithCancel(context.Background())
//	r := NewRpc(nil)
//	rc := r.NewClient(func(ctx context.Context, con ClientConnection) error {
//		for {
//		for i := 0; i < 10; i++ {
//			log.Printf("Call test")
//			var res string
//			err := con.Call(ctx, "test3", 123, &res)
//
//			if err != nil {
//				t.Error(err)
//				return err
//				log.Printf("Server returned error %+v\n", err)
//			}
//			log.Printf("Call success, res = " + res)
//			select {
//			case <-ctx.Done():
//				log.Println("Cancelled")
//				return ctx.Err()
//			default:
//				time.Sleep(1 * time.Millisecond)
//			}
//		}
//			time.Sleep(10*time.Second)
//		}
//		return nil
//	})
//	wg := sync.WaitGroup{}
//	cancelled := atomic.Value{}
//	cancelled.Store(false)
//	wg.Add(1)
//	go func() {
//		log.Println("Before serve")
//		err := r.Serve(ctx, "tcp", ":14598", rc)
//		log.Println("After serve")
//		if err != nil {
//			//should be cancelled error
//			if err != context.Canceled {
//				t.Errorf("Error returned should be context.Canceled - %+v", err)
//			}
//		}
//		cancelled.Store(true)
//		wg.Done()
//	}()
//	log.Println("Before start CS")
//	//go startCS(ctx, t)
//	log.Println("Before cancellation")
//
//	log.Println("Cancelled")
//	ctx2, cancel2 := context.WithCancel(context.Background())
//	go startCS(context.Background(), t)
//	time.Sleep(2*time.Second)
//	cancel()
//	wg.Wait()
//	if cn := cancelled.Load().(bool); !cn {
//		t.Errorf("Server should be properly cancelled on context cancel")
//	}
//	go func() {
//		 r.Serve(ctx2, "tcp", ":14598", rc)
//		wg.Done()
//	}()
//	wg.Add(1)
//	cancel2()
//	wg.Wait()
//	//cancel2()
//
//}
