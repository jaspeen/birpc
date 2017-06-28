package birpc

import (
	"context"
	"errors"
	"github.com/jaspeen/birpc/log"
	"github.com/jpillora/backoff"
	"github.com/ventu-io/slf"
	"net"
	"sync"
	"time"
)

type Client struct {
	rpc               *Rpc
	stickyConnections map[string]*StickyConnection //TODO: sync
	log               slf.Logger
	network           Network
}

// private constructor
func newClient(rpc *Rpc, network Network) *Client {
	return &Client{rpc: rpc, network: network, stickyConnections: map[string]*StickyConnection{}, log: slf.WithContext("birpc.Client")}
}

// process raw connection and construct or update sticky connection
func (c *Client) handle(ctx context.Context, con net.Conn, existing *bool, onDisconnect func(bool)) (*StickyConnection, error) {
	c.log.Debugf("Handle raw connection %+v", c.stickyConnections)

	transport, err := newClientTransport(c.rpc, func(error) {}, con)
	if err != nil {
		return nil, err
	}

	sc, ok := c.stickyConnections[transport.id]
	if ok {
		c.log.Debugf("Found connection by token %s", transport.id)
		state := sc.State()
		if state == SCON_TERMINATED {
			e := errors.New("Connection with token " + transport.id + " already terminated but not excluded from registry")
			transport.Close()
			return nil, e
		}
		if state == SCON_ONLINE {
			e := errors.New("Connection with token " + transport.id + " already established")
			transport.Close()
			return nil, e
		}
		*existing = true
		sc.onConnect(transport, onDisconnect)
	} else {
		c.log.Debugf("Creating new sticky connection %s", transport.id)
		sc = newStickyConnection(transport.id, transport, onDisconnect)
		c.stickyConnections[transport.id] = sc
	}
	return sc, nil
	//return rpcclient.Handler(ctx, connection)
}

func (c *Client) dial(ctx context.Context, network string, addr string) (net.Conn, error) {
	return c.network.DialContext(ctx, network, addr)
}

type disconnectChan struct {
	disconnect chan int
	handled    bool
	mutex      sync.Mutex
}

func (dc *disconnectChan) Call(terminate bool) {
	dc.mutex.Lock()
	if dc.handled {
		return
	}
	if terminate {
		close(dc.disconnect)
	} else {
		dc.disconnect <- 1
	}
	dc.handled = true
	defer dc.mutex.Unlock()
}

func (c *Client) connectionLoop(ctx context.Context, network string, addr string, resultCollect func(*StickyConnection, error)) {
	b := &backoff.Backoff{
		Min:    1 * time.Second,
		Max:    5 * time.Minute,
		Factor: 2,
		Jitter: false,
	}
	for {
		var sc *StickyConnection
		con, err := c.dial(ctx, network, addr)
		if err == nil {
			b.Reset()
			var existing bool
			dc := &disconnectChan{disconnect: make(chan int)}
			sc, err = c.handle(ctx, con, &existing, dc.Call)
			if err != nil {
				c.log.Error(err.Error())
			}
			c.log.Debug("Exec callback with connection")
			if !existing {
				resultCollect(sc, err) // return result to Connect function
			}

			// Wait until connection terminate or context cancelled
			select {
			case <-ctx.Done():
				sc.transport.Close()
				delete(c.stickyConnections, sc.Id())
				return
			case ctrlVal := <-dc.disconnect:
				sc.transport.Close()
				if ctrlVal == 0 {
					log.Debugf("Sticky connection %s going to be terminated", sc.Id())
					delete(c.stickyConnections, sc.Id())
					return
				} else {
					goto RECONNECT
				}
			}
		} else {
			c.log.Error(err.Error())
		}
	RECONNECT:
		select {
		case <-ctx.Done():
			return
		default:
			waitDur := b.Duration()
			c.log.Debugf("Waiting %s before reconnect", waitDur)
			time.Sleep(waitDur)
		}
	}
}

func (c *Client) Connect(ctx context.Context, network string, addr string) (*StickyConnection, error) {
	c.log.Debugf("Connecting to %s", addr)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var sc *StickyConnection
	var e error
	go c.connectionLoop(ctx, network, addr, func(con *StickyConnection, err error) {
		sc = con
		e = err
		wg.Done()
	})
	wg.Wait()
	return sc, e
}

func (c *Client) Serve(ctx context.Context, network string, addr string, connections chan *StickyConnection) error {
	c.log.Debugf("Start listening on %s", addr)

	listener, err := c.network.Listen(network, addr)

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
			c.log.Debug("Waiting listener.Accept")
			con, err := listener.Accept()
			select {
			case <-ctx.Done():
				c.log.Debug("Exiting from accept routine")
				return
			default:
				//do nothing
			}
			c.log.Debug("Accepted from listener.Accept")

			acceptChan <- acceptRes{con, err}
			c.log.Debug("Sent to accepts channel")
		}
	}()

	for {
		c.log.Debug("Waiting for accepts")
		select {
		case ar := <-acceptChan:

			if ar.err != nil {
				c.log.Debugf("Not accepted, err %+v\n", err)
				return err
			}
			c.log.Debug("Accepted raw connection")

			go func() {
				var existing bool
				dc := &disconnectChan{disconnect: make(chan int)}
				sc, err := c.handle(ctx, ar.conn, &existing, dc.Call)
				c.log.Debugf("Connection handled, existing = %t", existing)
				if sc != nil && err == nil && !existing {
					c.log.Debug("Pushing connection to channel")
					connections <- sc
				}
				if err != nil {
					c.log.Errorf("Error in server rpc handler: %s", err)
					return
				}
				select {
				case <-ctx.Done():
					return
				case val := <-dc.disconnect:
					// Reconnect should happen by new accept with same sticky token
					if val == 0 {
						delete(c.stickyConnections, sc.Id())
					}
					return
				}
			}()
		case <-ctx.Done():
			c.log.Debug("Closing listener")
			listener.Close()
			return ctx.Err()
		}

	}

}
